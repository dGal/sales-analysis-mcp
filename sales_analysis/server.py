import os
import asyncpg
from mcp.server.fastmcp import FastMCP
from typing import Literal, List
from dotenv import load_dotenv

load_dotenv()

# This is the shared MCP server instance
mcp = FastMCP("sales-analysis-mcp")


async def fetch_data(*args, **kwargs):

    pool = await asyncpg.create_pool(
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        database=os.getenv('POSTGRES_DB'),
        host=os.getenv('POSTGRES_HOST'),
        port=5432
    )

    async with pool.acquire() as conn:
        rows = await get_profit_change_by_segment(conn, *args, **kwargs)
        #rows = await conn.fetch('SELECT * FROM sales_data LIMIT 2')

    await pool.close()
    return [dict(row) for row in rows]

async def get_profit_change_by_segment(
    conn: asyncpg.Connection,
    profit_type: Literal["middle_man", "seller", "overall"],
    time_column: Literal["ym", "yq", "yh"],
    time_values: List[str],
    group_by: List[str] = ["bu", "product", "customer"],
    limit: int = 10):
    """
    Fetch profit change between two time periods by BU, middle_man, product.

    Args:
        conn: asyncpg.Connection
        profit_type: 'middle_man', 'seller', or 'overall'
        time_column: one of 'ym', 'yq', 'yh'
        time_values: list of exactly two time period strings (e.g. ['2022', '2023'])

    Returns:
        List of records with profit change and contribution percentage
    """
    assert len(time_values) == 2, "Exactly two time values required"
    t1, t2 = time_values

    group_by_clause = ", ".join(group_by)
    select_clause = ",\n            ".join(group_by)

    profit_expressions = {
        "middle_man": """
            COALESCE(mid_man_qty, 0) * (
                COALESCE(mid_man_unit_price_jpy, 0) - COALESCE(maker_unit_price_jpy, 0)
            )
        """,
        "seller": """
            COALESCE(seller_qty, 0) * (
                COALESCE(seller_unit_price_jpy, 0) - COALESCE(mid_man_unit_price_jpy, 0)
            )
        """,
        "overall": """
            COALESCE(mid_man_qty, 0) * (
                COALESCE(mid_man_unit_price_jpy, 0) - COALESCE(maker_unit_price_jpy, 0)
            ) +
            COALESCE(seller_qty, 0) * (
                COALESCE(seller_unit_price_jpy, 0) - COALESCE(mid_man_unit_price_jpy, 0)
            )
        """
    }

    profit_expr = profit_expressions[profit_type]

    sql = f"""
    WITH params AS (
        SELECT $1::text AS t1, $2::text AS t2, $3::text AS time_column
    ),
    calc AS (
        SELECT
            {select_clause},

            SUM(
                CASE WHEN (
                    (params.time_column = 'ym' AND sd.ym = params.t1) OR
                    (params.time_column = 'yq' AND sd.yq = params.t1) OR
                    (params.time_column = 'yh' AND sd.yh = params.t1) OR
                    (params.time_column = 'ym' AND position('/' IN params.t1) = 0 AND sd.ym LIKE params.t1 || '%') OR
                    (params.time_column = 'yq' AND position('Q' IN params.t1) = 0 AND sd.yq LIKE params.t1 || '%') OR
                    (params.time_column = 'yh' AND position('H' IN params.t1) = 0 AND sd.yh LIKE params.t1 || '%')
                )
                THEN {profit_expr} ELSE 0 END
            ) AS profit_t1_jpy,

            SUM(
                CASE WHEN (
                    (params.time_column = 'ym' AND sd.ym = params.t2) OR
                    (params.time_column = 'yq' AND sd.yq = params.t2) OR
                    (params.time_column = 'yh' AND sd.yh = params.t2) OR
                    (params.time_column = 'ym' AND position('/' IN params.t2) = 0 AND sd.ym LIKE params.t2 || '%') OR
                    (params.time_column = 'yq' AND position('Q' IN params.t2) = 0 AND sd.yq LIKE params.t2 || '%') OR
                    (params.time_column = 'yh' AND position('H' IN params.t2) = 0 AND sd.yh LIKE params.t2 || '%')
                )
                THEN {profit_expr} ELSE 0 END
            ) AS profit_t2_jpy
        FROM sales_data sd, params
        GROUP BY {group_by_clause}
    ),
    final AS (
        SELECT *,
            profit_t2_jpy - profit_t1_jpy AS profit_change,
            SUM(profit_t2_jpy - profit_t1_jpy) OVER () AS total_profit_change
        FROM calc
    )
    SELECT
        {select_clause},
        profit_t1_jpy,
        profit_t2_jpy,
        profit_change,
        CASE
            WHEN total_profit_change = 0 THEN 0
            ELSE ROUND((profit_change::numeric / ABS(total_profit_change)) * 100, 2)
        END AS percent_profit_change
    FROM final
    ORDER BY ABS(profit_change) DESC
    LIMIT {limit};
    """

    return await conn.fetch(sql, t1, t2, time_column)


@mcp.tool()
async def get_profit_change(
    profit_type: Literal["middle_man", "seller", "overall"],
    time_column: Literal["ym", "yq", "yh"],
    time_values: List[str],
    group_by: List[str] = ["bu", "product", "customer"],
    limit: int = 10) -> dict:
    """
    Get profit change data - calculates profit for all rows of one timestamp and compares them to rows in next timestamp.
    Then calculates percentage of change and returns it in order highest to lowest..
    Args:
        profit_type: 'middle_man', 'seller', or 'overall'
        time_values: list of exactly two time period strings (e.g. ['2022', '2023'])
        group_by: list of columns to group by (e.g. ['bu', 'product', 'customer', 'pn'])
        limit: number of rows to return (10 by default)

        Example of supported time_values:
        ['2022/02', '2022/03']  # year/month   (1-12)
        ['2022 Q2', '2022 Q3']  # year/quarter (1, 2, 3, 4)
        ['2022 H1', '2022 H2']  # year/half    (1, 2)
        ['2022', '2023']
    Returns:
        List of records with profit change and contribution percentages
    """

    if 'Q' in time_values[0]:
        time_column = 'yq'
    elif 'H' in time_values[0]:
        time_column = 'yh'
    else:
        time_column = 'ym'

    rows = await fetch_data(profit_type, time_column, time_values, group_by, limit)

    return rows


if __name__ == "__main__":
    import asyncio
    from pprint import pprint

    rows = asyncio.run(fetch_data("middle_man", "ym", ["2022", "2023"], limit=10))
    pprint(rows)
