"""
domains/sales_europe/facts_config.py

Aggregation definitions consumed by core/facts.py.
"""

FACTS_CONFIG: list[dict] = [
    {
        "target_table"    : "fct_sales",
        "source_table"    : "stg_orders",
        "group_by"        : ["order_date", "region"],
        "aggregations"    : {
            "total_amount"   : ("amount",    "SUM"),
            "order_count"    : ("order_id",  "COUNT"),
            "total_quantity" : ("quantity",  "SUM"),
        },
        "partition_column": "order_date",
    },
]
