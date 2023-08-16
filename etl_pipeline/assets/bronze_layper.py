import pandas as pd
from dagster import asset, Output, Definitions
from resources.minio_io_manager import MinIOIOManager

ls_tables = [
    "olist_orders_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_products_dataset",
    "product_category_name_translation",
]

#___Table 1

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom", "olist_products_dataset"],
    compute_kind="MySQL"
)
def bronze_olist_products_dataset(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM olist_products_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    
    return Output(
        pd_data,
        metadata={
            "table": "olist_products_dataset",
            "records count": len(pd_data),
        },
    )


#___Table 2

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom", "product_category_name_translation"],
    compute_kind="MySQL"
)
def bronze_product_category_name_translation(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM product_category_name_translation"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    
    return Output(
        pd_data,
        metadata={
            "table": "product_category_name_translation",
            "records count": len(pd_data),
        },
    )

#___Table 3

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom", "olist_order_payments_dataset"],
    compute_kind="MySQL"
)
def bronze_olist_order_payments_dataset(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM olist_order_payments_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    
    return Output(
        pd_data,
        metadata={
            "table": "olist_order_payments_dataset",
            "records count": len(pd_data),
        },
    )

#___Table 4

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom", "olist_order_items_dataset"],
    compute_kind="MySQL"
)
def bronze_olist_order_items_dataset(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM olist_order_items_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    
    return Output(
        pd_data,
        metadata={
            "table": "olist_order_items_dataset",
            "records count": len(pd_data),
        },
    )

#___Table 5

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom", "olist_orders_dataset"],
    compute_kind="MySQL"
)
def bronze_olist_orders_dataset(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM olist_orders_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    
    return Output(
        pd_data,
        metadata={
            "table": "olist_orders_dataset",
            "records count": len(pd_data),
        },
    )

MINIO_CONFIG = {
    "endpoint_url": "localhost:9000",
    "bucket": "warehouse",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
}
defs = Definitions(
    assets=[bronze_olist_products_dataset, bronze_product_category_name_translation, bronze_olist_order_payments_dataset, bronze_olist_order_items_dataset, bronze_olist_orders_dataset],
    resources={
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
    },
)