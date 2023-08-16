import pandas as pd
from dagster import asset, Output, AssetIn, multi_asset, AssetOut, SourceAsset, AssetKey, Definitions
from resources.minio_io_manager import MinIOIOManager

bronze_olist_products_dataset = SourceAsset(key=AssetKey("bronze_olist_products_dataset"))
bronze_product_category_name_translation = SourceAsset(key=AssetKey("bronze_product_category_name_translation"))

bronze_olist_order_payments_dataset = SourceAsset(key=AssetKey("bronze_olist_order_payments_dataset"))
bronze_olist_order_items_dataset = SourceAsset(key=AssetKey("bronze_olist_order_items_dataset"))
bronze_olist_orders_dataset = SourceAsset(key=AssetKey("bronze_olist_orders_dataset"))



#__ Table dim_products

@multi_asset(
    ins={
        "bronze_olist_products_dataset": AssetIn(
            key_prefix=["bronze", "ecom", "olist_products_dataset"],
        ),
        "bronze_product_category_name_translation": AssetIn(
            key_prefix=["bronze", "ecom", "product_category_name_translation"],
        ),
    },
    outs={
        "dim_products": AssetOut(
            io_manager_key="minio_io_manager",
            key_prefix=["silver", "ecom", "dim_products"],
            metadata={
                "primary_keys": [
                    "product_id",
                ],
                "columns": [
                    "product_id",
                    "product_category_name_english",
                ],
            },
        )
    },
    compute_kind="MinIO"
)
def dim_products(bronze_olist_products_dataset, bronze_product_category_name_translation) -> Output[pd.DataFrame]:

    return Output(
        dim_products,
        metadata={
            "schema": "public",
            "table": "dim_products",
            "records counts": len(dim_products),
        },
    )

#__ Table fact_sales

@multi_asset(
    ins={
        "bronze_olist_order_payments_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
        ),
        "bronze_olist_order_items_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
        ),
        "bronze_olist_orders_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
        ),
    },
    outs={
        "fact_sales": AssetOut(
            io_manager_key="minio_io_manager",
            key_prefix=["silver", "ecom", "fact_sales"],
            metadata={
                "primary_keys": [
                    "order_id",
                ],
                "columns": [
                    "order_id",
                    "customer_id",
                    "order_purchase_timestamp",
                    "product_id",
                    "payment_value",
                    "order_status",
                ],
            },
        )
    },
    compute_kind="MinIO"
)
def fact_sales(bronze_olist_order_payments_dataset, bronze_olist_order_items_dataset, bronze_olist_orders_dataset) -> Output[pd.DataFrame]:

    return Output(
        fact_sales,
        metadata={
            "schema": "public",
            "table": "fact_sales",
            "records counts": len(fact_sales),
        },
    )


MINIO_CONFIG = {
    "endpoint_url": "localhost:9000",
    "bucket": "warehouse",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
}
defs = Definitions(
    assets=[fact_sales, dim_products],
    resources={
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
    },
)