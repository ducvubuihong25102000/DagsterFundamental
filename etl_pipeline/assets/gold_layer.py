import pandas as pd
from dagster import Output, Definitions, AssetIn, multi_asset, AssetOut, SourceAsset, AssetKey
from resources.minio_io_manager import MinIOIOManager
from resources.mysql_io_manager import MySQLIOManager
from resources.psql_io_manager import PostgreSQLIOManager



dim_products = SourceAsset(key=AssetKey("dim_products"))
fact_sales = SourceAsset(key=AssetKey("fact_sales"))

#___ Table 1

@multi_asset(
    ins={
        "dim_products": AssetIn(
            key_prefix=["silver", "ecom"],
        ),
        "fact_sales": AssetIn(
            key_prefix=["silver", "ecom"],
        ),
    },
    outs={
        "sales_values_by_category": AssetOut(
            io_manager_key="minio_io_manager",
            key_prefix=["gold", "ecom"],
            metadata={
                "primary_keys": [
                    "monthly",
                    "category",
                ],
                "columns": [
                    "monthly",
                    "category",
                    "total_sales",
                    "total_bills",
                    "values_per_bills",
                ],
            },
        )
    },
)
def sales_values_by_category(dim_products, fact_sales) -> Output[pd.DataFrame]:
    return Output(
        sales_values_by_category,
        metadata={
            "schema": "public",
            "table": "sales_values_by_category",
            "records counts": len(sales_values_by_category),
        },
    )

MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "database": "brazillian_ecommerce",
    "user": "admin",
    "password": "admin123",
}
MINIO_CONFIG = {
    "endpoint_url": "localhost:9000",
    "bucket": "warehouse",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
}
PSQL_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "user": "admin",
    "password": "admin123",
}

defs = Definitions(
    assets=[sales_values_by_category],
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
    },
)