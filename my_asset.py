import pandas as pd
from dagster import asset

@asset(
    name="my_asset",
    key_prefix=["demo","fde"],
    metadata={"owner": "aide", "priority": "high"},
    compute_kind="python",
    group_name="demo"
)
def my_asset(context):
    pd_data = pd.DataFrame({
        "a": [1,2,3],
        "b": ["x","y","z"]
    })
    return pd_data