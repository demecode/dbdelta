from pyspark import pipelines as dp

# cust expectations

cust_rules = {"rule_1": "customer_id is NOT NULL", "rule_2": "region is not null"}

# ingest customers

@dp.table(name="customers_stg")

@dp.expect_all_or_drop(cust_rules)
def customers_stg():
    df = spark.readStream.table("delta_live.source.customers")
    return df