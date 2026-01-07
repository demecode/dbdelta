from pyspark import pipelines as dp

# products expectations

product_rules = {"rule_1": "product_id is NOT NULL", "rule_2": "price >= 0"}

# ingest products 

@dp.table(name="product_stg")

@dp.expect_all(product_rules)
def product_stg():
    df = spark.readStream.table("delta_live.source.products")
    return df