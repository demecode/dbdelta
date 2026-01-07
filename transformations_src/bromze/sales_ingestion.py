# creat empty stream / using append from both sources

from pyspark import pipelines as dp

sales_rules = {"rule_1": "sales_id is not null"}

# empty streaming table
dp.create_streaming_table(name="sales_stg",
                          expect_all_or_drop=sales_rules)

# east

@dp.append_flow(target="sales_stg")
def east_sales():
    df = spark.readStream.table("delta_live.source.sales_east")
    return df

@dp.append_flow(target="sales_stg")
def west_sales():
    df = spark.readStream.table("delta_live.source.sales_west")
    return df

