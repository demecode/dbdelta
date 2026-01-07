from pyspark import pipelines as dp
from pyspark.sql.functions import col, cast
from pyspark.sql.types import IntegerType



# create a view to capture changes only
# Transform product data

@dp.view(
    name = "products_enr_view"
) 

def products_stg_trans():
    df = spark.readStream.table("product_stg")
    df = df.withColumn("price", col("price").cast(IntegerType()))
    return df

# empty Destination Silver streaming table
dp.create_streaming_table(name="products_enr")

dp.create_auto_cdc_flow(
  target = "products_enr",
  source = "products_enr_view",
  keys = ["product_id"],
  sequence_by = "last_updated",
  ignore_null_updates = False,
  apply_as_deletes = None,
  apply_as_truncates = None,
  column_list = None,
  except_column_list = None,
  stored_as_scd_type = 1,
  track_history_column_list = None,
  track_history_except_column_list = None,
)

