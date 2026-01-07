from pyspark import pipelines as dp
from pyspark.sql.functions import col

# create a view to capture changes only
# Transform sales data

@dp.view(
    name = "sales_enr_view"
) 

def sales_stg_trans():
    df = spark.readStream.table("sales_stg")
    df = df.withColumn("total_amount", col("quantity") * ("amount"))
    return df
# empty Destination Silver streaming table
dp.create_streaming_table(name="sales_enr")


dp.create_auto_cdc_flow(
  target = "sales_enr",
  source = "sales_enr_view",
  keys = ["sales_id"],
  sequence_by = "sale_timestamp",
  ignore_null_updates = False,
  apply_as_deletes = None,
  apply_as_truncates = None,
  column_list = None,
  except_column_list = None,
  stored_as_scd_type = 1,
  track_history_column_list = None,
  track_history_except_column_list = None,
)

