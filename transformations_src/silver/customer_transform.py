from pyspark import pipelines as dp
from pyspark.sql.functions import col, cast,upper
from pyspark.sql.types import IntegerType



# create a view to capture changes only
# Transform customer data

@dp.view(
    name = "customer_enr_view"
) 

def customer_stg_trans():
    df = spark.readStream.table("customers_stg")
    df = df.withColumn("customer_name",upper(col("customer_name")))
    return df

# empty Destination Silver streaming table
dp.create_streaming_table(name="customer_enr")

dp.create_auto_cdc_flow(
  target = "customer_enr",
  source = "customer_enr_view",
  keys = ["customer_id"],
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

