from pyspark import pipelines as dp

# creatine empty str table SC2

dp.create_streaming_table(
    name="dim_customers"
)

# auto cdc

dp.create_auto_cdc_flow(
  target = "dim_customers",
  source = "customer_enr_view",
  keys = ["customer_id"],
  sequence_by = "last_updated",
  ignore_null_updates = False,
  apply_as_deletes = None,
  apply_as_truncates = None,
  column_list = None,
  except_column_list = None,
  stored_as_scd_type = 2,
  track_history_column_list = None,
  track_history_except_column_list = None,
)