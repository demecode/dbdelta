from pyspark import pipelines as dp
from pyspark.sql.functions import *

# mv business view - if you use streaming you will get skewed results because the streaming view will only compute the incremntal changes, i.e update balances

@dp.table(
    name="business_sales"
)

def business_sales():
    df_fact = spark.read.table("fact_sales")
    df_dim_cust = spark.read.table("dim_customers")
    df_dim_products = spark.read.table("dim_products")
    
    df_join = df_fact.join(df_dim_cust, df_fact.customer_id == df_dim_cust.customer_id, "inner").join(df_dim_products, df_fact.product_id == df_dim_products.product_id, "inner")

    df_prune  = df_join.select("region", "category", "total_amount")

    df_agg = df_prune.groupBy("region", "category").agg(sum("total_amount").alias("total_sales"))

    return df_agg
