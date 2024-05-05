# Databricks notebook source file.
product_df='dbfs:/mnt/silver/sales_view/product'
store_df='dbfs:/mnt/silver/sales_view/store'

# COMMAND ----------

def read_delta_file(delta_path):
    df = spark.read.format("delta").load(delta_path)
    return df
udf(read_delta_file)

# COMMAND ----------

product_df = read_delta_file(product_df)
store_df = read_delta_file(store_df)

# COMMAND ----------

merged_product_store_df = product_df.join(store_df, product_df.store_id == store_df.store_id, "inner")
product_store_df = merged_product_store_df.select(store_df.store_id,"store_name","location","manager_name","product_id","product_name","product_code","description","category_id","price","stock_quantity","supplier_id",product_df.created_at.alias("product_created_at"),product_df.updated_at.alias("product_updated_at"),"image_url","weight","expiry_date","is_active","tax_rate")


# COMMAND ----------

customer_sales_df = read_delta_file('dbfs:/mnt/silver/sales_view/customer')

# COMMAND ----------

merged_prodcust_custsale_df = product_store_df.join(customer_sales_df, product_store_df.product_id == customer_sales_df.product_id, "inner")
final_df = merged_prodcust_custsale_df.select("order_date", "category", "city", "customer_id", "order_id"), product_df.product_id.alias('product_id', "profit", "region", "sales", "segment", "ship_date", "ship_mode", "latitude", "longitude", "store_name", "location", "manager_name","product_name", "price", "stock_quantity", "image_url")



# COMMAND ----------

dbutils.fs.mount(
    source='wasbs://gold@adfassignsathya.blob.core.windows.net/',
    mount_point='/mnt/gold',
    extra_configs={'fs.azure.account.key.adfassignsathya.blob.core.windows.net':'XndDGisUTASVVfPUqy1+dLjRFFjEoTqCzA705ZpCjS7c6Gmb/OylPlkplDf1jmtbpm+Suh4TWN2V+AStsZMsyQ=='}
)

# COMMAND ----------

def write_delta_upsert(df, delta_path):
    df.write.format("delta").mode("overwrite").save(delta_path)

writeto = "dbfs:/mnt/gold/sales_view/StoreProductSalesAnalysi"
write_delta_upsert(product_store_df, writeto)
