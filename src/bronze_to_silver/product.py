# Databricks notebook source file
dbutils.fs.mount(
    source='wasbs://bronze@adfassignsathya.blob.core.windows.net/',
    mount_point='/mnt/bronze',
    extra_configs={'fs.azure.account.key.adfassignsathya.blob.core.windows.net':'XndDGisUTASVVfPUqy1+dLjRFFjEoTqCzA705ZpCjS7c6Gmb/OylPlkplDf1jmtbpm+Suh4TWN2V+AStsZMsyQ=='}
)

# COMMAND ----------

product_df = spark.read.csv("dbfs:/mnt/bronze/products/20240106_sales_product.csv", header=True)


# COMMAND ----------

from pyspark.sql.functions import udf
def toSnakeCase(product_df):
    for column in product_df.columns:
        snake_case_col = ''
        for char in column:
            if char ==' ':
                snake_case_col += '_'
            else:
                snake_case_col += char.lower()
        product_df = product_df.withColumnRenamed(column, snake_case_col)
    return product_df

udf(toSnakeCase)


# COMMAND ----------

product_df = toSnakeCase(product_df)

# COMMAND ----------

# Create column "sub_category" based on "category_id"
product_df = product_df.withColumn("sub_category", when(product_df["category_id"] == 1, "phone").when(product_df["category_id"] == 2, "laptop").when(product_df["category_id"] == 3, "playstation").when(product_df["category_id"] == 4, "e-device").otherwise("unknown"))

# COMMAND ----------

def write_delta_upsert(df, delta_path):
    df.write.format("delta").mode("overwrite").save(delta_path)

writeto = "dbfs:/mnt/silver/sales_view/product"
write_delta_upsert(product_df, writeto)
