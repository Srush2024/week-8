# week-8
#task 1
#step 1
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "<your-client-id>",
  "fs.azure.account.oauth2.client.secret": "<your-client-secret>",
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<your-tenant-id>/oauth2/token"
}

dbutils.fs.mount(
  source = "abfss://<your-container-name>@<your-storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<your-mount-point>",
  extra_configs = configs
)

#step2
# Load data into a DataFrame
taxi_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/mnt/<your-mount-point>/path/to/nyc_taxi_data.csv")

#step3
from pyspark.sql.functions import col, sum as sum_

taxi_df = taxi_df.withColumn("Revenue", 
    col("Fare_amount") + 
    col("Extra") + 
    col("MTA_tax") + 
    col("Improvement_surcharge") + 
    col("Tip_amount") + 
    col("Tolls_amount") + 
    col("Total_amount")
)
taxi_df.show()

#step 4
# Assuming there's a column 'Date' in 'yyyy-MM-dd' format and 'VendorID'
from pyspark.sql.functions import desc

date = "2023-01-01"  # Example date
highest_gaining_vendors = taxi_df.filter(col("Date") == date) \
    .groupBy("VendorID") \
    .agg(
        sum_("Passenger_count").alias("Total_passengers"),
        sum_("Trip_distance").alias("Total_distance")
    ) \
    .orderBy(desc("Total_distance")) \
    .limit(2)

highest_gaining_vendors.show()


