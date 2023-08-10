# Databricks notebook source
database = "sample_analytics"
collection = "accounts"

connectionString = 'mongodb+srv://MUmarAmanat:mongodbwithdatabricks@cluster0.ovt3ktq.mongodb.net/'

# COMMAND ----------

data_sdf = (spark
                .read
                .format("mongo")
                .option("spark.mongodb.input.uri", connectionString)
                .option("database", database)
                .option("collection", collection)
                .load()
            )


# COMMAND ----------

data_sdf.printSchema()

# COMMAND ----------

data_sdf.limit(5).display()

# COMMAND ----------


