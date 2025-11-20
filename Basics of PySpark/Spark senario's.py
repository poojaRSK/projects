# Databricks notebook source
# MAGIC %md
# MAGIC ## ### Senario 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Querying Source

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pyspark_cata.source.products

# COMMAND ----------


from pyspark.sql.window import Window
from pyspark.sql.functions import *


# COMMAND ----------

# df=spark.read.table("pyspark_cata.source.products")

df =spark.sql("select * from pyspark_cata.source.products")

#dedup
df=df.withColumn("dedup",row_number().over(Window.partitionBy("id").orderBy(desc("updatedDate"))))

df=df.filter(col('dedup')==1).drop('dedup')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Upserts**

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### Ways to read table data
# MAGIC
# MAGIC - if spark.catalog_tableExists("/Volumes/pyspark_cata/source/destination/products_sink"): or
# MAGIC
# MAGIC - if dbutils.fs.ls("/Volumes/pyspark_cata/source/destination/products_sink/"):

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating delta objects

# COMMAND ----------

from delta.tables import DeltaTable

if len(dbutils.fs.ls("/Volumes/pyspark_cata/source/destination/products_sink/")) > 0:
    dlt_obj = DeltaTable.forPath(spark,"/Volumes/pyspark_cata/source/destination/products_sink/")
    dlt_obj.alias("trg").merge\
    (df.alias("src"),\
      "src.id = trg.id")\
        .whenMatchedUpdateAll(condition="src.updatedDate >= trg.updatedDate")\
      .whenNotMatchedInsertAll().execute()
    print("This is upserting now")

else:
    df.write.format("delta")\
      .mode("Overwrite")\
        .save("/Volumes/pyspark_cata/source/destination/products_sink")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/pyspark_cata/source/destination/products_sink/`

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### Senario 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming Query

# COMMAND ----------

my_schema ="""
            order_id int,
            customer_id int,
            order_date date,
            amount double
           """

# COMMAND ----------

# To check batch processing

df_batch= spark.read.format("csv")\
    .option("header","true")\
    .schema(my_schema)\
    .load("/Volumes/pyspark_cata/source/destination/StreamSource/")
display(df_batch)

# COMMAND ----------

df= spark.readStream.format("csv")\
    .option("header","true")\
    .schema(my_schema)\
    .load("/Volumes/pyspark_cata/source/destination/StreamSource/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming Output

# COMMAND ----------

df.writeStream.format("delta")\
   .option("checkpointLocation","/Volumes/pyspark_cata/source/destination/StreamSink/checkpoint")\
   .option("mergeschema", True)\
    .trigger(once=True)\
    .start("/Volumes/pyspark_cata/source/destination/StreamSink/data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/pyspark_cata/source/destination/StreamSink/data/`

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### Senario 3

# COMMAND ----------

df= spark.read.format("json")\
    .option("inferschema", True)\
    .option("multiline", True)\
    .load("/Volumes/pyspark_cata/source/destination/JsonData/")

display(df)

# COMMAND ----------

df.schema

# COMMAND ----------

from pyspark.sql.functions import * 

# COMMAND ----------

df_cust=df.select("customer.customer_id", "customer.email","customer.location.city", "customer.location.country", "*").drop("customer")

df_cust_upd =df_cust.withColumn("delivery_updates", explode("delivery_updates"))\
    .withColumn("items", explode("items"))\
        .select("*")

display(df_cust_upd)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ###  Senario 4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Python class

# COMMAND ----------


  from pyspark.sql.functions import *
  from pyspark.sql.window import Window

# COMMAND ----------

class DataValidation:
  
  def __init__(self,df):
    self.df=df

  def dedup(self, keyCol, cdcCol):
    df=self.df.withColumn("dedup",row_number().over(Window.partitionBy(keyCol).orderBy(desc(cdcCol))))
    df=df.filter(col('dedup')==1).drop('dedup')
    return df
      
  def removeNulls(self, nullCol):
     df=self.df.filter(col('nullCol').isNotNull())
     return df

# COMMAND ----------

df=spark.createDataFrame([("1","2020-01-01", 100), ("2","2020-01-02", 200), ("3","2020-01-03", 300),("2","2020-01-13", 500)],["order_id", "order_date", "amount"])

display(df)

# COMMAND ----------

cls_obj = DataValidation(df)

# COMMAND ----------

df_dedup=cls_obj.dedup("order_id", "order_date")
display(df_dedup)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ###  Senario 5

# COMMAND ----------

# MAGIC %sql
# MAGIC create table pyspark_cata.source.customers
# MAGIC (
# MAGIC   id string,
# MAGIC   email string,
# MAGIC   city string,
# MAGIC   country string,
# MAGIC   modifiedDate timestamp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into pyspark_cata.source.customers
# MAGIC values
# MAGIC ('1','john.smith@example.com', 'Seattle', 'USA', current_timestamp),
# MAGIC ('6','jane.doe@example.com', 'London', 'UK',current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pyspark_cata.source.customers

# COMMAND ----------

if spark.catalog.tableExists("pyspark_cata.source.Dimcustomers"):

    pass

else:

    spark.sql("""
              create table pyspark_Cata.source.Dimcustomers
              select *, current_timestamp() as startTime,
              cast('3000-01-01' as Timestamp)as endTime,
              'Y' as isActive
               from pyspark_cata.source.customers
              """
            )

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable


# COMMAND ----------

df= spark.sql("""
              select * from pyspark_cata.source.customers
              """)
df= df.withColumn("dedup", row_number().over(Window.partitionBy("id").orderBy(desc("modifiedDate"))))\
    .drop('dedup')
df=df.filter(col('dedup')==1)

df.createOrReplaceTempView('srctemp')

df=spark.sql("""
              select *, current_timestamp() as startTime,
              cast('3000-01-01' as Timestamp)as endTime,
              'Y' as isActive
               from srctemp """)

df.createOrReplaceTempView('src')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from src

# COMMAND ----------

# MAGIC %md
# MAGIC ### **MERGE 1 -marking the updated records as expired**

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into pyspark_cata.source.Dimcustomers trg
# MAGIC using src
# MAGIC on trg.id=src.id
# MAGIC and trg.isActive='Y'
# MAGIC when matched and src.email <> trg.email
# MAGIC or src.city <> trg.city 
# MAGIC or src.country<> trg.country 
# MAGIC or src.modifiedDate <> trg.modifiedDate 
# MAGIC then update set 
# MAGIC trg.endTime=current_timestamp,
# MAGIC trg.isActive='N'
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### **MERGE 2 -Inserting new + Updated records**

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE into pyspark_cata.source.Dimcustomers as trg
# MAGIC USING src
# MAGIC ON src.id = trg.id
# MAGIC and trg.isActive = 'Y'
# MAGIC
# MAGIC WHEN not MATCHED then insert *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pyspark_Cata.source.Dimcustomers