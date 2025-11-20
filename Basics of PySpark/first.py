import dlt
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

@dlt.table(
    name ="customers_raw"
)
def customers_raw():
    df=spark.readStream.table("pyspark_cata.source.customers")
    return df

@dlt.table(
    name="customers_enr"
)
def customers_enr():
    df=spark.readStream.table("customers_raw")
    df=df.withColumn("dedup", row_number().over(Window.partitionBy("id").orderBy(col("modifiedDate").desc())))
    return df.where(col("dedup")==1).drop("dedup")


dlt.create_streaming_table(
    name= "customers_dim"
)

dlt.create_auto_cdc_flow(
  target = "customers_dim",
  source = "customers_enr",
  keys = ["id"],
  sequence_by = "modifiedDate",
  stored_as_scd_type = 2
)