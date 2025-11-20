# Databricks notebook source
# MAGIC %md
# MAGIC ## **Data Reading**

# COMMAND ----------

df =spark.read.format("csv").option("inferschema", True).option("header", True).load('/workspace/fileserver/bigmart_data.csv/')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Json

# COMMAND ----------

df_json =spark.read.format("json").option("inferschema", True).option("header", True).option("multiline", False).load('/Volumes/workspace/sales/rawjson/raw_nyc_phil.json')

# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Schema Defination**

# COMMAND ----------

df.printSchema()
df_json.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DDL SCHEMA

# COMMAND ----------

my_ddl_schema= '''
                     Item_Identifier string,
                     Item_Weight string, 
                     Item_Fat_Content string, 
                     Item_Visibility double,
                     Item_Type string, 
                     Item_MRP double, 
                     Outlet_Identifier string, 
                     Outlet_Establishment_Year integer,
                     Outlet_Size string, 
                     Outlet_Location_Type string, 
                     Outlet_Type string,
                     Item_Outlet_Sales double 
               '''

# COMMAND ----------

df = spark.read.format('csv').schema(my_ddl_schema).option('header', True).load('/Volumes/workspace/sales/bigdatamart/bigmart_data.csv')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### StructType() Schema

# COMMAND ----------

from pyspark.sql.types import * 
from pyspark.sql.functions import *


# COMMAND ----------

my_struct_schema= StructType([
                               StructField('item_identifier',StringType(), True),
                               StructField('item_weight',StringType(), True),
                               StructField('item_fat_content',StringType(), True),
                               StructField('item_visibility',StringType(), True),
                               StructField('item_mrp',StringType(), True),
                               StructField('outlet_identifier',StringType(), True),
                               StructField('outlet_establishment_year',StringType(), True),
                               StructField('outlet_size',StringType(), True),
                               StructField('outlet_location_type',StringType(), True),
                               StructField('outlet_type',StringType(), True),
                               StructField('item_outlet_sales',StringType(), True)
])


# COMMAND ----------

df = spark.read.format('csv').schema(my_struct_schema).option('header', True).load('/Volumes/workspace/sales/bigdatamart/bigmart_data.csv')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### Select

# COMMAND ----------

df.display()

# COMMAND ----------

df.select(col('Item_Identifier'), col('Item_Weight'), col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### Alias

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### **Filter/ Where**

# COMMAND ----------

# MAGIC %md
# MAGIC **Senario- 1**

# COMMAND ----------

df.filter(col('item_fat_content')=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Senario -2**

# COMMAND ----------

df.filter((col('Item_Type')=='Soft Drinks') & (col('Item_Weight')<10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Senario -3**

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1', 'Tier  2') )) .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### **WithColumnRenamed**

# COMMAND ----------

df.withColumnRenamed('Item_Weight', 'Item_Wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC **Senario -1**

# COMMAND ----------

df = df.withColumn('flag',lit("new"))

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('Multiply', col("Item_Weight")* col("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Senario 2**

# COMMAND ----------

df.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), "Regular", "Reg"))\
   .withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), "Low Fat", "LF"))\
   .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Type Casting**

# COMMAND ----------

df= df.withColumn('Item_Weight', col('Item_Weight').cast(StringType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Sort/ OrderBy**

# COMMAND ----------

# MAGIC %md
# MAGIC **Senario 1**

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Senario 2**

# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Senario 3**

# COMMAND ----------

df.sort(['Item_Weight', 'Item_Visibility'],ascending=[0,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Senario 4**

# COMMAND ----------

df.sort(['Item_Weight', 'Item_Visibility'],ascending=[0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### **Limit**

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Drop**

# COMMAND ----------

# MAGIC %md
# MAGIC **Senario 1**

# COMMAND ----------

df.drop('Item_Visibility').display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Senario 2**

# COMMAND ----------

df.drop('Item_Visibility', 'Item_Type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Drop Duplicates**

# COMMAND ----------

# MAGIC %md
# MAGIC **Senario 1**

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Senario 2**

# COMMAND ----------

df.drop_duplicates(subset=['Item_Type']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Union and Union BY NAME**

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing Dataframes**

# COMMAND ----------

data1 = [('1', 'Kad'),
         ('2', 'sid')]
Schema1 = 'id String, name String'

df1=spark.createDataFrame(data1,Schema1)

data2 = [('3', 'Raj'),
         ('4', 'Rag')]
Schema2 = 'id String, name String'

df2=spark.createDataFrame(data2,Schema2)


# COMMAND ----------

df1.display()
df2.display()  

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

data1 = [('Kad', '1'),
         ('sid', '2')]
Schema1 = 'name String, id String'

df1=spark.createDataFrame(data1,Schema1).display()

data2 = [('3', 'Raj'),
         ('4', 'Rag')]
Schema2 = 'id String, name String'

df2=spark.createDataFrame(data2,Schema2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### Union by Name

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### String Functions

# COMMAND ----------

# MAGIC %md
# MAGIC **initcap()**

# COMMAND ----------

#df.select(initcap('Item_Type')).display()

#df.select(lower('Item_Type')).display()

df.select(upper('Item_Type').alias('upper_item_type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### Date Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Current_date()

# COMMAND ----------

df=df.withColumn('curr_date', current_date())
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### **Date_add()**

# COMMAND ----------

df =df.withColumn('week_after', date_add('curr_date', 7))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### **Date_sub()**

# COMMAND ----------

df =df.withColumn('week_before', date_sub('curr_date', 7))
df.display()

df =df.withColumn('week_before', date_add('curr_date', -7))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### DATEDIFF

# COMMAND ----------

df =df.withColumn('datediff', datediff('curr_date', 'week_after'))
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## ### DATE_FORMAT

# COMMAND ----------

df.withColumn('week_before', date_format('week_before', 'MM-dd-yyyy')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### Handling NULLs

# COMMAND ----------

# MAGIC %md
# MAGIC **Dropping NULLs**

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

df.dropna(subset=['Outlet_Size']).display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### Filling Nulls

# COMMAND ----------

df.fillna('N/A').display()

# COMMAND ----------

df.fillna('N/A', subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### SPLIT and Indexing

# COMMAND ----------

# MAGIC %md
# MAGIC **split**

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### **Indexing**

# COMMAND ----------

df.withColumn('Outlet_Type', split('Outlet_Type', ' ')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### EXPLODE

# COMMAND ----------

df_exxp=df.withColumn('Outlet_Type', split('Outlet_Type', ' '))
df_exxp.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Explode splits list in to new rows**

# COMMAND ----------

df_exxp.withColumn('Outlet_Type', explode('Outlet_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### Array_Contains

# COMMAND ----------

df_exxp.display()

# COMMAND ----------

df_exxp.withColumn('Type_flag', array_contains('Outlet_Type', 'Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### Group_By

# COMMAND ----------

# MAGIC %md
# MAGIC **Senario 1**

# COMMAND ----------

df.display()

# COMMAND ----------

df.groupBy('Item_Type').agg(sum("Item_MRP")).display()

# COMMAND ----------

df.groupBy('Item_Type').agg(avg("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Senario 2**

# COMMAND ----------

df.groupBy('Item_Type', 'Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type', 'Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP'),avg('Item_MRP').alias('Avg_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### Collect_ List

# COMMAND ----------

# MAGIC %md
# MAGIC **Collect_list will aggregate values instead numbers**

# COMMAND ----------

data =[('user1','book1'),
       ('user1','book2'),
       ('user2','book2'),
       ('user2','book4'),
       ('user3','book1')]

schema ='user string, book string'

df_book = spark.createDataFrame(data, schema)
df_book.display()

# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### Pivot

# COMMAND ----------

df.select(col('Item_Type'), col('Outlet_Size'), col('Item_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### When-Otherwise

# COMMAND ----------

# MAGIC %md
# MAGIC **Senario 1**

# COMMAND ----------

from pyspark.sql.types import * 
from pyspark.sql.functions import *

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn('veg_flag', when(col('Item_Type')=='Meat', 'Non-veg').otherwise('Veg'))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Senario 2**

# COMMAND ----------

df=df.withColumn('veg_exp_flag', when(((col('veg_flag')=='Veg')& (col('Item_MRP')<100)),'Veg_Inexpensive')\
                             .when((col('veg_flag')=='Veg')& (col('Item_MRP')>100),'Veg_expensive')\
                             .otherwise('Non-veg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### **Joins**

# COMMAND ----------

dataj1 =[('1','gaur', 'd01'),
         ('2','sita', 'd02'),
         ('3','ram', 'd03'),
         ('4','shyam', 'd03'),
         ('5','rita', 'd05'),
         ('6','sham', 'd06')]
schemaj1 = 'emp_id string, emp_name string, dept_id string'

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 =[('d01', 'HR'),
         ('d02', 'MARKETING'),
         ('d03', 'Accounts'),
         ('d04', 'IT'),
         ('d05', 'FINANCE')]     
schemaj2 = 'dept_id string, department string'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

df1.display()
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Inner join**

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], "inner").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **left join**

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], "left").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Right join**

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], "right").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Anti join**

# COMMAND ----------

# MAGIC %md
# MAGIC **Anti join is used to get the records which are not available in other table**

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], "Anti").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### Window Functions

# COMMAND ----------

# MAGIC %md
# MAGIC **Row number()**

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df.withColumn('rowCol', row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Rank() VS Dense_rank()**

# COMMAND ----------

df.withColumn('rank', rank().over(Window.orderBy(col('Item_Identifier').desc())))\
    .withColumn('denseRank', dense_rank().over(Window.orderBy(col('Item_Identifier').desc())))\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Cumulative Sum**

# COMMAND ----------

df.withColumn('cumsum', sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()

# COMMAND ----------

df.withColumn('cumsum', sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding, Window.currentRow))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### User Defined Functions

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1**

# COMMAND ----------

def my_func(x):
  return x*x

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2**

# COMMAND ----------

my_udf= udf(my_func)

# COMMAND ----------

df.withColumn('mynewcol', my_udf('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### Data Writing

# COMMAND ----------

# MAGIC %md
# MAGIC ### **csv**

# COMMAND ----------

df.write.format('csv')\
    .save('/Fileserver/tables/CSV/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Modes of writing
# MAGIC - Append
# MAGIC - error
# MAGIC - overwrite
# MAGIC - Ignore

# COMMAND ----------

df.write.format('csv')\
  .mode('append')\
    .save('/Fileserver/tables/CSV/data.csv')




# COMMAND ----------

df.write.format('csv')\
    .mode('overwrite')\
     .option('path','/Fileserver/tables/CSV/data.csv' )\
      .save()   

# COMMAND ----------

df.write.foramt('csv')\
    .mode('error')\
    .option('path'.'/fileserver/tables/CSV/data.csv' )\
      .save()   

# COMMAND ----------

df.write.foramt('csv')\
    .mode('ignore')\
    .option('path'.'/fileserver/tables/CSV/data.csv' )\
      .save()   

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parquet

# COMMAND ----------

df.write.format('parquet')\
    .mode('overwrite')\
     .option('path','/Volumes/workspace/sales/bigdatamart/bigmart_data.csv' )\
      .save()   

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table

# COMMAND ----------

df.write.format('parquet')\
    .mode('overwrite')\
     .saveAsTable('my_table')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ## CraetTempView

# COMMAND ----------

df.createTempView('my_view')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_view where Item_Fact_Content ='LF'

# COMMAND ----------

df_sql = spark.sql("select * from my_view where Item_Fact_Content ='LF'")


# COMMAND ----------

df_sql.display()

# COMMAND ----------

