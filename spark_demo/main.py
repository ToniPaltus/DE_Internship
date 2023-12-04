import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]")\
    .appName('SparkExample.com')\
    .getOrCreate()

print("Info:", spark)
spark2 = SparkSession.newSession
print(spark2)

spark3 = SparkSession.builder.getOrCreate
print(spark3)

partitions = spark.conf.get('spark.sql.shuffle.partitions')
print(partitions, type(partitions))

#Create DataFrame
df = spark.createDataFrame(
    [('Scala', 25000),
     ('Spark', 35000),
     ('PHP', 21000)]
)

df.show()

#Spark SQL
df.createOrReplaceTempView('sample_table')
df2 = spark.sql("select _1, _2 from sample_table")
df2.show()

# Create Hive table & query it
# spark.table('sample_table').write.saveAsTable('sample_hive_table')
# df3 = spark.sql('select _1, _2 from sample_hive_table')
# df3.show()

# Get metadata from the Catalog
bds = spark.catalog.listDatabases()
print(bds)

tbls = spark.catalog.listTables()
print(tbls)

