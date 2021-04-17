from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, IntegerType

# TO-DO: create a kafka message schema StructType including the following JSON elements:
# {"truckNumber":"5169","destination":"Florida","milesFromShop":505,"odomoterReading":50513}
truckStatusSchema = StructType (
        [
          StructField("truckNumber", StringType()),
          StructField("destination", StringType()),
          StructField("milesFromShop", IntegerType()),
          StructField("odometerReading", IntegerType())
        ]
)


# TO-DO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName('vehicle-status').getOrCreate()

#TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel("WARN")

#TO-DO: read the vehicle-status kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible
vehicle_status_rawDF = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "vehicle-status") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
vedhicleStatusStreamingDF = vehicle_status_rawDF.selectExpr("cast(key as string) key", "cast(value as string) value")



#TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe
vedhicleStatusStreamingDF = vedhicleStatusStreamingDF \
  .withColumn("value", from_json("value",truckStatusSchema)).select(col('value.*'))

# TO-DO: create a temporary streaming view called "VehicleStatus"
# it can later be queried with spark.sql
vedhicleStatusStreamingDF.createOrReplaceTempView("VehicleStatusView")



#TO-DO: using spark.sql, select * from VehicleStatus
selectDF = spark.sql("select * from VehicleStatusView")


# TO-DO: write the stream to the console, and configure it to run indefinitely, the console output will look something like this:

selectDF.writeStream.format("console") \
    .start() \
    .awaitTermination()


# +-----------+------------+-------------+---------------+
# |truckNumber| destination|milesFromShop|odometerReading|
# +-----------+------------+-------------+---------------+
# |       9974|   Tennessee|          221|         335048|
# |       3575|      Canada|          354|          74000|
# |       1444|      Nevada|          257|         395616|
# |       5540|South Dakota|          856|         176293|
# +-----------+------------+-------------+---------------+


