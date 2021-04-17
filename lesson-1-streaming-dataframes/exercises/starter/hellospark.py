from pyspark.sql import SparkSession

# TO-DO: create a variable with the absolute path to the text file
path="/home/workspace/lesson-1-streaming-dataframes/exercises/starter/Test.txt"

# TO-DO: create a Spark session
spark = SparkSession.builder.appName('abc').getOrCreate()


# TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel("WARN")

# TO-DO: using the Spark session variable, call the appropriate
# function referencing the text file path to read the text file 
df=spark.read.text(path).cache()

#df.show()


# TO-DO: call the appropriate function to filter the data containing
# the letter 'a', and then count the rows that were found
print(df.filter(df.value.contains("a")).count())
# TO-DO: call the appropriate function to filter the data containing
# the letter 'b', and then count the rows that were found
print(df.filter(df.value.contains("b")).count())


# TO-DO: print the count for letter 'd' and letter 's'
print(df.filter(df.value.contains("b")).count())
print(df.filter(df.value.contains("s")).count())

# TO-DO: stop the spark application
spark.stop()
