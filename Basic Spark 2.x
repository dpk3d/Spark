#Spark 2.x With Scala
#spark2-shell

// Creating a Dataframe and finding the count of even number in the dataframe
val num = spark.range(100).toDF("Number")
val even = num.where("Number % 2 = 0")
even.count()

############## Creating a dataframe from CSV/JSON/PARQUET/ORC File ######################

val dataDf = spark.read.option("inferSchema", "true").option("header", "true").csv("/data/temperature/csv/1991-temperature.csv")

// Here we can pass the type of format like ORC,JSON,PARQUET etc. and pass respective path in LOAD.
val dataDf1 = spark.read.format("ORC").option("header", "true").option("inferSchema", "true").load("/data/temperature/ORC/*.orc")

dataDF.take(10)  //Displaying 10 Records
dataDF.sort("count").take(10)

#To Generate Physical Plan
dataDF.sort("count").explain()

#To get the Metrix out of DF like Min,Max,Median..etc

dataDF.describe()




spark.conf.set("spark.sql.shuffle.partitions", "5")
Narrow Dependencies 
Wide Dependencies
