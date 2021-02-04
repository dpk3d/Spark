package com.singh.deepak.spark.read

import org.apache.spark.sql.SparkSession

/*
* Reading Json Data from a flat file
*
**/

object JsonDump {

@transient lazy val logger: Logger = Logger.getLogger(this.getClass)

val spark = SparkSession.builder().appName("Read Flat File").master("local").getOrCreate()
val sc = spark.sparkContext

val jsonDump = sc.textfile("Path to the flat file")

val cnt = jsonDump.count

val removeHeaderAndFooter = jsonDump.take(cnt.toInt).drop(1).dropRight(1)
val rdd = sc.parallelize(removeHeaderAndFooter)
val splitRdd = rdd.map(row => row.split("#")).map(x => (x(0) , x(1))
val filterRdd = splitRdd.map(record => record._2.trim)

val df = spark.read.option("multiLine", true).option("mode", "PERMISSIVE").json(filterRdd)
df.printSchema
df.show(false)

}
