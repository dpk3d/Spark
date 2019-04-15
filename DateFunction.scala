
import org.apache.spark.sql.functions._
val dateDF = spark.range(10).withColumn("today", current_date()).withColumn("now", current_timestamp())
//dateDF.printSchema()
/*
root
 |-- id: long (nullable = false)
 |-- today: date (nullable = false)
 |-- now: timestamp (nullable = false)
*/

dateDF.take(8).foreach(println)
/*
[0,2019-04-15,2019-04-15 08:45:07.878]
[1,2019-04-15,2019-04-15 08:45:07.878]
[2,2019-04-15,2019-04-15 08:45:07.878]
[3,2019-04-15,2019-04-15 08:45:07.878]
[4,2019-04-15,2019-04-15 08:45:07.878]
[5,2019-04-15,2019-04-15 08:45:07.878]
[6,2019-04-15,2019-04-15 08:45:07.878]
[7,2019-04-15,2019-04-15 08:45:07.878]
*/

dateDF.createOrReplaceTempView("dateTable")
dateDF.select(date_sub(col("today"), 5),date_add(col("today"), 5)).show(3)
/*
+------------------+------------------+
|date_sub(today, 5)|date_add(today, 5)|
+------------------+------------------+
|        2019-04-10|        2019-04-20|
|        2019-04-10|        2019-04-20|
|        2019-04-10|        2019-04-20|
+------------------+------------------+
*/

dateDF.withColumn("week_ago", date_sub(col("today"), 7)).select(datediff(col("week_ago"), col("today"))).show(3)
/*
+-------------------------+
|datediff(week_ago, today)|
+-------------------------+
|                       -7|
|                       -7|
|                       -7|
+-------------------------+
*/

dateDF.select(to_date(lit("2019-01-01")).alias("start"),to_date(lit("2019-03-22")).alias("end")).select(months_between(col("start"), col("end"))).show(4)
/*
+--------------------------+
|months_between(start, end)|
+--------------------------+
|               -2.67741935|
|               -2.67741935|
|               -2.67741935|
|               -2.67741935|
+--------------------------+
*/
spark.range(5).withColumn("date", lit("2018-01-01")).select(to_date(col("date"))).show(3)
/*
+---------------+
|to_date(`date`)|
+---------------+
|     2018-01-01|
|     2018-01-01|
|     2018-01-01|
+---------------+
*/

val dateFormat = "yyyy-dd-MM"
val cleanDateDF = spark.range(2).select(to_date(lit("2017-12-11"), dateFormat).alias("date"),to_date(lit("2017-20-12"), dateFormat).alias("date2")).show(2)
/*
+----------+----------+
|      date|     date2|
+----------+----------+
|2017-11-12|2017-12-20|
|2017-11-12|2017-12-20|
+----------+----------+
*/

//cleanDateDF.select(to_timestamp(col("date"))).show()
