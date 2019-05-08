/*
Launch the Spark2 Shell, and execute below
*/

import org.apache.spark.sql.functions._
val extend = spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("/FileStore/tables/ExtendedTracker.csv")

extend.take(5)
/*
 Array[org.apache.spark.sql.Row] = Array([1,LA/AP,Brazil,LAB,M12/17,43110,Manish,12:00AM - 2:00AM,2.0,null,null], [2,LA/AP,Dominican Republic,DOR,M12/17,43110,Manish,12:00AM - 2:00AM,2.0,null,null], [3,LA/AP,Mexico,LAM,M12/17,43115,Manish,12:00AM - 2:30AM,2.5,null,null], [4,LA/AP,Mexico,LAM,M12/17,43116,Manish,12:00AM - 2:00AM,2.0,null,null], [5,LA/AP,Ecuador,LAE,M12/17,43117,Deepak,12:00AM - 2:00AM,2.0,Regular,null])
*/

extend.show(false)
/*
+---+-----+------------------+------+------+-----+--------+----------------+-----+--------------+--------+
|SL |Team |countryName       |Audit |Period|Date |Resource|timeInIST       |Hours|ProductionType|Comments|
+---+-----+------------------+------+------+-----+--------+----------------+-----+--------------+--------+
|1  |LA/AP|Brazil            |LAB   |M12/17|43110|Manish  |12:00AM - 2:00AM|2.0  |null          |null    |
|2  |LA/AP|Dominican Republic|DOR   |M12/17|43110|Manish  |12:00AM - 2:00AM|2.0  |null          |null    |
|3  |LA/AP|Mexico            |LAM   |M12/17|43115|Manish  |12:00AM - 2:30AM|2.5  |null          |null    |
|4  |LA/AP|Mexico            |LAM   |M12/17|43116|Manish  |12:00AM - 2:00AM|2.0  |null          |null    |
|5  |LA/AP|Ecuador           |LAE   |M12/17|43117|Deepak  |12:00AM - 2:00AM|2.0  |Regular       |null    |
|6  |LA/AP|Mexico            |LCM   |M12/17|43117|Deepak  |12:00AM - 2:00AM|2.0  |null          |null    |
|7  |LA/AP|Argentina         |LAA   |M12/17|43118|Manish  |12:00AM - 2:00AM|2.0  |Rerun         |null    |
|8  |LA/AP|Argentina         |LAA   |M12/17|43119|Manish  |12:00AM - 2:00AM|2.0  |Rerun         |null    |
|9  |LA/AP|Peru              |LAK   |M12/17|43122|Manish  |12:00AM - 2:00AM|2.0  |Regular       |null    |
|10 |LA/AP|Ecuador           |LRE   |M12/17|43122|Manish  |12:00AM - 2:00AM|2.0  |Regular       |null    |
|11 |LA/AP|Mexico            |LRM   |M12/17|43123|Deepak  |12:00AM - 2:00AM|2.0  |null          |null    |
|12 |LA/AP|Bolivia           |PBO   |M12/17|43123|Deepak  |12:00AM - 2:00AM|2.0  |null          |null    |
|13 |LA/AP|Peru              |LAK   |M12/17|43123|Deepak  |12:00AM - 2:00AM|2.0  |null          |null    |
|14 |LA/AP|Colombia          |LAX   |M12/17|43123|Deepak  |12:00AM - 2:00AM|2.0  |null          |null    |
|15 |LA/AP|Argentina         |LCA   |M12/17|43123|Deepak  |12:00AM - 2:00AM|2.0  |null          |null    |
|16 |LA/AP|Central America   |LAG   |M12/17|43124|Manish  |12:00AM - 3:30AM|3.5  |null          |null    |
|17 |LA/AP|Mexico            |LRM   |M12/17|43125|Deepak  |12:00AM - 3:00AM|3.0  |null          |null    |
|18 |LA/AP|Colombia          |LAX   |M12/17|43125|Deepak  |12:00AM - 3:00AM|3.0  |null          |null    |
|19 |LA/AP|Uruguay           |LAU   |M12/17|43125|Deepak  |12:00AM - 2:00AM|2.0  |null          |null    |
|20 |LA/AP|Uruguay           |UMU   |M12/17|43125|Deepak  |12:00AM - 2:00AM|2.0  |null          |null    |
+---+-----+------------------+------+------+-----+--------+----------------+-----+--------------+--------+
*/

extend.explain()
/*
== Physical Plan ==
*FileScan csv [SL#132,Team#133,countryName#134,Audit #135,Period#136,Date#137,Resource#138,timeInIST#139,Hours#140,ProductionType#141,Comments#142] Batched: false, Format: CSV, Location: InMemoryFileIndex[dbfs:/FileStore/tables/ExtendedTracker.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<SL:int,Team:string,countryName:string,Audit :string,Period:string,Date:int,Resource:string...
import org.apache.spark.sql.functions._
extend: org.apache.spark.sql.DataFrame = [SL: int, Team: string ... 9 more fields]
*/

val dataFrameWay = extend.groupBy("Resource").count() // dataFrameWay: org.apache.spark.sql.DataFrame = [Resource: string, count: bigint]

val dataFrameWay1 = extend.groupBy("Resource").count().collect // dataFrameWay1: Array[org.apache.spark.sql.Row] = Array([Manish,13], [Deepti,4], [Ramesh,1], [Deepak,32])

dataFrameWay.explain
/*
== Physical Plan ==
*HashAggregate(keys=[Resource#405], functions=[finalmerge_count(merge count#461L) AS count(1)#434L])
+- Exchange hashpartitioning(Resource#405, 200)
   +- *HashAggregate(keys=[Resource#405], functions=[partial_count(1) AS count#461L])
      +- *FileScan csv [Resource#405] Batched: false, Format: CSV, Location: InMemoryFileIndex[dbfs:/FileStore/tables/ExtendedTracker.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<Resource:string>
*/

dataFrameWay.show
/*
+--------+-----+
|Resource|count|
+--------+-----+
|  Manish|   13|
|  Deepti|    4|
|  Ramesh|    1|
|  Deepak|   32|
+--------+-----+
*/

extend.select(max("countryName")).take(1) // Array[org.apache.spark.sql.Row] = Array([Uruguay])

extend.groupBy("countryName").sum("Hours").withColumnRenamed("sum(Hours)", "total_Hours_perCountry").sort(desc("total_Hours_perCountry")).limit(5)
// org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [countryName: string, total_Hours_perCountry: double]

extend.groupBy("countryName").sum("Hours").withColumnRenamed("sum(Hours)", "total_Hours_perCountry").sort(desc("total_Hours_perCountry")).limit(5).collect()
// Array[org.apache.spark.sql.Row] = Array([Mexico,48.0], [Brazil,16.0], [Central America,15.0], [Argentina,10.0], [Colombia,9.0])

extend.groupBy("countryName").sum("Hours").withColumnRenamed("sum(Hours)", "total_Hours_perCountry").sort(desc("total_Hours_perCountry")).limit(5).explain()
/*
== Physical Plan ==
TakeOrderedAndProject(limit=5, orderBy=[total_Hours_perCountry#728 DESC NULLS LAST], output=[countryName#667,total_Hours_perCountry#728])
+- *HashAggregate(keys=[countryName#667], functions=[finalmerge_sum(merge sum#736) AS sum(Hours#673)#723])
   +- Exchange hashpartitioning(countryName#667, 200)
      +- *HashAggregate(keys=[countryName#667], functions=[partial_sum(Hours#673) AS sum#736])
         +- *FileScan csv [countryName#667,Hours#673] Batched: false, Format: CSV, Location: InMemoryFileIndex[dbfs:/FileStore/tables/ExtendedTracker.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<countryName:string,Hours:double>
*/

extend.select(initcap(col("countryName"))).show(2, false)
/*
+--------------------+
|initcap(countryName)|
+--------------------+
|Brazil              |
|Dominican Republic  |
+--------------------+
*/

extend.where(col("Comments").equalTo("Rerun")).select("countryName", "Period").show(5, false)
/*
+-----------+------+
|countryName|Period|
+-----------+------+
+-----------+------+
*/

extend.withColumn("Rerun", not(col("Hours").leq(2))).filter("Rerun").select("countryName", "Resource").show(5)
/*
+---------------+--------+
|    countryName|Resource|
+---------------+--------+
|         Mexico|  Manish|
|Central America|  Manish|
|         Mexico|  Deepak|
|       Colombia|  Deepak|
|         Mexico|  Ramesh|
+---------------+--------+
*/

extend.select(round(col("Hours"), 1).alias("rounded"),col("countryName")).show(5)
/*
+-------+------------------+
|rounded|       countryName|
+-------+------------------+
|    2.0|            Brazil|
|    2.0|Dominican Republic|
|    2.5|            Mexico|
|    2.0|            Mexico|
|    2.0|           Ecuador|
+-------+------------------+
*/

extend.select(round(lit("2.5")),bround(lit("2.5"))).show(2)
/*
+-------------+--------------+
|round(2.5, 0)|bround(2.5, 0)|
+-------------+--------------+
|          3.0|           2.0|
|          3.0|           2.0|
+-------------+--------------+
*/

extend.describe().show()
/*
+-------+------------------+-----+-----------+------+------+------------------+--------+-----------------+------------------+--------------+----------------+
|summary|                SL| Team|countryName|Audit |Period|              Date|Resource|        timeInIST|             Hours|ProductionType|        Comments|
+-------+------------------+-----+-----------+------+------+------------------+--------+-----------------+------------------+--------------+----------------+
|  count|                50|   50|         50|    50|    50|                50|      50|               50|                50|            34|               8|
|   mean|              25.5| null|       null|  null|  null|          43137.28|    null|             null|              2.49|          null|            null|
| stddev|14.577379737113251| null|       null|  null|  null|18.285758928516046|    null|             null|1.6552020654744908|          null|            null|
|    min|                 1|LA/AP|  Argentina|   DOR|M01/18|             43110|  Deepak| 09:00AM - 5:00PM|               2.0|       Regular|No Input from LO|
|    max|                50|LA/AP|    Uruguay|   UMU|M12/17|             43175|  Ramesh|3:00 PM - 3:00 AM|              12.0|       regular|No Input from LO|
+-------+------------------+-----+-----------+------+------+------------------+--------+-----------------+------------------+--------------+----------------+
*/

extend.stat.crosstab("Resource", "Hours").show()
/*
+--------------+----+---+---+---+---+---+---+
|Resource_Hours|12.0|2.0|2.5|3.0|3.5|4.0|8.0|
+--------------+----+---+---+---+---+---+---+
|        Ramesh|   0|  0|  0|  0|  0|  0|  1|
|        Deepti|   0|  3|  0|  0|  0|  1|  0|
|        Manish|   0| 11|  1|  0|  1|  0|  0|
|        Deepak|   1| 25|  3|  3|  0|  0|  0|
+--------------+----+---+---+---+---+---+---+
*/

extend.select(monotonically_increasing_id()).show(5)
/*
+-----------------------------+
|monotonically_increasing_id()|
+-----------------------------+
|                            0|
|                            1|
|                            2|
|                            3|
|                            4|
+-----------------------------+
*/

extend.select(col("Resource"),lower(col("Resource")),upper(lower(col("Resource")))).show(5)
/*
+--------+---------------+----------------------+
|Resource|lower(Resource)|upper(lower(Resource))|
+--------+---------------+----------------------+
|  Manish|         manish|                MANISH|
|  Manish|         manish|                MANISH|
|  Manish|         manish|                MANISH|
|  Manish|         manish|                MANISH|
|  Deepak|         deepak|                DEEPAK|
+--------+---------------+----------------------+
*/
