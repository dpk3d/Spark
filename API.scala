/* 
Some of the API of Dataframe
*/

val itPostsRows = sc.textFile("/FileStore/tables/italianPosts.csv")
// itPostsRows: org.apache.spark.rdd.RDD[String] = /FileStore/tables/italianPosts.csv MapPartitionsRDD[10] at textFile at command-1275360516494518:1

val itPostsSplit = itPostsRows.map(x => x.split("~"))
// itPostsSplit: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[11] at map at command-1275360516494518:2

val itPostsRDD = itPostsSplit.map(x => (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12)))
// itPostsRDD: org.apache.spark.rdd.RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)] = MapPartitionsRDD[12] at map at command-1275360516494518:3

val itPostsDFrame = itPostsRDD.toDF()
// itPostsDFrame: org.apache.spark.sql.DataFrame = [_1: string, _2: string ... 11 more fields]

itPostsDFrame.show(10)
/* 
+---+--------------------+---+--------------------+---+--------------------+----+--------------------+--------------------+----+----+---+----+
| _1|                  _2| _3|                  _4| _5|                  _6|  _7|                  _8|                  _9| _10| _11|_12| _13|
+---+--------------------+---+--------------------+---+--------------------+----+--------------------+--------------------+----+----+---+----+
|  4|2013-11-11 18:21:...| 17|&lt;p&gt;The infi...| 23|2013-11-10 19:37:...|null|                    |                    |null|null|  2|1165|
|  5|2013-11-10 20:31:...| 12|&lt;p&gt;Come cre...|  1|2013-11-10 19:44:...|  61|Cosa sapreste dir...| &lt;word-choice&gt;|   1|null|  1|1166|
|  2|2013-11-10 20:31:...| 17|&lt;p&gt;Il verbo...|  5|2013-11-10 19:58:...|null|                    |                    |null|null|  2|1167|
|  1|2014-07-25 13:15:...|154|&lt;p&gt;As part ...| 11|2013-11-10 22:03:...| 187|Ironic constructi...|&lt;english-compa...|   4|1170|  1|1168|
|  0|2013-11-10 22:15:...| 70|&lt;p&gt;&lt;em&g...|  3|2013-11-10 22:15:...|null|                    |                    |null|null|  2|1169|
|  2|2013-11-10 22:17:...| 17|&lt;p&gt;There's ...|  8|2013-11-10 22:17:...|null|                    |                    |null|null|  2|1170|
|  1|2013-11-11 09:51:...| 63|&lt;p&gt;As other...|  3|2013-11-11 09:51:...|null|                    |                    |null|null|  2|1171|
|  1|2013-11-12 23:57:...| 63|&lt;p&gt;The expr...|  1|2013-11-11 10:09:...|null|                    |                    |null|null|  2|1172|
|  9|2014-01-05 11:13:...| 63|&lt;p&gt;When I w...|  5|2013-11-11 10:28:...| 122|Is &quot;scancell...|&lt;usage&gt;&lt;...|   3|1181|  1|1173|
|  0|2013-11-11 10:58:...| 18|&lt;p&gt;Wow, wha...|  5|2013-11-11 10:58:...|null|                    |                    |null|null|  2|1174|
+---+--------------------+---+--------------------+---+--------------------+----+--------------------+--------------------+----+----+---+----+
*/

val itPostsDF = itPostsRDD.toDF("commentCount", "lastActivityDate","ownerUserId", "body", "score", "creationDate", "viewCount", "title","tags", "answerCount", "acceptedAnswerId", "postTypeId", "id")
// itPostsDF: org.apache.spark.sql.DataFrame = [commentCount: string, lastActivityDate: string ... 11 more fields]
itPostsDF.printSchema
/*
root
 |-- commentCount: string (nullable = true)
 |-- lastActivityDate: string (nullable = true)
 |-- ownerUserId: string (nullable = true)
 |-- body: string (nullable = true)
 |-- score: string (nullable = true)
 |-- creationDate: string (nullable = true)
 |-- viewCount: string (nullable = true)
 |-- title: string (nullable = true)
 |-- tags: string (nullable = true)
 |-- answerCount: string (nullable = true)
 |-- acceptedAnswerId: string (nullable = true)
 |-- postTypeId: string (nullable = true)
 |-- id: string (nullable = true)
*/

import java.sql.Timestamp
case class Post(commentCount:Option[Int],lastActivityDate:Option[java.sql.Timestamp],ownerUserId:Option[Long],body:String,score:Option[Int],creationDate:Option[java.sql.Timestamp],viewCount:Option[Int],title:String,tags:String,answerCount:Option[Int],acceptedAnswerId:Option[Long],postTypeId:Option[Long],id:Long)
// defined case class Post

object StringImplicits {
implicit class StringImprovements(val s: String) {
import scala.util.control.Exception.catching
def toIntSafe = catching(classOf[NumberFormatException]) opt s.toInt
def toLongSafe = catching(classOf[NumberFormatException]) opt s.toLong
def toTimestampSafe = catching(classOf[IllegalArgumentException]) opt
Timestamp.valueOf(s)
}
}
// defined object StringImplicits	


import StringImplicits._
// import StringImplicits._

def stringToPost(row:String):Post = {
val r = row.split("~")
Post(r(0).toIntSafe,r(1).toTimestampSafe,r(2).toLongSafe,r(3),r(4).toIntSafe,r(5).toTimestampSafe,r(6).toIntSafe,r(7),r(8),r(9).toIntSafe,r(10).toLongSafe,r(11).toLongSafe,r(12).toLong)
}
// defined stringToPost: (row: String)Post

val itPostsDFCase = itPostsRows.map(x => stringToPost(x)).toDF()
// itPostsDFCase: org.apache.spark.sql.DataFrame = [commentCount: int, lastActivityDate: timestamp ... 11 more fields]


itPostsDFCase.printSchema
/* 
root
 |-- commentCount: integer (nullable = true)
 |-- lastActivityDate: timestamp (nullable = true)
 |-- ownerUserId: long (nullable = true)
 |-- body: string (nullable = true)
 |-- score: integer (nullable = true)
 |-- creationDate: timestamp (nullable = true)
 |-- viewCount: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- tags: string (nullable = true)
 |-- answerCount: integer (nullable = true)
 |-- acceptedAnswerId: long (nullable = true)
 |-- postTypeId: long (nullable = true)
 |-- id: long (nullable = false)
*/


import org.apache.spark.sql.types._
// import org.apache.spark.sql.types._

val postSchema = StructType(Seq(
StructField("commentCount", IntegerType, true),
StructField("lastActivityDate", TimestampType, true),
StructField("ownerUserId", LongType, true),
StructField("body", StringType, true),
StructField("score", IntegerType, true),
StructField("creationDate", TimestampType, true),
StructField("viewCount", IntegerType, true),
StructField("title", StringType, true),
StructField("tags", StringType, true),
StructField("answerCount", IntegerType, true),
StructField("acceptedAnswerId", LongType, true),
StructField("postTypeId", LongType, true),
StructField("id", LongType, false))
)
// postSchema: org.apache.spark.sql.types.StructType = StructType(StructField(commentCount,IntegerType,true), StructField(lastActivityDate,TimestampType,true), StructField(ownerUserId,LongType,true), StructField(body,StringType,true), StructField(score,IntegerType,true), StructField(creationDate,TimestampType,true), StructField(viewCount,IntegerType,true), StructField(title,StringType,true), StructField(tags,StringType,true), StructField(answerCount,IntegerType,true), StructField(acceptedAnswerId,LongType,true), StructField(postTypeId,LongType,true), StructField(id,LongType,false))

def stringToRow(row:String):Row = {
val r = row.split("~")
Row(r(0).toIntSafe.getOrElse(null),r(1).toTimestampSafe.getOrElse(null),r(2).toLongSafe.getOrElse(null),r(3),r(4).toIntSafe.getOrElse(null),r(5).toTimestampSafe.getOrElse(null),r(6).toIntSafe.getOrElse(null),r(7),r(8),r(9).toIntSafe.getOrElse(null),r(10).toLongSafe.getOrElse(null),r(11).toLongSafe.getOrElse(null),r(12).toLong)
}
// stringToRow: (row: String)org.apache.spark.sql.Row

val rowRDD = itPostsRows.map(row => stringToRow(row))
// rowRDD: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitionsRDD[16] at map at command-1275360516494518:115

val itPostsDFStruct = spark.createDataFrame(rowRDD, postSchema)
// itPostsDFStruct: org.apache.spark.sql.DataFrame = [commentCount: int, lastActivityDate: timestamp ... 11 more fields]

itPostsDFStruct.dtypes // This will give the datatypes

import org.apache.spark.sql.functions._
// import org.apache.spark.sql.functions._

val postsDf = itPostsDFStruct // postsDf: org.apache.spark.sql.DataFrame = [commentCount: int, lastActivityDate: timestamp ... 11 more fields]

val postsIdBody = postsDf.select("id", "body") // postsIdBody: org.apache.spark.sql.DataFrame = [id: bigint, body: string]
val postsIdBody1 = postsDf.select(postsDf.col("id"), postsDf.col("body")) // postsIdBody1: org.apache.spark.sql.DataFrame = [id: bigint, body: string]
val postsIdBody2 = postsDf.select(Symbol("id"), Symbol("body")) // postsIdBody2: org.apache.spark.sql.DataFrame = [id: bigint, body: string]
val postsIdBody3 = postsDf.select('id, 'body) // postsIdBody3: org.apache.spark.sql.DataFrame = [id: bigint, body: string]
val postIds = postsIdBody.drop("body") // postIds: org.apache.spark.sql.DataFrame = [id: bigint]
postsIdBody.filter('body contains "Italiano").count // gives the count

postsDf.select(avg('score),max('score),count('score)).show
/*
+-----------------+----------+------------+
|       avg(score)|max(score)|count(score)|
+-----------------+----------+------------+
|4.159397303727201|        24|        1261|
+-----------------+----------+------------+
*/

import org.apache.spark.sql.expressions.Window

postsDf.filter('postTypeId === 1).select('ownerUserId, 'acceptedAnswerId, 'score, max('score).over(Window.partitionBy('ownerUserId)) as "maxPerUser")
.withColumn("toMax", 'maxPerUser - 'score).show(10)

/*
+-----------+----------------+-----+----------+-----+
|ownerUserId|acceptedAnswerId|score|maxPerUser|toMax|
+-----------+----------------+-----+----------+-----+
|        348|            1570|    5|         5|    0|
|        736|            null|    1|         1|    0|
|         22|            1263|    6|        12|    6|
|         22|            null|    6|        12|    6|
|         22|            1293|   12|        12|    0|
|         22|            null|    6|        12|    6|
|         22|            1338|    5|        12|    7|
|         22|            1408|    3|        12|    9|
|         22|            1379|    5|        12|    7|
|         22|            1411|    5|        12|    7|
+-----------+----------------+-----+----------+-----+
*/

postsDf.filter('postTypeId === 1).select('ownerUserId, 'id, 'creationDate,lag('id, 1).over(Window.partitionBy('ownerUserId).orderBy('creationDate)) as "prev",lead('id, 1).over(Window.partitionBy('ownerUserId).orderBy('creationDate)) as "next").orderBy('ownerUserId, 'id).show(10)
/*
+-----------+----+--------------------+----+----+
|ownerUserId|  id|        creationDate|prev|next|
+-----------+----+--------------------+----+----+
|          4|1637|2014-01-24 06:51:...|null|null|
|          8|   1|2013-11-05 20:22:...|null| 112|
|          8| 112|2013-11-08 13:14:...|   1|1192|
|          8|1192|2013-11-11 21:01:...| 112|1276|
|          8|1276|2013-11-15 16:09:...|1192|1321|
|          8|1321|2013-11-20 16:42:...|1276|1365|
|          8|1365|2013-11-23 09:09:...|1321|null|
|         12|  11|2013-11-05 21:30:...|null|  17|
|         12|  17|2013-11-05 22:17:...|  11|  18|
|         12|  18|2013-11-05 22:34:...|  17|  19|
+-----------+----+--------------------+----+----+
*/

// Creating a UDF
val countTags = udf((tags: String) =>"&lt;".r.findAllMatchIn(tags).length)
// countTags: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,IntegerType,Some(List(StringType)))

val countTags1 = spark.udf.register("countTags",(tags: String) => "&lt;".r.findAllMatchIn(tags).length)
// countTags1: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,IntegerType,Some(List(StringType)))

postsDf.filter('postTypeId === 1).select('tags, countTags1('tags) as "tagCnt").show(10, false)
/*
+-------------------------------------------------------------------+------+
|tags                                                               |tagCnt|
+-------------------------------------------------------------------+------+
|&lt;word-choice&gt;                                                |1     |
|&lt;english-comparison&gt;&lt;translation&gt;&lt;phrase-request&gt;|3     |
|&lt;usage&gt;&lt;verbs&gt;                                         |2     |
|&lt;usage&gt;&lt;tenses&gt;&lt;english-comparison&gt;              |3     |
|&lt;usage&gt;&lt;punctuation&gt;                                   |2     |
|&lt;usage&gt;&lt;tenses&gt;                                        |2     |
|&lt;history&gt;&lt;english-comparison&gt;                          |2     |
|&lt;idioms&gt;&lt;etymology&gt;                                    |2     |
|&lt;idioms&gt;&lt;regional&gt;                                     |2     |
|&lt;grammar&gt;                                                    |1     |
+-------------------------------------------------------------------+------+
*/

val cleanPosts = postsDf.na.drop()
// cleanPosts: org.apache.spark.sql.DataFrame = [commentCount: int, lastActivityDate: timestamp ... 11 more fields]
cleanPosts.count()
// gives Count

postsDf.na.drop(Array("acceptedAnswerId"))

postsDf.na.fill(Map("viewCount" -> 0))

val postsDfCorrected = postsDf.na.replace(Array("id", "acceptedAnswerId"), Map(1177 -> 3000))
// postsDfCorrected: org.apache.spark.sql.DataFrame = [commentCount: int, lastActivityDate: timestamp ... 11 more fields]

val postsRdd = postsDf.rdd // Converting DataFrame to RDD
// postsRdd: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitionsRDD[48] at rdd at command-1275360516494518:147

val postsMapped = postsDf.rdd.map(row => Row.fromSeq(row.toSeq.updated(3, row.getString(3).replace("&lt;","<").replace("&gt;",">")).updated(8, row.getString(8).replace("&lt;","<").replace("&gt;",">"))))
// postsMapped: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitionsRDD[49] at map at command-1275360516494518:148

val postsDfNew = spark.createDataFrame(postsMapped, postsDf.schema)
// postsDfNew: org.apache.spark.sql.DataFrame = [commentCount: int, lastActivityDate: timestamp ... 11 more fields]

postsDfNew.groupBy('ownerUserId, 'tags,'postTypeId).count.orderBy('ownerUserId desc).show(10)
/*
+-----------+--------------------+----------+-----+
|ownerUserId|                tags|postTypeId|count|
+-----------+--------------------+----------+-----+
|        862|                    |         2|    1|
|        855|         <resources>|         1|    1|
|        846|<translation><eng...|         1|    1|
|        845|<word-meaning><tr...|         1|    1|
|        842|  <verbs><resources>|         1|    1|
|        835|    <grammar><verbs>|         1|    1|
|        833|<meaning><article...|         1|    1|
|        833|           <meaning>|         1|    1|
|        833|                    |         2|    1|
|        814|                    |         2|    1|
+-----------+--------------------+----------+-----+
*/

postsDfNew.groupBy('ownerUserId).agg(max('lastActivityDate), max('score)).show(10)
/*
+-----------+---------------------+----------+
|ownerUserId|max(lastActivityDate)|max(score)|
+-----------+---------------------+----------+
|        270| 2014-02-25 17:43:...|         1|
|        730| 2014-07-12 00:58:...|        12|
|        720| 2014-07-07 21:33:...|         1|
|         19| 2013-11-27 14:21:...|        10|
|        348| 2014-01-06 13:00:...|         5|
|        415| 2014-08-25 00:23:...|         5|
|        656| 2014-05-27 19:30:...|         9|
|        736| 2014-07-15 11:09:...|         1|
|         22| 2014-09-10 07:15:...|        19|
|        198| 2013-12-18 15:57:...|         5|
+-----------+---------------------+----------+
*/
postsDfNew.groupBy('ownerUserId).agg(Map("lastActivityDate" -> "max", "score" -> "max")).show(10)
/*
+-----------+---------------------+----------+
|ownerUserId|max(lastActivityDate)|max(score)|
+-----------+---------------------+----------+
|        270| 2014-02-25 17:43:...|         1|
|        730| 2014-07-12 00:58:...|        12|
|        720| 2014-07-07 21:33:...|         1|
|         19| 2013-11-27 14:21:...|        10|
|        348| 2014-01-06 13:00:...|         5|
|        415| 2014-08-25 00:23:...|         5|
|        656| 2014-05-27 19:30:...|         9|
|        736| 2014-07-15 11:09:...|         1|
|         22| 2014-09-10 07:15:...|        19|
|        198| 2013-12-18 15:57:...|         5|
+-----------+---------------------+----------+
*/
postsDfNew.groupBy('ownerUserId).agg(max('lastActivityDate), max('score).gt(5)).show(10)
/*
+-----------+---------------------+----------------+
|ownerUserId|max(lastActivityDate)|(max(score) > 5)|
+-----------+---------------------+----------------+
|        270| 2014-02-25 17:43:...|           false|
|        730| 2014-07-12 00:58:...|            true|
|        720| 2014-07-07 21:33:...|           false|
|         19| 2013-11-27 14:21:...|            true|
|        348| 2014-01-06 13:00:...|           false|
|        415| 2014-08-25 00:23:...|           false|
|        656| 2014-05-27 19:30:...|            true|
|        736| 2014-07-15 11:09:...|           false|
|         22| 2014-09-10 07:15:...|            true|
|        198| 2013-12-18 15:57:...|           false|
+-----------+---------------------+----------------+
*/
