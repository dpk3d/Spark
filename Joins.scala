object join extends App {
case class Book(book_name: String, cost: Int, author_id:Int)
case class Author(author_name: String, author_id:Int)

val bookDS = Seq(
  Book("Scala", 700, 1),Book("Spark", 1000, 2),Book("Kafka", 1700, 3),
  Book("Java", 350, 5)).toDS()

 bookDS.show()
/*
+---------+----+---------+
|book_name|cost|author_id|
+---------+----+---------+
|    Scala| 700|        1|
|    Spark|1000|        2|
|    Kafka|1700|        3|
|     Java| 350|        5|
+---------+----+---------+
*/
val authorDS = Seq(
  Author("Martin",1),Author("Deepak",2),Author("Moni", 3),
  Author("Bani", 4)).toDS()

 authorDS.show()
/*
+-----------+---------+
|author_name|author_id|
+-----------+---------+
|     Martin|        1|
|     Deepak|        2|
|       Moni|        3|
|       Bani|        4|
+-----------+---------+ */

s"""The INNER JOIN returns the dataset which has the rows that have matching values in both the datasets.
i.e. value of the common field will be the same."""

val BookAuthorInner = bookDS.join(authorDS, bookDS("author_id") === authorDS("author_id"), "inner")
 BookAuthorInner.show()
/*
+---------+----+---------+-----------+---------+
|book_name|cost|author_id|author_name|author_id|
+---------+----+---------+-----------+---------+
|    Scala| 700|        1|     Martin|        1|
|    Spark|1000|        2|     Deepak|        2|
|    Kafka|1700|        3|       Moni|        3|
+---------+----+---------+-----------+---------+
*/

s"""The CROSS JOIN returns the dataset which is the number of rows in the first dataset"
multiplied by the number of rows in the second dataset. Like a Cartesian Product."""
  
// spark.sql.crossJoin.enabled must be set to true
spark.conf.set("spark.sql.crossJoin.enabled", true)
  
val BookAuthorCross = bookDS.join(authorDS)
 BookAuthorCross.show()
/*
+---------+----+---------+-----------+---------+
|book_name|cost|author_id|author_name|author_id|
+---------+----+---------+-----------+---------+
|    Scala| 700|        1|     Martin|        1|
|    Scala| 700|        1|     Deepak|        2|
|    Scala| 700|        1|       Moni|        3|
|    Scala| 700|        1|       Bani|        4|
|    Spark|1000|        2|     Martin|        1|
|    Spark|1000|        2|     Deepak|        2|
|    Spark|1000|        2|       Moni|        3|
|    Spark|1000|        2|       Bani|        4|
|    Kafka|1700|        3|     Martin|        1|
|    Kafka|1700|        3|     Deepak|        2|
|    Kafka|1700|        3|       Moni|        3|
|    Kafka|1700|        3|       Bani|        4|
|     Java| 350|        5|     Martin|        1|
|     Java| 350|        5|     Deepak|        2|
|     Java| 350|        5|       Moni|        3|
|     Java| 350|        5|       Bani|        4|
+---------+----+---------+-----------+---------+
*/

s"""The LEFT OUTER JOIN returns the dataset that has all rows from the left dataset, and the matched rows from the right dataset."""

val BookAuthorLeft = bookDS.join(authorDS, bookDS("author_id") === authorDS("author_id"), "leftouter")
 BookAuthorLeft.show()
/*
+---------+----+---------+-----------+---------+
|book_name|cost|author_id|author_name|author_id|
+---------+----+---------+-----------+---------+
|    Scala| 700|        1|     Martin|        1|
|    Spark|1000|        2|     Deepak|        2|
|    Kafka|1700|        3|       Moni|        3|
|     Java| 350|        5|       null|     null|
+---------+----+---------+-----------+---------+
*/

s"""The RIGHT OUTER JOIN returns the dataset that has all rows from the right dataset, and the matched rows from the left dataset."""

val BookAuthorRight = bookDS.join(authorDS, bookDS("author_id") === authorDS("author_id"), "rightouter")
 BookAuthorRight.show()
/*
+---------+----+---------+-----------+---------+
|book_name|cost|author_id|author_name|author_id|
+---------+----+---------+-----------+---------+
|    Scala| 700|        1|     Martin|        1|
|    Spark|1000|        2|     Deepak|        2|
|    Kafka|1700|        3|       Moni|        3|
|     null|null|     null|       Bani|        4|
+---------+----+---------+-----------+---------+
*/

s"""The FULL OUTER JOIN returns the dataset that has all rows when there is a match in either the left or right dataset."""

val BookAuthorFull = bookDS.join(authorDS, bookDS("author_id") === authorDS("author_id"), "fullouter")
 BookAuthorFull.show()
/*
+---------+----+---------+-----------+---------+
|book_name|cost|author_id|author_name|author_id|
+---------+----+---------+-----------+---------+
|    Scala| 700|        1|     Martin|        1|
|    Spark|1000|        2|     Deepak|        2|
|    Kafka|1700|        3|       Moni|        3|
|     null|null|     null|       Bani|        4|
|     Java| 350|        5|       null|     null|
+---------+----+---------+-----------+---------+
*/

s"""The LEFT SEMI JOIN returns the dataset which has all rows from the left dataset having their correspondence in the right dataset.
Unlike the LEFT OUTER JOIN, the returned dataset in LEFT SEMI JOIN contains only the columns from the left dataset."""

val BookAuthorLeftSemi = bookDS.join(authorDS, bookDS("author_id") === authorDS("author_id"), "leftsemi")
 BookAuthorLeftSemi.show()
/*
+---------+----+---------+
|book_name|cost|author_id|
+---------+----+---------+
|    Scala| 700|        1|
|    Spark|1000|        2|
|    Kafka|1700|        3|
+---------+----+---------+
*/

s"""The LEFT ANTI SEMI JOIN returns the dataset which has all the rows from the left dataset that don’t have their matching in the right dataset.
It also contains only the columns from the left dataset."""

val BookAuthorLeftAnti = bookDS.join(authorDS, bookDS("author_id") === authorDS("author_id"), "leftanti")
BookAuthorLeftAnti.show()
/*
+---------+----+---------+
|book_name|cost|author_id|
+---------+----+---------+
|     Java| 350|        5|
+---------+----+---------+
*/
}
