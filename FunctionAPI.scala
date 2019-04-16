/*
Lanch Spark2 Shell and execute below
*/
import org.apache.spark.sql.functions._

val initialDF = Seq(("Deepak", "p1"), ("Deepak", "p1"), ("Deepak", "p4"), ("Deepak", "p2"), ("Deepak", "p3"), ("Deepak", "p3"), ("Veeru", "p1"), ("Veeru", "p3"), ("Veeru", "p3"), ("Veeru", "p2"), ("Veeru", "p2"), ("Veeru", "p2")).toDF("Name", "Id")
// initialDF: org.apache.spark.sql.DataFrame = [Name: string, Id: string]

 val countDf = initialDF.groupBy('Name, 'Id).count.show()
 /*
+------+---+-----+
|  Name| Id|count|
+------+---+-----+
| Veeru| p1|    1|
|Deepak| p2|    1|
|Deepak| p1|    2|
|Deepak| p3|    2|
| Veeru| p3|    2|
|Deepak| p4|    1|
| Veeru| p2|    3|
+------+---+-----+
*/

val distinctCountDF = initialDF.groupBy('Name).agg(countDistinct('Id)).show
/*
+------+------------------+
|  Name|count(DISTINCT Id)|
+------+------------------+
| Veeru|                 3|
|Deepak|                 4|
+------+------------------+
*/

val distinctCountDF = initialDF.groupBy('Name).agg(approx_count_distinct('Id)).show
/*
+------+-------------------------+
|  Name|approx_count_distinct(Id)|
+------+-------------------------+
| Veeru|                        3|
|Deepak|                        4|
+------+-------------------------+
*/

val foodPrice = Seq(("Deepak", "tomato", 1.99),  ("Veeru", "carrot", 0.45),  ("Vibhav", "apple", 0.99),
                    ("Deepak", "banana", 1.29),  ("Vibhav", "mango", 2.59))
                    .toDF("name", "food", "price")
// foodPrice: org.apache.spark.sql.DataFrame = [name: string, food: string ... 1 more field]

val collectListDF = foodPrice.groupBy('Name).agg(collect_list(struct('food, 'price)).as("FOOD")).show(false)
/*
+------+--------------------------------+
|Name  |FOOD                            |
+------+--------------------------------+
|Veeru |[[carrot, 0.45]]                |
|Deepak|[[tomato, 1.99], [banana, 1.29]]|
|Vibhav|[[apple, 0.99], [mango, 2.59]]  |
+------+--------------------------------+
*/

// Another way to do is by writting UDF and using ZIP but it may degrade the performance...

val zipingUdf = udf[Seq[(String, Double)], Seq[String], Seq[Double]](_.zip(_))
// zipingUdf: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function2>,ArrayType(StructType(StructField(_1,StringType,true), StructField(_2,DoubleType,false)),true),Some(List(ArrayType(StringType,true), ArrayType(DoubleType,false))))


val collectListUsingUdfAndZip = foodPrice.groupBy('name).agg(collect_list('food).as("food"), collect_list('price).as("price"))
                                         .withColumn("FOOD", zipingUdf('food, 'price)).drop('price).show(false)
/*
+------+--------------------------------+
|name  |FOOD                            |
+------+--------------------------------+
|Veeru |[[carrot, 0.45]]                |
|Deepak|[[tomato, 1.99], [banana, 1.29]]|
|Vibhav|[[apple, 0.99], [mango, 2.59]]  |
+------+--------------------------------+
*/
