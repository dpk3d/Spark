/*
Lanch Spark2 Shell and execute below
*/
import org.apache.spark.sql.functions._

val initialDF = Seq(("Deepak", "p1"), ("Deepak", "p1"), ("Deepak", "p4"), ("Deepak", "p2"),
                    ("Deepak", "p3"), ("Deepak", "p3"), ("Veeru", "p1"), ("Veeru", "p3"),
                    ("Veeru", "p3"), ("Veeru", "p2"), ("Veeru", "p2"), ("Veeru", "p2"))
                .toDF("Name", "Id")
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


val inputDF = (Seq((0, "Deepak", "Singh", "Google", "Newyork"),
              (1, "Veeru", "Royal", "Swiggy", null),
              (2, "Smiley", null, "Amazon", "Texas"),
              (3, "Prakriti", "Jain", null, "LA"),
              (4, "Manav", "Singhal", "HPE", "California")))
              .toDF("id", "fName", "lName", "company", "location") 
// inputDF: org.apache.spark.sql.DataFrame = [id: int, fName: string ... 3 more fields]


inputDF.first // org.apache.spark.sql.Row = [0,Deepak,Singh,Google,Newyork]
inputDF.show
/*
+---+--------+-------+-------+----------+
| id|   fName|  lName|company|  location|
+---+--------+-------+-------+----------+
|  0|  Deepak|  Singh| Google|   Newyork|
|  1|   Veeru|  Royal| Swiggy|      null|
|  2|  Smiley|   null| Amazon|     Texas|
|  3|Prakriti|   Jain|   null|        LA|
|  4|   Manav|Singhal|    HPE|California|
+---+--------+-------+-------+----------+
*/

inputDF.filter(col("lName").isNotNull && col("company").isNotNull).show()
/*
+---+------+-------+-------+----------+
| id| fName|  lName|company|  location|
+---+------+-------+-------+----------+
|  0|Deepak|  Singh| Google|   Newyork|
|  1| Veeru|  Royal| Swiggy|      null|
|  4| Manav|Singhal|    HPE|California|
+---+------+-------+-------+----------+
*/

val filterCond = inputDF.columns.map(x=>col(x).isNotNull).reduce(_ && _)
// filterCond: org.apache.spark.sql.Column = (((((id IS NOT NULL) AND (fName IS NOT NULL)) AND (lName IS NOT NULL)) AND (company IS NOT NULL)) AND (location IS NOT NULL))

val filterDF = inputDF.filter(filterCond).show()
/*
+---+------+-------+-------+----------+
| id| fName|  lName|company|  location|
+---+------+-------+-------+----------+
|  0|Deepak|  Singh| Google|   Newyork|
|  4| Manav|Singhal|    HPE|California|
+---+------+-------+-------+----------+
*/

// Now Manupulating the null value

val manupulatingNullDF = inputDF.withColumn("lastName", when($"lName".isNull, "Love").otherwise("0")).drop($"lName")
                              .withColumn("Company", when($"company".isNull, "ABC")).drop($"company")
                               .withColumn("Location", when($"location".isNull, "XYZ")).drop($"location").show
/*
+---+--------+-------+--------+--------+
| id|   fName|Company|Location|lastName|
+---+--------+-------+--------+--------+
|  0|  Deepak|   null|    null|       0|
|  1|   Veeru|   null|     XYZ|       0|
|  2|  Smiley|   null|    null|    Love|
|  3|Prakriti|    ABC|    null|       0|
|  4|   Manav|   null|    null|       0|
+---+--------+-------+--------+--------+
*/
val manupulatingNullDF = inputDF.withColumn("lastName", when($"lName".isNull,"Love").otherwise(col("lname")))
                                .withColumn("Company",when($"company".isNull, "ABC").otherwise(col("company")))
                                .withColumn("Location",when($"location".isNull, "XYZ").otherwise(col("location")))
                                .drop("lName").show
/*
+---+--------+-------+----------+--------+
| id|   fName|Company|  Location|lastName|
+---+--------+-------+----------+--------+
|  0|  Deepak| Google|   Newyork|   Singh|
|  1|   Veeru| Swiggy|       XYZ|   Royal|
|  2|  Smiley| Amazon|     Texas|    Love|
|  3|Prakriti|    ABC|        LA|    Jain|
|  4|   Manav|    HPE|California| Singhal|
+---+--------+-------+----------+--------+
*/
