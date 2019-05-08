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

import org.apache.spark.sql.functions._

val prime = Seq(
               ("Bangalore", 2019, 82000),
               ("Mumbai", 2018, 72000),
               ("Mumbai", 2015, 48000),
               ("Pune", 2017, 60000),
               ("Delhi", 2015, 54000),
               ("Delhi", 2016, 50000),
               ("Chennai", 2015, 40000),
               ("Bangalore", 2018, 80000)
).toDF("City", "Year", "Price")

prime.show
/*
+---------+----+-----+
|     City|Year|Price|
+---------+----+-----+
|Bangalore|2019|82000|
|   Mumbai|2018|72000|
|   Mumbai|2015|48000|
|     Pune|2017|60000|
|    Delhi|2015|54000|
|    Delhi|2016|50000|
|  Chennai|2015|40000|
|Bangalore|2018|80000|
+---------+----+-----+
*/

val groupByCityAndYear = prime.groupBy("City").agg(sum("Price") as "Amount").show
/*
+---------+------+
|     City|Amount|
+---------+------+
|Bangalore|162000|
|  Chennai| 40000|
|   Mumbai|120000|
|     Pune| 60000|
|    Delhi|104000|
+---------+------+
*/

val nullCity = Seq((null , 2018, 876570),(null.asInstanceOf[String], 2019, 82200)).toDF("City", "Year", "Price")

val allPrime = prime union nullCity

allPrime.show()
/*
+---------+----+------+
|     City|Year| Price|
+---------+----+------+
|Bangalore|2019| 82000|
|   Mumbai|2018| 72000|
|   Mumbai|2015| 48000|
|     Pune|2017| 60000|
|    Delhi|2015| 54000|
|    Delhi|2016| 50000|
|  Chennai|2015| 40000|
|Bangalore|2018| 80000|
|     null|2018|876570|
|     null|2019| 82200|
+---------+----+------+
*/
val cube = allPrime
  .cube("City", "Year")
  .agg(grouping("City"), grouping("Year")) // <-- grouping here
cube.show()
/*
+---------+----+--------------+--------------+
|     City|Year|grouping(City)|grouping(Year)|
+---------+----+--------------+--------------+
|   Mumbai|null|             0|             1|
|   Mumbai|2018|             0|             0|
|     null|2015|             1|             0|
|    Delhi|2016|             0|             0|
|     Pune|2017|             0|             0|
|  Chennai|2015|             0|             0|
|     null|null|             0|             1|
|    Delhi|2015|             0|             0|
|     null|null|             1|             1|
|    Delhi|null|             0|             1|
|     null|2018|             0|             0|
|     null|2016|             1|             0|
|     null|2019|             1|             0|
|     null|2019|             0|             0|
|   Mumbai|2015|             0|             0|
|  Chennai|null|             0|             1|
|Bangalore|2019|             0|             0|
|     null|2017|             1|             0|
|     null|2018|             1|             0|
|Bangalore|2018|             0|             0|
+---------+----+--------------+--------------+
*/

val cubeArrangingNull = allPrime
  .cube("City", "Year")
  .agg(grouping("City"), grouping("Year")) // <-- grouping here
  .sort($"City".desc_nulls_last, $"Year".desc_nulls_last)
cubeArrangingNull.show()
/*
+---------+----+--------------+--------------+
|     City|Year|grouping(City)|grouping(Year)|
+---------+----+--------------+--------------+
|     Pune|2017|             0|             0|
|     Pune|null|             0|             1|
|   Mumbai|2018|             0|             0|
|   Mumbai|2015|             0|             0|
|   Mumbai|null|             0|             1|
|    Delhi|2016|             0|             0|
|    Delhi|2015|             0|             0|
|    Delhi|null|             0|             1|
|  Chennai|2015|             0|             0|
|  Chennai|null|             0|             1|
|Bangalore|2019|             0|             0|
|Bangalore|2018|             0|             0|
|Bangalore|null|             0|             1|
|     null|2019|             0|             0|
|     null|2019|             1|             0|
|     null|2018|             0|             0|
|     null|2018|             1|             0|
|     null|2017|             1|             0|
|     null|2016|             1|             0|
|     null|2015|             1|             0|
+---------+----+--------------+--------------+
*/

val cube2 = allPrime.cube("City", "Year").agg(grouping("City"))
println(cube.queryExecution)

/*
== Parsed Logical Plan ==
'Sort ['City DESC NULLS LAST, 'Year DESC NULLS LAST], true
+- Aggregate [City#125, Year#126, spark_grouping_id#122], [City#125, Year#126, cast((shiftright(spark_grouping_id#122, 1) & 1) as tinyint) AS grouping(City)#120, cast((shiftright(spark_grouping_id#122, 0) & 1) as tinyint) AS grouping(Year)#121]
   +- Expand [List(City#91, Year#92, Price#93, City#123, Year#124, 0), List(City#91, Year#92, Price#93, City#123, null, 1), List(City#91, Year#92, Price#93, null, Year#124, 2), List(City#91, Year#92, Price#93, null, null, 3)], [City#91, Year#92, Price#93, City#125, Year#126, spark_grouping_id#122]
      +- Project [City#91, Year#92, Price#93, City#91 AS City#123, Year#92 AS Year#124]
         +- Union
            :- Project [_1#87 AS City#91, _2#88 AS Year#92, _3#89 AS Price#93]
            :  +- LocalRelation [_1#87, _2#88, _3#89]
            +- Project [_1#101 AS City#105, _2#102 AS Year#106, _3#103 AS Price#107]
               +- LocalRelation [_1#101, _2#102, _3#103]

== Analyzed Logical Plan ==
City: string, Year: int, grouping(City): tinyint, grouping(Year): tinyint
Sort [City#125 DESC NULLS LAST, Year#126 DESC NULLS LAST], true
+- Aggregate [City#125, Year#126, spark_grouping_id#122], [City#125, Year#126, cast((shiftright(spark_grouping_id#122, 1) & 1) as tinyint) AS grouping(City)#120, cast((shiftright(spark_grouping_id#122, 0) & 1) as tinyint) AS grouping(Year)#121]
   +- Expand [List(City#91, Year#92, Price#93, City#123, Year#124, 0), List(City#91, Year#92, Price#93, City#123, null, 1), List(City#91, Year#92, Price#93, null, Year#124, 2), List(City#91, Year#92, Price#93, null, null, 3)], [City#91, Year#92, Price#93, City#125, Year#126, spark_grouping_id#122]
      +- Project [City#91, Year#92, Price#93, City#91 AS City#123, Year#92 AS Year#124]
         +- Union
            :- Project [_1#87 AS City#91, _2#88 AS Year#92, _3#89 AS Price#93]
            :  +- LocalRelation [_1#87, _2#88, _3#89]
            +- Project [_1#101 AS City#105, _2#102 AS Year#106, _3#103 AS Price#107]
               +- LocalRelation [_1#101, _2#102, _3#103]

== Optimized Logical Plan ==
Sort [City#125 DESC NULLS LAST, Year#126 DESC NULLS LAST], true
+- Aggregate [City#125, Year#126, spark_grouping_id#122], [City#125, Year#126, cast((shiftright(spark_grouping_id#122, 1) & 1) as tinyint) AS grouping(City)#120, cast((shiftright(spark_grouping_id#122, 0) & 1) as tinyint) AS grouping(Year)#121]
   +- Expand [List(City#123, Year#124, 0), List(City#123, null, 1), List(null, Year#124, 2), List(null, null, 3)], [City#125, Year#126, spark_grouping_id#122]
      +- Union
         :- LocalRelation [City#123, Year#124]
         +- LocalRelation [City#208, Year#209]

== Physical Plan ==
*Sort [City#125 DESC NULLS LAST, Year#126 DESC NULLS LAST], true, 0
+- Exchange rangepartitioning(City#125 DESC NULLS LAST, Year#126 DESC NULLS LAST, 200)
   +- *HashAggregate(keys=[City#125, Year#126, spark_grouping_id#122], functions=[], output=[City#125, Year#126, grouping(City)#120, grouping(Year)#121])
      +- Exchange hashpartitioning(City#125, Year#126, spark_grouping_id#122, 200)
         +- *HashAggregate(keys=[City#125, Year#126, spark_grouping_id#122], functions=[], output=[City#125, Year#126, spark_grouping_id#122])
            +- *Expand [List(City#123, Year#124, 0), List(City#123, null, 1), List(null, Year#124, 2), List(null, null, 3)], [City#125, Year#126, spark_grouping_id#122]
               +- Union
                  :- LocalTableScan [City#123, Year#124]
                  +- LocalTableScan [City#208, Year#209]
*/

val cubeWithCity = allPrime
  .cube("City")
  .agg(grouping("City")) // <-- grouping here
  .sort($"City".desc_nulls_last)
cubeWithCity.show()
/*
+---------+--------------+
|     City|grouping(City)|
+---------+--------------+
|     Pune|             0|
|   Mumbai|             0|
|    Delhi|             0|
|  Chennai|             0|
|Bangalore|             0|
|     null|             1|
|     null|             0|
+---------+--------------+
*/


