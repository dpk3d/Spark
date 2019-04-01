package com.deepak.spark

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window 
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.functions._

object WindowFunctions{

 def main(args: Array[String]): Unit = {
 
val spark = SparkSession.builder
  .master("local")
  .appName("Window Functions")
  .getOrCreate()

// Creating a Case class
case class Salary (deptName: String, empNo: Int, empName: String, salary: Long, skill : Seq[String])
import spark.implicits._
//Creating a Dataset
val empSalary = Seq( 
  Salary("Admin", 21, "Katam", 50000, List("Spark", "Scala")), 
  Salary("HR", 22, "Karanpreet", 39000, List("Hiring", "Scala")),    
  Salary("Admin", 33, "Rakesh", 48000, List("Spark", "Scala")), 
  Salary("Admin", 44, "Ravi", 48000, List("Spark", "Scala")), 
  Salary("HR", 55, "Suhani", 35000, List("Hiring", "Scala")), 
  Salary("IT", 71, "Deepak", 42000, List("Spark", "Scala")), 
  Salary("IT", 81, "Veeru", 60000, List("Spark", "Python")), 
  Salary("IT", 90, "Manav", 45000, List("Spark", "Java")), 
  Salary("IT", 101, "Pritam", 52000, List("Spark", "Scala")), 
  Salary("IT", 112, "Abhishek", 52000, List("Spark", "Python"))).toDS 
  
 // empSalary.createTempView("empSalary")
 
 empSalary.show()
 /*****
 * Output
+--------+-----+----------+------+---------------+
|deptName|empNo|   empName|salary|          skill|
+--------+-----+----------+------+---------------+
|   Admin|   21|     Katam| 50000| [Spark, Scala]|
|      HR|   22|Karanpreet| 39000|[Hiring, Scala]|
|   Admin|   33|    Rakesh| 48000| [Spark, Scala]|
|   Admin|   44|      Ravi| 48000| [Spark, Scala]|
|      HR|   55|    Suhani| 35000|[Hiring, Scala]|
|      IT|   71|    Deepak| 42000| [Spark, Scala]|
|      IT|   81|     Veeru| 60000|[Spark, Python]|
|      IT|   90|     Manav| 45000|  [Spark, Java]|
|      IT|  101|    Pritam| 52000| [Spark, Scala]|
|      IT|  112|  Abhishek| 52000|[Spark, Python]|
+--------+-----+----------+------+---------------+
*
*/

// Creating a Window over Department name
val window1 = Window.partitionBy("deptName")

val df = empSalary.withColumn("average_salary_dept", array_contains('skill, "Spark") over window1)
df.show()
/***
* array_contains gives error with window function
*
org.apache.spark.sql.AnalysisException: Expression 'array_contains(skill#128, Spark)' not supported within a window function.;;
*
*/

// Getting the Average and total salary department Wise
val avgAndTotalSalary = empSalary.withColumn("salaries", collect_list('salary) over window1)
                                 .withColumn("avg_salary", (avg('salary) over window1).cast("Int"))
                                 .withColumn("total_salary", sum('salary) over window1)
                                 .select("deptName", "empNo", "empName", "salary", "salaries", "avg_salary", "total_salary")
avgAndTotalSalary.show(false)

/**** Output *****
+--------+-----+----------+------+-----------------------------------+----------+------------+
|deptName|empNo|empName   |salary|salaries                           |avg_salary|total_salary|
+--------+-----+----------+------+-----------------------------------+----------+------------+
|HR      |22   |Karanpreet|39000 |[39000, 35000]                     |37000     |74000       |
|HR      |55   |Suhani    |35000 |[39000, 35000]                     |37000     |74000       |
|Admin   |21   |Katam     |50000 |[50000, 48000, 48000]              |48666     |146000      |
|Admin   |33   |Rakesh    |48000 |[50000, 48000, 48000]              |48666     |146000      |
|Admin   |44   |Ravi      |48000 |[50000, 48000, 48000]              |48666     |146000      |
|IT      |71   |Deepak    |42000 |[42000, 60000, 45000, 52000, 52000]|50200     |251000      |
|IT      |81   |Veeru     |60000 |[42000, 60000, 45000, 52000, 52000]|50200     |251000      |
|IT      |90   |Manav     |45000 |[42000, 60000, 45000, 52000, 52000]|50200     |251000      |
|IT      |101  |Pritam    |52000 |[42000, 60000, 45000, 52000, 52000]|50200     |251000      |
|IT      |112  |Abhishek  |52000 |[42000, 60000, 45000, 52000, 52000]|50200     |251000      |
+--------+-----+----------+------+-----------------------------------+----------+------------+
*
*/

// Creating a window on partition with Department Name and ordered by salary (Ordered Frame)
val window2 = Window.partitionBy("deptName").orderBy('salary desc)
  
val avgAndTotalSalaryWithOrderBy = empSalary.withColumn("salaries", collect_list('salary) over window2)
                                            .withColumn("avg_salary", (avg('salary) over window2).cast("Int"))
                                            .withColumn("total_salary", sum('salary) over window2)
                                            .select("deptName", "empNo", "empName", "salary", "salaries", "avg_salary", "total_salary")

avgAndTotalSalaryWithOrderBy.show(false)

/**** Output *****
+--------+-----+----------+------+-----------------------------------+----------+------------+
|deptName|empNo|empName   |salary|salaries                           |avg_salary|total_salary|
+--------+-----+----------+------+-----------------------------------+----------+------------+
|HR      |22   |Karanpreet|39000 |[39000]                            |39000     |39000       |
|HR      |55   |Suhani    |35000 |[39000, 35000]                     |37000     |74000       |
|Admin   |21   |Katam     |50000 |[50000]                            |50000     |50000       |
|Admin   |33   |Rakesh    |48000 |[50000, 48000, 48000]              |48666     |146000      |
|Admin   |44   |Ravi      |48000 |[50000, 48000, 48000]              |48666     |146000      |
|IT      |81   |Veeru     |60000 |[60000]                            |60000     |60000       |
|IT      |101  |Pritam    |52000 |[60000, 52000, 52000]              |54666     |164000      |
|IT      |112  |Abhishek  |52000 |[60000, 52000, 52000]              |54666     |164000      |
|IT      |90   |Manav     |45000 |[60000, 52000, 52000, 45000]       |52250     |209000      |
|IT      |71   |Deepak    |42000 |[60000, 52000, 52000, 45000, 42000]|50200     |251000      |
+--------+-----+----------+------+-----------------------------------+----------+------------+
*
*/

// Applying Various Ranking Functions
val rankFunctions = empSalary.withColumn("salaries", collect_list('salary) over window2)
                             .withColumn("Rank", rank() over window2)
                             .withColumn("DenseRank", dense_rank() over window2)
                             .withColumn("RowNumber", row_number() over window2)
                             .withColumn("Ntile", ntile(3) over window2)
                             .withColumn("PercentageRank", percent_rank() over window2)
                             .select("deptName", "empNo", "empName", "salary", "Rank", "DenseRank", "RowNumber", "Ntile", "PercentageRank")
rankFunctions.show(false)

/**** Output *****
+--------+-----+----------+------+----+---------+---------+-----+--------------+
|deptName|empNo|empName   |salary|Rank|DenseRank|RowNumber|Ntile|PercentageRank|
+--------+-----+----------+------+----+---------+---------+-----+--------------+
|HR      |22   |Karanpreet|39000 |1   |1        |1        |1    |0.0           |
|HR      |55   |Suhani    |35000 |2   |2        |2        |2    |1.0           |
|Admin   |21   |Katam     |50000 |1   |1        |1        |1    |0.0           |
|Admin   |33   |Rakesh    |48000 |2   |2        |2        |2    |0.5           |
|Admin   |44   |Ravi      |48000 |2   |2        |3        |3    |0.5           |
|IT      |81   |Veeru     |60000 |1   |1        |1        |1    |0.0           |
|IT      |101  |Pritam    |52000 |2   |2        |2        |1    |0.25          |
|IT      |112  |Abhishek  |52000 |2   |2        |3        |2    |0.25          |
|IT      |90   |Manav     |45000 |4   |3        |4        |2    |0.75          |
|IT      |71   |Deepak    |42000 |5   |4        |5        |3    |1.0           |
+--------+-----+----------+------+----+---------+---------+-----+--------------+
*
*/

// Getting the Top 3 salary department wise

val top3Salary = empSalary.withColumn("RowNumber", row_number() over window2)
                          .filter('RowNumber <=3)
                          .select("deptName", "empNo", "empName", "salary")

top3Salary.show(false)

/**** Output *****
+--------+-----+----------+------+
|deptName|empNo|empName   |salary|
+--------+-----+----------+------+
|HR      |22   |Karanpreet|39000 |
|HR      |55   |Suhani    |35000 |
|Admin   |21   |Katam     |50000 |
|Admin   |33   |Rakesh    |48000 |
|Admin   |44   |Ravi      |48000 |
|IT      |81   |Veeru     |60000 |
|IT      |101  |Pritam    |52000 |
|IT      |112  |Abhishek  |52000 |
+--------+-----+----------+------+
*
*/

// Applying Lead and Lag
val leadAndLag = empSalary.withColumn("lead", lead('salary, 1).over (window2))
                          .withColumn("lag", lag('salary, 1).over (window2))
                          .select("deptName", "empNo", "empName", "salary", "lead", "lag")

leadAndLag.show(false)

/**** Output *****
+--------+-----+----------+------+-----+-----+
|deptName|empNo|empName   |salary|lead |lag  |
+--------+-----+----------+------+-----+-----+
|HR      |22   |Karanpreet|39000 |35000|null |
|HR      |55   |Suhani    |35000 |null |39000|
|Admin   |21   |Katam     |50000 |48000|null |
|Admin   |33   |Rakesh    |48000 |48000|50000|
|Admin   |44   |Ravi      |48000 |null |48000|
|IT      |81   |Veeru     |60000 |52000|null |
|IT      |101  |Pritam    |52000 |52000|60000|
|IT      |112  |Abhishek  |52000 |45000|52000|
|IT      |90   |Manav     |45000 |42000|52000|
|IT      |71   |Deepak    |42000 |null |45000|
+--------+-----+----------+------+-----+-----+
*
*/

// Getting the Difference
val leadAndLagDiff = leadAndLag.withColumn("higher_than_next" , 'salary - 'lead)
                               .withColumn("lower_than_previous", 'lag - 'salary)

leadAndLagDiff.show(false)

/**** Output *****
+--------+-----+----------+------+-----+-----+----------------+-------------------+
|deptName|empNo|empName   |salary|lead |lag  |higher_than_next|lower_than_previous|
+--------+-----+----------+------+-----+-----+----------------+-------------------+
|HR      |22   |Karanpreet|39000 |35000|null |4000            |null               |
|HR      |55   |Suhani    |35000 |null |39000|null            |4000               |
|Admin   |21   |Katam     |50000 |48000|null |2000            |null               |
|Admin   |33   |Rakesh    |48000 |48000|50000|0               |2000               |
|Admin   |44   |Ravi      |48000 |null |48000|null            |0                  |
|IT      |81   |Veeru     |60000 |52000|null |8000            |null               |
|IT      |101  |Pritam    |52000 |52000|60000|0               |8000               |
|IT      |112  |Abhishek  |52000 |45000|52000|7000            |0                  |
|IT      |90   |Manav     |45000 |42000|52000|3000            |7000               |
|IT      |71   |Deepak    |42000 |null |45000|null            |3000               |
+--------+-----+----------+------+-----+-----+----------------+-------------------+
*
*/

// Removing the null values and filling it with "0"
val removingNull = leadAndLag.withColumn("higher_than_next", when('lead.isNull, 0).otherwise('salary - 'lead))
                             .withColumn("lower_than_previous", when('lag.isNull, 0).otherwise('lag - 'salary)) 

removingNull.show(false)

/**** Output *****
+--------+-----+----------+------+-----+-----+----------------+-------------------+
|deptName|empNo|empName   |salary|lead |lag  |higher_than_next|lower_than_previous|
+--------+-----+----------+------+-----+-----+----------------+-------------------+
|HR      |22   |Karanpreet|39000 |35000|null |4000            |0                  |
|HR      |55   |Suhani    |35000 |null |39000|0               |4000               |
|Admin   |21   |Katam     |50000 |48000|null |2000            |0                  |
|Admin   |33   |Rakesh    |48000 |48000|50000|0               |2000               |
|Admin   |44   |Ravi      |48000 |null |48000|0               |0                  |
|IT      |81   |Veeru     |60000 |52000|null |8000            |0                  |
|IT      |101  |Pritam    |52000 |52000|60000|0               |8000               |
|IT      |112  |Abhishek  |52000 |45000|52000|7000            |0                  |
|IT      |90   |Manav     |45000 |42000|52000|3000            |7000               |
|IT      |71   |Deepak    |42000 |null |45000|0               |3000               |
+--------+-----+----------+------+-----+-----+----------------+-------------------+
*
*/

// Getting Running Total (Adding everything upto currentRow)
val runningTotal = empSalary.withColumn("Rank", rank().over (window2))
                            .withColumn("Cost", sum('salary).over(window2))
                            .select("deptName", "empNo", "empName", "salary", "Rank", "Cost")

runningTotal.show(false)

/**** Output *****
+--------+-----+----------+------+----+------+
|deptName|empNo|empName   |salary|Rank|Cost  |
+--------+-----+----------+------+----+------+
|HR      |22   |Karanpreet|39000 |1   |39000 |
|HR      |55   |Suhani    |35000 |2   |74000 |
|Admin   |21   |Katam     |50000 |1   |50000 |
|Admin   |33   |Rakesh    |48000 |2   |146000|
|Admin   |44   |Ravi      |48000 |2   |146000|
|IT      |81   |Veeru     |60000 |1   |60000 |
|IT      |101  |Pritam    |52000 |2   |164000|
|IT      |112  |Abhishek  |52000 |2   |164000|
|IT      |90   |Manav     |45000 |4   |209000|
|IT      |71   |Deepak    |42000 |5   |251000|
+--------+-----+----------+------+----+------+
*
*/

// Creating a window on ROW
val rowWindow = Window.partitionBy("deptName").orderBy("RowNumber")

val runningTotal2 = empSalary.withColumn("RowNumber", row_number over window2)
                             .withColumn("Cost", sum('salary) over rowWindow)
                             .select("deptName", "empNo", "empName", "salary", "RowNumber", "Cost")

runningTotal2.show(false)

/**** Output *****
+--------+-----+----------+------+---------+------+
|deptName|empNo|empName   |salary|RowNumber|Cost  |
+--------+-----+----------+------+---------+------+
|HR      |22   |Karanpreet|39000 |1        |39000 |
|HR      |55   |Suhani    |35000 |2        |74000 |
|Admin   |21   |Katam     |50000 |1        |50000 |
|Admin   |33   |Rakesh    |48000 |2        |98000 |
|Admin   |44   |Ravi      |48000 |3        |146000|
|IT      |81   |Veeru     |60000 |1        |60000 |
|IT      |101  |Pritam    |52000 |2        |112000|
|IT      |112  |Abhishek  |52000 |3        |164000|
|IT      |90   |Manav     |45000 |4        |209000|
|IT      |71   |Deepak    |42000 |5        |251000|
+--------+-----+----------+------+---------+------+
*
*/


// Applying Range Frame in Window

//creating a window without ordered
val window3 = Window.partitionBy("deptName").rowsBetween(Window.currentRow, 1)

val rangeFrame = empSalary.withColumn("salaries", collect_list('salary) over window3)
                          .withColumn("total_salary", sum('salary) over window3)
                          .select("deptName", "empNo", "empName", "salary", "salaries", "total_salary")

rangeFrame.show(false)

/**** Output *****
+--------+-----+----------+------+--------------+------------+
|deptName|empNo|empName   |salary|salaries      |total_salary|
+--------+-----+----------+------+--------------+------------+
|HR      |22   |Karanpreet|39000 |[39000, 35000]|74000       |
|HR      |55   |Suhani    |35000 |[35000]       |35000       |
|Admin   |21   |Katam     |50000 |[50000, 48000]|98000       |
|Admin   |33   |Rakesh    |48000 |[48000, 48000]|96000       |
|Admin   |44   |Ravi      |48000 |[48000]       |48000       |
|IT      |71   |Deepak    |42000 |[42000, 60000]|102000      |
|IT      |81   |Veeru     |60000 |[60000, 45000]|105000      |
|IT      |90   |Manav     |45000 |[45000, 52000]|97000       |
|IT      |101  |Pritam    |52000 |[52000, 52000]|104000      |
|IT      |112  |Abhishek  |52000 |[52000]       |52000       |
+--------+-----+----------+------+--------------+------------+
*
*/

// Creating a window with ordered
val window4 = Window.partitionBy("deptName").orderBy('salary desc).rowsBetween(Window.currentRow, 1)

val rangeFrameWithOrderBy = empSalary.withColumn("salaries", collect_list('salary) over window4)
                                     .withColumn("total_salary", sum('salary) over window4)
                                     .select("deptName", "empNo", "empName", "salary", "salaries", "total_salary")

rangeFrameWithOrderBy.show(false)

/**** Output *****
+--------+-----+----------+------+--------------+------------+
|deptName|empNo|empName   |salary|salaries      |total_salary|
+--------+-----+----------+------+--------------+------------+
|HR      |22   |Karanpreet|39000 |[39000, 35000]|74000       |
|HR      |55   |Suhani    |35000 |[35000]       |35000       |
|Admin   |21   |Katam     |50000 |[50000, 48000]|98000       |
|Admin   |33   |Rakesh    |48000 |[48000, 48000]|96000       |
|Admin   |44   |Ravi      |48000 |[48000]       |48000       |
|IT      |81   |Veeru     |60000 |[60000, 52000]|112000      |
|IT      |101  |Pritam    |52000 |[52000, 52000]|104000      |
|IT      |112  |Abhishek  |52000 |[52000, 45000]|97000       |
|IT      |90   |Manav     |45000 |[45000, 42000]|87000       |
|IT      |71   |Deepak    |42000 |[42000]       |42000       |
+--------+-----+----------+------+--------------+------------+
*
*/

// Creating a ordered window on all the rows
val window5 = Window.partitionBy("deptName").orderBy("salary").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

// Getting Median 
val medianSalaries = empSalary.withColumn("salaries", collect_list('salary) over window5)
                              .withColumn("median_salaries", element_at('salaries, (size('salaries)/2 + 1).cast("Int")))
                              .select("deptName", "empNo", "empName", "salary", "salaries", "median_salaries")

medianSalaries.show(false)

/**** Output *****
+--------+-----+----------+------+-----------------------------------+---------------+
|deptName|empNo|empName   |salary|salaries                           |median_salaries|
+--------+-----+----------+------+-----------------------------------+---------------+
|HR      |55   |Suhani    |35000 |[35000, 39000]                     |39000          |
|HR      |22   |Karanpreet|39000 |[35000, 39000]                     |39000          |
|Admin   |33   |Rakesh    |48000 |[48000, 48000, 50000]              |48000          |
|Admin   |44   |Ravi      |48000 |[48000, 48000, 50000]              |48000          |
|Admin   |21   |Katam     |50000 |[48000, 48000, 50000]              |48000          |
|IT      |71   |Deepak    |42000 |[42000, 45000, 52000, 52000, 60000]|52000          |
|IT      |90   |Manav     |45000 |[42000, 45000, 52000, 52000, 60000]|52000          |
|IT      |101  |Pritam    |52000 |[42000, 45000, 52000, 52000, 60000]|52000          |
|IT      |112  |Abhishek  |52000 |[42000, 45000, 52000, 52000, 60000]|52000          |
|IT      |81   |Veeru     |60000 |[42000, 45000, 52000, 52000, 60000]|52000          |
+--------+-----+----------+------+-----------------------------------+---------------+
*
*/

// Another way to get the Median of Salary ---> Get the median and join 

val median = empSalary.groupBy("deptName").agg(sort_array(collect_list('salary)).as("salaries"))
                      .select('deptName, 'salaries, element_at('salaries, (size('salaries)/2 +1 ).cast("int")).as("median_salary"))
median.show(false)

/**** Output *****
+--------+-----------------------------------+-------------+
|deptName|salaries                           |median_salary|
+--------+-----------------------------------+-------------+
|HR      |[35000, 39000]                     |39000        |
|Admin   |[48000, 48000, 50000]              |48000        |
|IT      |[42000, 45000, 52000, 52000, 60000]|52000        |
+--------+-----------------------------------+-------------+
*
*/

val joinedMedianSalary = empSalary.join(broadcast(median), "deptName")
                                  .select("deptName", "empNo", "empName", "salary", "salaries", "median_salary")

joinedMedianSalary.show(false)

/**** Output *****
+--------+-----+----------+------+-----------------------------------+-------------+
|deptName|empNo|empName   |salary|salaries                           |median_salary|
+--------+-----+----------+------+-----------------------------------+-------------+
|Admin   |21   |Katam     |50000 |[48000, 48000, 50000]              |48000        |
|HR      |22   |Karanpreet|39000 |[35000, 39000]                     |39000        |
|Admin   |33   |Rakesh    |48000 |[48000, 48000, 50000]              |48000        |
|Admin   |44   |Ravi      |48000 |[48000, 48000, 50000]              |48000        |
|HR      |55   |Suhani    |35000 |[35000, 39000]                     |39000        |
|IT      |71   |Deepak    |42000 |[42000, 45000, 52000, 52000, 60000]|52000        |
|IT      |81   |Veeru     |60000 |[42000, 45000, 52000, 52000, 60000]|52000        |
|IT      |90   |Manav     |45000 |[42000, 45000, 52000, 52000, 60000]|52000        |
|IT      |101  |Pritam    |52000 |[42000, 45000, 52000, 52000, 60000]|52000        |
|IT      |112  |Abhishek  |52000 |[42000, 45000, 52000, 52000, 60000]|52000        |
+--------+-----+----------+------+-----------------------------------+-------------+
*
*/

}
}

