import java.time.zonedDateTime
import java.time.format.DateTimeFormatter

import scala.util.try

import org.apache.spark.sql.{functions, Column}
import org.apache.spark.sql.functions._

import org.joda.time.format.DateTimeFormat

/* Various Date Formats  */
object DateFormats {

val YYYYMMDD = "yyyymmdd"
val DDMMYYYY = "ddmmyyyy"
val YYMMDD = "yymmdd"
val DDMMYY = "ddmmyy"
val MMYYYY = "mmyyyy"
val YYYYMM = "yyyymm"
val MMYY = "mmyy"
val YYMM = "yymm"
val YYYY_MM_DD = "yyyy_mm_dd"
val DD_MM_YYYY = "dd_mm_yy"
}

/* General Date Transformation  */
object DateTransforamtion {

/* Simple Date Class */
case class Date(year: String, month: String, day: String)

/* Getting a year from a given date
*
*@param date -> String in yyyyMMdd format
*@return -> String in yyyy
*/

def getYear(date: String): String = {
date.substring(0,4)
}

/* Getting a month from a given date
*
*@param date -> String in yyyyMMdd format
*@return -> String in MM format
*/

def getMonth(date: String): String = {
date.substring(4,6)
}

/* Getting a date from a given date
*
*@param date -> String in yyyyMMdd format
*@return -> String in dd format
*/

def getDate(date: String): String = {
date.substring(6,8)
}


/* Getting number of months between two given date
*
*@param start -> Input Date
*@param end -> Input Date
*@param inputFormat -> Date Format
*@return -> Number of months between the two input dates
*/

def monthsBetween (start: Column, end:Column, inputFormat: String = YYYYMM): Column = {

val startDate = to_date(unix_timestamp(start.substring(1,6),inputFormat).cast("timestamp")) 
val endDate = to_date(unix_timestamp(end.substring(1,6), inputFormat).cast("timestamp")) 
months_between (startDate, endDate).cast("Int")
}






