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
  
/* Add the Specified months for a given Date
*
*@note Providing a negative value substracts the months from dates
*@param Column -> Initial / Given Date
*@param monthsToAdd -> Months to be added
*@param outputFormat -> Output format of the date required
*@return -> Derived date with Additional Month Added
*/
  
 def addMonths (column: Column, monthsToAdd: Int, outputFormat: String = YYYYMMDD) : Column = {
    val stringToDate = to_date (unix_timstamp(column , YYYYMMDD).cast("timestamp"))
   val endOfMonth = add_months (stringToDate , monthsToAdd)
   date_format (endOfMonth, outputFormat)
 } 
  
/* Subtracts the number of months for a given Date
*
*@note Providing a negative value substracts the months from dates
*@param Date -> Input Date
*@param monthsToSubtract -> Months to subtracts from the input date
*@return -> Date After Substractions
*/
  
def substractsMonths (Date: String , monthsToSubstract: Int = 1 ): String = {
  var year = date.substring(0, 4)
  var month = date.substring (4, 6)
  
  if (month  == 1) {
      month = 12
      year = year - 1
  }
    else {
        month = month - 1
    }
  
  "04d%02d".format( year, month)
}
  
/*
* Rerun the current times stamp
*@param timestampFormat -> Required Output Timestamp Format
*@return -> Return Date in required timestamp format
*/
  
def currentTimeZone (timestampFormat: String ) : Column = {
    from_unixtime( unix_timestamp, timestampFormat)
}
  
/*
* To check whether date is Valid or Not
*@param Date -> Input Date
*@return -> Boolean status, date is valid or not
*/
  
  def isValidDate (Date : String) = Boolean = {
      try {
          var status = false
          if (Date.trim.equalsIgnoreCase("") || Date.toInt == 0)
            status = true
        else if (Date.toInt != 0 ) {
          val fmt = DateTimeFormat.forPattern( YYYYMMDD )
            Try ( fmt.parseDateTime(Date)).isSuccess
          status = Try (fmt.parseDateTime(Date)).isSuccess
        }
        status
      }
      catch {
            case e: Exception => false
      }
  }
  






