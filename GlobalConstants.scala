package com.deepak.common.constants

import scala.collection.immutable.ListMap

object GlobalConstants {
		
	val SPACE = " "
	val INPUT_FILE_DELIMETER = "[|]@[|]"
	val OUTPUT_FILE_DELIMETER = "|@|"
	val HEADER_FIELD_COUNT = 3 
	
	val EMPTY = ""
	val RECEIVED_DATE = "receivedDate"
	val RECEIVED_DATE_REGEX = "\\d{14}[+|-]\\d{4}"
	val DEFAULT_TIMEZONE_FORMAT = "yyyyMMddHHmmssZ"
	val EMPTY_STRING = ""
	val DATA_RECORD = "dataRecord"
	val FILE_NAME = "fileName"
	val DELIMETER = "delimeter"
	val HEADER = "header"
	
	}
	
	/* Delimeter used in parser */	
	
object FIELD_DELIMETER extends Enumeration {

	val FIXED = Value ("")
	val PIPE = Value (\\|)
	val DELIMETED = Value(GlobalConstants.INPUT_FILE_DELIMETER)
	val JSON = Value ("")
	val ETX = Value ("\u0003")
	val CAP = Value ("\\^")

}

// To Denote the Record Tyepe

object RECORD_TYPE extends Enumeration {

	val FIXED = Value ("fixed")
	val DELIMETED = Value ("delimeted")
	val JSON = Value ("json")
}

// Common CSV Option

object CSVOption {
 
 val HEADER_TRUE = true
 val HEADER_FALSE = false
 val QUOTE = "quote"
 val EMPTY = ""
 val SEMICOLON_DELIMETER = "[;]"
 val COLON_DELIMETER = "[:]"
 val COMA_DELIMETER = "[,]"
 val PIPE_DELIMETER = "[|]"
 val DOT_DELIMETER = "[.]" 
 val ETX_DELIMETER = "\\^C"
 val FIELD_DELIMETER = "\u0001"
 val CHARSET = "charset"
 val CHARSET_ISO8859_15 = "ISO8859_15"
 val COMPRESSION = "compression"
 val COMPRESSION_OFF = "none"
 val SNAPPY = "snappy"
 val UTF8 = "UTF-8"
 
}

// Common Delimeters

object Delimeters {

 val SEMICOLON = ";"
 val COLON = ":"
 val PIPE = "|"
 val ETX = "\u0003"
 val SPACE = " "
 val DOT = "."
 val HYPHEN = "-"
 val CAP = "^"
 val ONE = "\1"
 
}

// Various Date Formats

object DateFromats {

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

object FilterTypes extends Enumeration {

	val DELIMETED = "delimitedFilter"
	val FIXED = "fixedLengthFilter"
	val PATTERN = "filterByPattern"
}
