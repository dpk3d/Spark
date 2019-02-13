 package com.deepak.common.utils
 
 import org.apache.spark.sql.{DataFrame, SparkSession}
 import org.apache.spark.sql._
 
 import com.deepak.common.constants.{CSVOption, Delimiters, GlobalConstants}

 /* Contains Common file Transformation and Operations */
 
 object FileFunctions {
 
 /*
 *@param df  -----------> Input DataFrame
 *@param outputPath  ---> Output File Path in HDFS
 *@param charset  ------> Charset Encoding
 *@param partitionCols -> List of Columns on which partioning need to be done
 */
 
 def writeDate (df: DataFrame,
				outputPath: String,
				partitionCols: List [String],
				charset: String = CSVOption.UTF8,
				headerExists: Boolean = false,
				compression: String = CSVOption.COMPRESSION_OFF,
				delimeters: String = Delimiters.PIPE,
				quote: String = "\"",
				mode: String = "error",
				save: Boolean = true): Unit = {
				
				if ( save ) {
				
					if ( partitionCols.nonEmpty && partitionCols.diff( df.columns).isEmpty) {
					
					df.write.partionBy( partitionCols: _*)
							.option(CSVOption.CHARSET, charset)
							.option(GlobalConstants.DELIMETER, delimeter)
							.option(GlobalConstants.HEADER, headerExists)
							.option(CSVOption.QUOTE, quote)
							.option(CSVOption.COMPRESSION, compression)
							.mode(mode)
							.csv(outputPath)
					}   
					else df.write.option(CSVOption.CHARSET, charset)
								.option(GlobalConstants.DELIMETER, delimeter)
								.option(CSVOption.QUOTE, quote)
								.option(CSVOption.COMPRESSION, compression)
								.mode(mode)
								.csv(outputPath)
					
					}
				}

 /*
 *@param df  -----------> Input DataFrame
 *@param outputPath  ---> Output File Path in HDFS
 *@param charset  ------> Charset Encoding
 *@param partitionCols -> List of Columns on which partioning need to be done
 */
 
 def writeIntermidiateDate (df: DataFrame,
							outputPath: String,
							partitionCols: List[String],
							save: Boolean,
							charset: String = CSVOption.UTF8,
							headerExists: Boolean = false,
							compression: String = CSVOption.COMPRESSION_OFF,
							delimeter: String = Delimiters.PIPE,
							quote: String = "\""
							) : Unit = {
							
				if (save) {
					writeData(df, outputPath, partitionCols, charset, headerExists, compression, delimeter, quote = quote)
					}
							}
	  /*
 * Methods to read a delimited file as DataFrame
 @@param spark 				-> Spark Session object
 *@param filePath		-----> Input file Path
 *@param requiredColumns 	-> The Column mapping to be used to split the flat file into DataFrame
 *@param delimeter		-----> Delimiter to split the flat file records
 *@param minFieldCount	-----> Minimum number of field required in DataFrame
 *@param charset  		----> Charset Encoding
 *@param return			----> DataFrame containing required fields
 */
 
 def readFileAsDataFrame (spark: SparkSession, filePath: String, requiredColumns: List[(String, Int)], delimeter: String,
							minFieldCount: Int , charset: String): DataFrame = {
							
		/* Generating a SELECT statement that will be used for fetching the required field from input flat file */

	val query = requiredColumns.map( columnMap => s"value[${columnMap._2}] as [${columnMap._1}] ")
	
		spark.read.option(GlobalConstants.DELIMETER, CSVOption.FIELD_DELIMETER)
				  .option(CSVOption.CHARSET, charset)
				  .csv(filePath)
				  .withColumn("value", split(col ("_c0"), delimeter ))
				  .drop("_c0")
				  .where(size(col("value")) > minFieldCount)
				  .selectExpr(query: _*)
				  }
				  
 /*
 * Methods to read a delimited file as DataFrame
 @@param sparkSession 		-> Spark Session object
 *@param headerExists		-> Header true/false
 *@param delimeter		-----> Delimiter value
 *@param inputPath		-----> Input File Path
 *@param return				->  Return the DataFrame
 */
 
 def readFileAsDataFrame (sparkSession: SparkSession, headerExists: Boolean, delimeter: String,
							inputPath: String, charset: String = CSVOption.UTF8): DataFrame = {
					
					sparkSession.read.option(GlobalConstants.DELIMETER, delimeter)
									 .option(GlobalConstants.HEADER, headerExists)
									 .option(CSVOption.CHARSET, charset)
									 .csv(inputPath)
									 .na.fill.(GlobalConstants.EMPTY_STRING)
							}
						
 /*
 * Methods to read a delimited file as DataFrame
 @@param sparkSession		-> Spark Session object
 *@param filePath		-----> Input file Path
 *@param requiredColumns 	-> The Column mapping to be used to split the flat file into DataFrame
 *@param delimeter		-----> Delimiter to split the flat file records
 *@param minFieldCount	-----> Minimum number of field required in DataFrame
 *@param charset  		----> Charset Encoding
 *@param return			----> DataFrame containing required fields
 */
 
 def readDelimitedFile (sparkSession: SparkSession, filePath: String, requiredColumns: List[(String, Int)],
						delimeter: String = Delimiters.PIPE, minFieldCount: Int = 0,
						charset: String = CSVOption.UTF8): DataFrame = {
						
		val query = requiredColumns.map(columnMap => s"_c${columnMap._2} as ${columnMap._1} " )
		
		sparkSession.read.option(GlobalConstants.DELIMETER, delimeter)
						 .option(CSVOption.CHARSET, charset)
						 .csv(filePath)
						 .selectExpr(query: _*)
						}
	 
/*
 * Methods to extracts a fixed length file
 *@param spark				-> Spark Session object
 *@param requiredColumns 	-> The Column mapping to be used to split the flat file into DataFrame
							Structure: (FILTER_TYPE, (delimeter, delimitedRecordsFileCondition), fixedLengthFilterCondition, List(Pattern to Filter))
 *@param return			----> DataFrame containing required fields
 */
 
 
 def fixedLengthParser (spark: SparkSession, df: DataFrame, requiredColumns: Map[String, (Int, Int)],
						columnName: String = "_c0"): DataFrame = {
						
		requiredColumns.keys.foldLeft( df ) {
		
		(df, fieldMap) => {
		df.withColumn(fieldMap, substring(col(columnName),
								requiredColumns(fieldMap)._1 + 1,
								requiredColumns(fieldMap)._2 - requiredColumns(fieldMap)._1 ))
			}
		}.drop(columnName)
	}
	
	
	
def execurteQuery (sparkSession: SparkSession, columnsList: Seq[String], query: String, outputPath: String) {

val regex = "\\$[0-9]".r
	columnsList.foreach(c => {
	println(regex.replaceAllIn(query, c))
	writeData(sparkSession.sql(regex.replaceAllIn(query, c)),
			outputPath + "//" + c,
			List (),
			headerExists = CSVOption.HEADER_TRUE,
			delimeter = Delimiters.PIPE)
	})
}
 /*
 * Methods to read Avro File as DataFrame
 *@param sparkSession		-> Spark Session object
 *@param inputPath		 	-> Input File path
 *@param return			----> DataFrame
 */
 
 def readAvroFileAsDataFrame(sparkSession: SparkSession, inputPath: String): DataFrame = {
 import com.databricks.spark.avro._*
 sparkSession.read.avro(inputPath)
 }
 
  /*
 * Methods to read JSON File as DataFrame
 *@param sparkSession		-> Spark Session object
 *@param inputPath		 	-> Input File path
 *@param return			----> DataFrame
 */
 
  def readJsonFileAsDataFrame(sparkSession: SparkSession, inputPath: String): DataFrame = {
 sparkSession.read.json(inputPath)
 }
 
   /*
 * Methods to read PARQUET File as DataFrame
 *@param sparkSession		-> Spark Session object
 *@param inputPath		 	-> Input File path
 *@param return			----> DataFrame
 */
   def readParquetFileAsDataFrame(sparkSession: SparkSession, inputPath: String): DataFrame = {
 sparkSession.read.parquet(inputPath)
 }
 
  /*
 * Methods to read Avro File as DataFrame
 *@param sparkSession		-> Spark Session object
 *@param inputPath		 	-> Input File path
 *@param return			----> DataFrame
 */
 
 def readAvroFileAsDataFrame(sparkSession: SparkSession, inputPath: String): DataFrame = {
 import com.databricks.spark.avro._*
 sparkSession.read.option(CSVOption.CHARSET, charset).avro(inputPath)
 }
 }
 
