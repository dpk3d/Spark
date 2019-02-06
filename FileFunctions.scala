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
  
 }
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 

 
 
 
