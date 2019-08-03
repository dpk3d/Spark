package com.deepak.common.utils

import com.deepak.common.constants.{CSVOption, Delimiters, GlobalConstants}
import com.deepak.common.constants.GlobalConstants._

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, functions}


/* Utility Class: Contains commonly utilised String operations */
object StringFunctions {

    /**
      * Padding is applied to every columns for the provided dataFrame , by consuming the Padding information specified in the Constants
      * It will also add the columns that are not present in dataFrame with default values
      *
      * @param dataFrameForPadding -> The dataFrame which has to undergo the padding process
      * @param paddingInformation  -> Padding info Map
      * @return                    -> A DataFrame where every column is padded to its accurate lengths and accurate pad character.
      */
    def padColumns( dataFrameForPadding: DataFrame, paddingInformation: Map[String, (Int, String, String, String)] ): DataFrame = {
        val inputColumns = dataFrameForPadding.columns

        val columns: List[Column] = paddingInformation.keys.toList.map {
            column => {
                {
                    if ( inputColumns.contains( column ) ) {
                        paddingInformation( column )._2 match {
                            case "L" =>
                                if ( !paddingInformation( column )._4.equalsIgnoreCase( EMPTY ) ) {
                                    when( length( trim( col( column ) ) ) =!= 0, lpad( col( column ),
                                                                                       paddingInformation( column )._1,
                                                                                       paddingInformation( column )._4
                                                                                     )
                                        )
                                    .otherwise( lpad( col( column ), paddingInformation( column )._1, paddingInformation( column )._3 ) )
                                }
                                else
                                    lpad( col( column ), paddingInformation( column )._1, paddingInformation( column )._3 )

                            case "R" => rpad( col( column ), paddingInformation( column )._1, paddingInformation( column )._3 )
                        }
                    }
                    else {
                        lpad( lit( paddingInformation( column )._3 ).alias( column ), paddingInformation( column )._1,
                              paddingInformation( column )._3 )
                    }
                }.alias( column )
            }
        }
        dataFrameForPadding.select( columns.map( c => c ): _* )
    }

    /**
      * Method to create fixed length DataFrame which holds two Half's / Parts of information
      * Part1 subscriber Code , type of account, Date first event , subscriber event - which will be used to generate the
      * header and the footers.
      * Part2 fixed length record.
      *
      * @param dataFrame     -> DataFrame with padded values
      * @param selectColumns -> List of Columns to select from DataFrame
      * @param concatColumns -> List of Columns to concat as single column
      * @return              -> DataFrame where all the columns are converged into a single column
      */
    def concatFields( dataFrame: DataFrame, selectColumns: List[String], concatColumns: List[String] ): DataFrame = {
        dataFrame.select( selectColumns.map( col( _ ) ).+:( concat( concatColumns.map( column => col( column ) ): _* ).alias( DATA_RECORD ) ): _* )
    }

    /**
      * Method returns the name based on indicator
      *
      * @param indicator  -> Column for indicator field
      * @param fullName   -> name field from ICMCHK file
      * @return           -> returns the complete name
      */
    def getCompleteName( indicator: Column, fullName: Column ): Column = {
        when( indicator === "9", StringFunctions.subStringName( fullName ) ).otherwise( EMPTY_STRING )
    }

    /**
      * Method to trim Complete Name to have length less than or equal to 45
      *
      * @param inputName -> Input Name to be normalized have maximum length of 45
      * @return          -> Normalized Name to be 45 characters in length
      */
    def subStringName( inputName: Column ): Column = {
        when( length( inputName ).gt( 45 ), substring( inputName, 0, 45 ) ).otherwise( inputName )
    }

    /**
      * Historical data purposes, blockIndicator of type 3 has to be shown/represented as Type 5
      *
      * @param    blockIndicator -> BlockIndicator column with values 0 TO 9
      * @return                  -> Required Output : blockIndicator column with value mapped 3 to 5.
      */
    def getBlockIndicator( blockIndicator: Column ): Column = {
        when( blockIndicator === "3", "5" ).otherwise( blockIndicator )
    }

    /**
      * Converts String type to Integer Type and returns defaultValue if the input string is invalid
      *
      * @param s            -> Input string to be converted to Integer Type
      * @param defaultValue -> The default value to be return if the input string is an invalid string
      * @return             -> The Integer equivalent of the String or default value if it is an invalid string
      */
    def stringToInt( s: String, defaultValue: Int = 0 ): Int = {
        Try( s.trim.toInt ) match {
            case Success( s ) => s
            case Failure( s ) => defaultValue
        }
    }

    /**
      * Returns the number of months between two date
      *
      * @param startDate -> Starting date
      * @param endDate   -> ending date
      * @return          -> Number of months between Start date and End date
      */
    def monthsBetween( startDate: String, endDate: String ): Int = {
        val yearDiffToMonth = ( endDate.substring( 0, 4 ).toInt - startDate.substring( 0, 4 ).toInt ) * 12
        val monthsDiff = endDate.substring( 4, 6 ).toInt - startDate.substring( 4, 6 ).toInt
        yearDiffToMonth + monthsDiff
    }

    // @formatter:on
    /**
      * Generates a  Filename compatible using fileNameTemplate
      * Current time will be the filename Timestamp
      *
      * @param dataFrame        Input DataFrame
      * @param fileNameTemplate filename format ex:"CL<SubscriberCode><NITEntity><cutOffDate>"
      * @return           DataFrame with a Filename
      */

    def generateFileName( dataFrame: DataFrame, fileNameTemplate: String, patternColumns: List[String] = List( ) ): DataFrame = {

        val fileDF = dataFrame.withColumn( FILE_NAME, lit( fileNameTemplate ) )

        val fileNameDF = if ( patternColumns.isEmpty ) {
            dataFrame.columns.foldLeft( fileDF )( ( fileDF, columnName ) => {
                fileDF.withColumn( FILE_NAME,
                                   functions.regexp_replace( functions.col( FILE_NAME ), lit( "<" + columnName + ">" ), col( columnName ) ) )
            } )
        } else {
            patternColumns.foldLeft( fileDF )( ( fileDF, columnName ) => {
                fileDF.withColumn( FILE_NAME,
                                   functions.regexp_replace( functions.col( FILE_NAME ), lit( "<" + columnName + ">" ), col( columnName ) ) )

            } )
        }

        fileNameDF
    }

    /**
      * Filters the either Delimited or Fixed Length Header from the given input DataFrame
      *
      * @param df         -> Input DataFrame
      * @param column     -> Column to be filtered for header
      * @param filterInfo -> Contains information about what to be filtered.
      *                   Structure:(FILTER_TYPE, (delimiter, List(delimitedRecordsFilterCondition)), List(fixedLengthFilterCondition), List(Patterns to
      *                   filter))
      * @return           -> DataFrame with filtered records
      */
    def filterRecords( df: DataFrame, column: String, filterInfo: Map[String, List[String]], delimiter: String = CSVOptions.PIPE_DELIMITER ): DataFrame = {
        // @formatter:off
        filterInfo.keys.foldLeft( df ) {
            ( df, filterType ) => {
                filterType match {
                    case FilterTypes.DELIMITED => filterInfo( filterType ).foldLeft( df ) {
                        ( df, filterCondition ) => {df.filter( !( size( split( col( column ), delimiter ) ) === filterCondition.toInt ) )}
                    }
                    case FilterTypes.FIXED => filterInfo( filterType ).foldLeft( df ) {
                        ( df, filterCondition ) => {df.filter( length( col( column ) ) > filterCondition.toInt )}
                    }
                    case FilterTypes.PATTERN => filterInfo( filterType ).foldLeft( df ) {
                        ( df, filterCondition ) => {df.filter( !col( column ).startsWith( filterCondition ) )}
                    }
                    case _ => df
                }
            }
        }
         // @formatter:on
    }

    /**
      * Add RECEIVED_DATE to the DataFrame, checks the condition and convert timestamp to DEFAULT_TIMEZONE_FORMAT
      * @param dataFrame          -> Input DataFrame
      * @param datePatternRegex   -> regex to extract receiveDate from filename
      * @return                   -> DataFrame with RECEIVED_DATE
      */
    def getDateFromFilename( dataFrame: DataFrame, datePatternRegex: String, columnName: String = RECEIVED_DATE ): DataFrame = {
        // @formatter:off
        dataFrame.withColumn( columnName, regexp_extract( input_file_name( ), datePatternRegex, 0 ) )
                 .withColumn( columnName, when( trim( col( columnName ) ) =!= EMPTY, col( columnName ) )
                                              .otherwise( DateFunctions.getCurrentTimezone( DEFAULT_TIMEZONE_FORMAT ) ) )
        // @formatter:on
    }

    /**
      * Join Multiple (2 or more) DataFrames with a common set of Primary Keys
      * The order in the which the DataFrames will be supplied will be maintained during the JOIN process
      * @param joinKeys        -> The Primary Keys on which the JOIN has to be performed on the DataFrames
      * @param joinType        -> The Type of JOIN to be performed on the DataFrames
      * @param dataFrameA      -> The base or the First DataFrame in the JOIN sequence
      * @param renameColumnMap -> Any rename columns that are required to be renamed should be provided here, this rename operation will occur after the JOIN
      * @param requiredColumns -> Fetches on the Columns which are required after the JOIN, thus dropping unwanted columns
      * @param dataFrameB      -> The DataFrames that are to be JOINED with the base DataFrame
      *                        -> Order provided in this parameter will be maintained during the JOIN operation
      * @return                -> Returns the Final JOINED and post processed DataFrame as per the requirement provided
      */
    def join( dataFrameA: DataFrame,
              dataFrameB: DataFrame,
              joinKeys: List[String],
              joinType: String,
              renameColumnMap: Map[String, String] = Map( ),
              requiredColumns: List[String] = List( ) ): DataFrame = {

        /* Joins Multiple DataFrame with a similar primary keys */
        val joinedDF = dataFrameA.join( dataFrameB, joinKeys, joinType ).na.fill( EMPTY_STRING )

        /* Renames the columns to as per the renameColumnMap information provided */
        val renamedDF = renameColumnsNames( joinedDF, renameColumnMap )

        /* Fetch only the required columns from the Final DataFrame */
        selectColumns( renamedDF, requiredColumns )
    }

    /**
      * Join 2 DataFrames based on a JOIN expression provided
      * Utilize this JOIN when the Columns to be joined in either DataFrames are not having the same Column Names
      * @param dataFrameA      -> Base DataFrame to be JOINED
      * @param dataFrameB      -> DataFrame to be JOINED
      * @param joinExpr        -> JOIN Expression or JOIN condition
      * @param joinType        -> JOIN type
      * @param renameColumnMap -> Columns to be renamed if required
      * @param requiredColumns -> Fetches ont he required columns after the join, thus dropping the unwanted columns
      * @return                -> Returns a JOINED dataFrame as per the requirements
      */
    def joinByExpr( dataFrameA: DataFrame,
                    dataFrameB: DataFrame,
                    joinExpr: Column,
                    joinType: String,
                    renameColumnMap: Map[String, String] = Map( ),
                    requiredColumns: List[String] = List( ) ): DataFrame = {

        val joinedDF = dataFrameA.join( dataFrameB, joinExpr, joinType )
        val renamedDF = renameColumnsNames( joinedDF, renameColumnMap )
        selectColumns( renamedDF, requiredColumns )
    }

    /**
      * Rename the Column names of a DataFrame
      * @param dataFrame       -> DataFrame for which the column names has to be renamed
      * @param renameColumnMap -> Old Columns name -> new Column name Map to perform the rename operation
      * @return                -> DataFrame with its column name renamed as per renameColumnMap
      */
    def renameColumnsNames( dataFrame: DataFrame, renameColumnMap: Map[String, String] ): DataFrame = {
        if ( renameColumnMap.isEmpty )
            dataFrame
        else
            renameColumnMap.foldLeft( dataFrame ) {( df, newName ) => df.withColumnRenamed( newName._1, newName._2 ) }
    }

    /**
      * Fetches only the required columns from the input DataFrame
      * @param dataFrame       -> Input DataFrame
      * @param requiredColumns -> Columns to be Fetched from the input DataFrame
      * @return                -> DataFrame with only the required columns
      */
    def selectColumns( dataFrame: DataFrame, requiredColumns: List[String] ): DataFrame = {
        if ( requiredColumns.isEmpty )
            dataFrame
        else
            dataFrame.selectExpr( requiredColumns: _* )
    }

    /**
      * Method to Extract fileName for file path
      *
      * @param fileNameRegex : Regex to extract
      * @return
      */
    def getFileName( fileNameRegex: String ): Column = {
        regexp_extract( input_file_name( ), fileNameRegex, 0 )
    }

    /**
      * Method to trim leading zeros
      * @param rawFieldMap  ->  Input data
      * @param fields       ->  Input->Output columns map
      */
    def standardizeFields( rawFieldMap: mutable.LinkedHashMap[String, String], fields: Map[String, String] ): Unit = {
        fields.keys.foreach( field => rawFieldMap.put( fields( field ), rawFieldMap( field ).toString.replaceAll( "^0+(?!$)", "" ) ) )
        rawFieldMap.put( "isMigrated", "true" )

    }

    def updateIDTypeForDB2Files(dataFrame: DataFrame,
                                idTypeCorrectionMapping: Map[String, String] = GlobalConstants.Mapping.ID_TYPE_CORRECTION_COBOL): DataFrame = {

        val correctIdType = udf((idType: String) => {
            idTypeCorrectionMapping.getOrElse(idType, idType)
        })

        val toLong = udf[Long, String](_.toLong)

        dataFrame.withColumn(GlobalConstants.ID_TYPE, when(toLong(col(GlobalConstants.ID_NUMBER)) > 599999999 && col(GlobalConstants.ID_TYPE) === "2"
          && toLong(col(GlobalConstants.ID_NUMBER)) < 800000000, lit("3")).otherwise(
            correctIdType(col(GlobalConstants.ID_TYPE))))

    }

    /**
      *
      * Method to update ID Number and ID Type for Superintendencia/Credit/Checking records
      * @param dataFrame -> Input dataFrame
      * @return          -> DataFrame with updated ID Number and ID Type
      */
    def updateIDNumberAndIDTypeForCobolFiles(dataFrame: DataFrame ): DataFrame = {

        /* Performs the Superintendencia Reverse engineering for ID Type */
        val correctIdType = udf( ( idNumberFirstCharacter: String, idType: String ) => GlobalConstants.Mapping.ID_TYPE_CORRECTION_COBOL.getOrElse( idNumberFirstCharacter, idType ) )

        /* Performs the Superintendencia Reverse engineering for ID Number */
        val existsInCorrectionList = udf( ( idNumberFirstCharacter: String ) => GlobalConstants.Mapping.ID_NUMBER_CORRECTION_COBOL.contains( idNumberFirstCharacter ) )

        // @formatter:off
        dataFrame.withColumn( ID_NUMBER_FIRST_CHARACTER, substring( col( ID_NUMBER ), 0, 1 ) )
          .withColumn( ID_NUMBER, when(col(ID_TYPE) === "4",
              when(existsInCorrectionList( col( ID_NUMBER_FIRST_CHARACTER ) ),
                  regexp_replace( col( ID_NUMBER ), "^.", "0" ) )
                .otherwise( col(ID_NUMBER ) )
          )
            .otherwise( col( ID_NUMBER ) )
          )
          .withColumn( ID_TYPE, when( col( ID_TYPE ) === "2", "3" )
            .when( col( ID_TYPE ) === "4",
                correctIdType( col( ID_NUMBER_FIRST_CHARACTER ), col( ID_TYPE ) )
            )
            .otherwise( col( ID_TYPE ) )
          )

          .drop( col( ID_NUMBER_FIRST_CHARACTER ) )
        // @formatter:on
    }

}
