package com.deepak.spark

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  
  def main(args: Array[String]): Unit = {
    
    //Suppressing the log
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    
    //Initializing the Spark Context
   val conf = new SparkConf().setMaster("local").setAppName("Word Count")
   val sc = new SparkContext(conf)
    
    //Loading the data
    val lines = sc.textFile("Data/Lines")
    
    //Converting lines into word
    val word = lines.flatMap(rec => rec.split(" "))
    
    //word.collect().foreach(println)
    
    //let the word maintains it's count ----(word,1); key-->word; value --> count
    
    val wordkv = word.map(word => (word,1)) //Pair-RDD -->Any RDD which holds data in key/value pair, there are special
                                        //action offered by spark to Pair RDD like reduceByKey, countByKey
    //Count the unique word
    
    val countWord = wordkv.reduceByKey((accumulator, input) => accumulator + input) //reduceByKey is only for Pair RDD
                            // In reduceByKey, the input is the value part of K,V pair
    
    /*
     * wordsKV: RDD[(Hadoop,1),(Hadoop,1),(Spark,1),(Spark,1)]
     * reduceByKey:
     * 				Phase1: Group operation- The grouping is done based on Key part of the data
     * 				select * from kv group by key;
     * 
     * 						(Hadoop ,(Hadoop,1),(Hadoop,1))
     * 						(Spark , (Spark,1),(Spark,1))
     * 
     * 				Phase2: Shuffle operation
     * 
     * 						(Hadoop ,(1),(1))
     * 						(Spark , (1),(1))
     * 		
     * 				Phase3: reduce over value part
     * 					Hadoop
     * 							acc = 0 , input = 1 , 0+1 , acc = 1
     * 							acc = 1 , input = 1 , 1+1 , acc = 2
     * 					(Hadoop,2)
     * 					Spark
     * 							acc = 0 , input = 1 , 0+1 , acc = 1
     * 							acc = 1 , input = 1 , 1+1 , acc = 2						
     * 					(Spark,2)
     * 
     */
    
   // countWord.collect().foreach(println)
    
    val sortRDD = countWord.sortByKey(false) //Sorts the data in descending order based on value part of the data.
    
   sortRDD.collect().foreach(println)
   
  }
}
