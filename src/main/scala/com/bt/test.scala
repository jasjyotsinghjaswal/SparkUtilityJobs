package com.bt

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark._ 

object test {
   def main(args: Array[String]) {
  //val conf = new SparkConf().setAppName("wordCount").setMaster("local")
  val sc = new SparkContext("local[*]", "MarksComputation")  
  val sqlContext= new org.apache.spark.sql.SQLContext(sc)
  
  val inputFile = "C:\\Users\\611253444\\testinput.txt"
  val input = sc.textFile(inputFile)
  input.take(10).foreach(println)
   }
  
}