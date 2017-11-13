package org.local
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.util.Calendar
import org.apache.spark.sql._


object DeltaCountProcessor {
  
  
 Logger.getLogger(getClass).setLevel(Level.INFO)
 //Set Spark context for cluster
 val sc = new SparkContext("local[*]", "DeltaProcess") 
/* val conf=new SparkConf().setAppName("DeltaCountProcessor")
 val sc=new SparkContext(conf) */
 //Load Hive Context
 val hc=new org.apache.spark.sql.hive.HiveContext(sc)
 //Load File System Object
 val fs=FileSystem.get(sc.hadoopConfiguration)
 val output = new Path("/user/HAASAAP0256_05038/myinputtest");  
 val job_time:String = (Calendar.getInstance().getTime()).toString 
 //Initialise job time to be Stored in Hive Table as metadata ad well as get Date in YYYY-MM-DD format for finding inserts updates and deletes
 val cur_sys_dt=new java.text.SimpleDateFormat("YYYY-MM-dd").format(new java.util.Date())
  
  def main(args: Array[String]) {
    
    
//Load QueueName,TableName,Process_name,CRTD_COL,UPDT_COL,DEL_COL from Lookup file 
val lkp_fl_path = "C:\\SparkScala\\lookup_path.dat"
val lkp_file_rdd = sc.textFile(lkp_fl_path)
val lkp_file_with_flds=lkp_file_rdd.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2),x(3),x(4),x(5)))
//Map the RDD to return Counts for the process for each of the entity
val count_per_process=lkp_file_with_flds.map(retrieve_data_cnt)
//Store the RDD in a file with convention of date
val fl_nm=s"${job_time}"
//count_per_process.saveAsTextFile("/user/HAASAAP0256_05038/delta_entity_counts_1") 
count_per_process.collect
    
  }
  
def retrieve_data_cnt(lookup:(String, String, String, String, String,String))={
//Retrieving values for queuename,entity,process,created_col,updated_col,deleted_col
val queueName=lookup._1
val entity=lookup._2
val process=lookup._3
val crt_col=lookup._4
val upd_col=lookup._5
val del_col=lookup._6

val lkp_input="Queuename : ${queueName} , Entity : ${entity} , Process : ${process} , Created_Col : ${crt_col} , Update_col : ${upd_col} , Deleted_col : ${del_col} "
Logger.getLogger(getClass).info(s"${lkp_input}")

//Find Inserts
val ins_query=s"select count(1) AS ins from ${queueName}.${process} where SUBSTR(${crt_col},0,10) = '{cur_sys_dt}'"
Logger.getLogger(getClass).info(s"Insert Query is : ${ins_query}")
val ins_cnt=hc.sql(s"${ins_query}")
val fetch_ins_val = ins_cnt.map(RowValues=>RowValues.getAs[Any]("ins")).collect()(0).toString
//Find Updates
val upd_query=s"select count(1) AS upd from ${queueName}.${process} where SUBSTR(${upd_col},0,10) = '{cur_sys_dt}' AND SUBSTR(${upd_col},0,10) != SUBSTR(${crt_col},0,10) AND ${del_col} = 'N'"
Logger.getLogger(getClass).info(s"Insert Query is : ${upd_query}")
val upd_cnt=hc.sql(s"${upd_query}")
val fetch_upd_val = upd_cnt.map(RowValues=>RowValues.getAs[Any]("upd")).collect()(0).toString
//Fine deletes
val del_query=s"select count(1) AS del from ${queueName}.${process} where SUBSTR(${upd_col},0,10) = '{cur_sys_dt}' AND SUBSTR(${upd_col},0,10) != SUBSTR(${crt_col},0,10) AND ${del_col} = 'Y'"
Logger.getLogger(getClass).info(s"Insert Query is : ${del_query}")
val del_cnt=hc.sql(s"${del_query}")
val fetch_del_val = del_cnt.map(RowValues=>RowValues.getAs[Any]("del")).collect()(0).toString
//Return metadata to be written to the file
entity+","+process+","+fetch_ins_val+","+fetch_upd_val+","+fetch_del_val+","+cur_sys_dt
}
}
 