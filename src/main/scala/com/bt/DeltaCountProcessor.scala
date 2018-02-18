package com.bt
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

  def main(args: Array[String]) {

    case class DeltaRecMetadata(entity: String, process: String, created: String, updated: String, deleted: String, job_date: String)
    Logger.getLogger(getClass).setLevel(Level.INFO)
    //Set Spark context for cluster
    val conf = new SparkConf().setAppName("DeltaCountProcessor")
    val sc = new SparkContext(conf)
    //Load Hive Context
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)
    //Load File System Object
    val fs = FileSystem.get(sc.hadoopConfiguration)
    //Get Absolute Date and Time to be used as job time in Metadata table
    val job_time: String = new java.text.SimpleDateFormat("YYYY-MM-dd HH:mm:ss").format(new java.util.Date())
    //Initialise job time to be Stored in Hive Table as metadata ad well as get Date in YYYY-MM-DD format for finding inserts updates and deletes
    val cur_sys_dt = new java.text.SimpleDateFormat("YYYY-MM-dd").format(new java.util.Date())

    //Check the length of the arguments
    val args_length = args.length
    if (args_length != 2) {
      Logger.getLogger(getClass).error("Current no. of arguments are : " + args_length + " required is : 2")
      System.exit(1)
    }

    //Load QueueName,TableName,Process_name,CRTD_COL,UPDT_COL,DEL_COL from Lookup file and check if exists
    val lkp_fl_path = args(0)
    if (!fs.exists(new Path(lkp_fl_path))) {
      Logger.getLogger(getClass).error("Input Folder doesnt exist : " + lkp_fl_path)
      System.exit(1)
    }

    //Load AbsoluteOutputPath
    val absOutputPath = args(1)
    val lkp_file_rdd = sc.textFile(lkp_fl_path)
    val lkp_file_with_flds = lkp_file_rdd.map(x => x.split(",")).map(x => (x(0), x(1), x(2), x(3), x(4), x(5))).collect
    var resultArray: Array[String] = Array()
    //Looup through each record in lookup file for the gathering delta records for each of the process for each of the entity
    for (lookup <- lkp_file_with_flds) {
      try {
        //Retrieving values for queuename,entity,process,created_col,updated_col,deleted_col
        val queueName = lookup._1
        val entity = lookup._2
        val process = lookup._3
        val crt_col = lookup._4
        val upd_col = lookup._5
        val del_col = lookup._6

        val lkp_input = "Queuename : ${queueName} , Entity : ${entity} , Process : ${process} , Created_Col : ${crt_col} , Update_col : ${upd_col} , Deleted_col : ${del_col} "
        Logger.getLogger(getClass).info(s"${lkp_input}")

        //Initialise Results to NA and use this if column value in lookup is also NA
        var fetch_ins_val = "NA"
        var fetch_upd_val = "NA"
        var fetch_del_val = "NA"
        var tot_val = "NA"
        var tot_act_val = "NA"
        var tot_inact_val = "NA"

        //Compute results for total records
        val tot_cnt_query = s"select count(1) AS cnt from ${queueName}.${process} "
        Logger.getLogger(getClass).info(s"Total Count Query is : ${tot_cnt_query}")
        val tot_cnt = hc.sql(s"${tot_cnt_query}")
        tot_val = tot_cnt.map(RowValues => RowValues.getAs[Any]("cnt")).collect()(0).toString

        //Compute result for insert if Created column is not NA
        if (crt_col != "NA") {
          //Find Inserts
          val ins_query = s"select count(1) AS ins from ${queueName}.${process} where SUBSTR(${crt_col},0,10) >= '${cur_sys_dt}' AND SUBSTR(${upd_col},0,10) >= '${cur_sys_dt}'"
          Logger.getLogger(getClass).info(s"Insert Query is : ${ins_query}")
          val ins_cnt = hc.sql(s"${ins_query}")
          fetch_ins_val = ins_cnt.map(RowValues => RowValues.getAs[Any]("ins")).collect()(0).toString
        }

        //Compute result for update if Updated column is not NA
        if (upd_col != "NA") {
          if (del_col != "NA") {
            //Find Updates
            val upd_query = s"select count(1) AS upd from ${queueName}.${process} where SUBSTR(${upd_col},0,10) >= '${cur_sys_dt}' AND SUBSTR(${upd_col},0,10) != SUBSTR(${crt_col},0,10) AND ${del_col} = 'N'"
            Logger.getLogger(getClass).info(s"Update Query is : ${upd_query}")
            val upd_cnt = hc.sql(s"${upd_query}")
            fetch_upd_val = upd_cnt.map(RowValues => RowValues.getAs[Any]("upd")).collect()(0).toString
          } else {
            //Find Updates
            val upd_query = s"select count(1) AS upd from ${queueName}.${process} where SUBSTR(${upd_col},0,10) >= '${cur_sys_dt}' AND SUBSTR(${upd_col},0,10) != SUBSTR(${crt_col},0,10) "
            Logger.getLogger(getClass).info(s"Update Query is : ${upd_query}")
            val upd_cnt = hc.sql(s"${upd_query}")
            fetch_upd_val = upd_cnt.map(RowValues => RowValues.getAs[Any]("upd")).collect()(0).toString

          }
        }

        //Compute result for delete if Deleted column is not NA
        if (del_col != "NA") {
          //Fine deletes
          val del_query = s"select count(1) AS del from ${queueName}.${process} where SUBSTR(${upd_col},0,10) >= '${cur_sys_dt}' AND SUBSTR(${upd_col},0,10) != SUBSTR(${crt_col},0,10) AND ${del_col} = 'Y'"
          Logger.getLogger(getClass).info(s"Delete Query is : ${del_query}")
          val del_cnt = hc.sql(s"${del_query}")
          fetch_del_val = del_cnt.map(RowValues => RowValues.getAs[Any]("del")).collect()(0).toString

          //Compute results for total active records
          val tot_act_query = s"select count(1) AS cnt from ${queueName}.${process} where  ${del_col} = 'N' "
          Logger.getLogger(getClass).info(s"Total Active Count Query is : ${tot_act_query}")
          val tot_act_cnt = hc.sql(s"${tot_act_query}")
          tot_act_val = tot_act_cnt.map(RowValues => RowValues.getAs[Any]("cnt")).collect()(0).toString

          //Compute results for total inactive records
          val tot_inact_query = s"select count(1) AS cnt from ${queueName}.${process} where  ${del_col} = 'Y' "
          Logger.getLogger(getClass).info(s"Total Inactive Count Query is : ${tot_inact_query}")
          val tot_inact_cnt = hc.sql(s"${tot_inact_query}")
          tot_inact_val = tot_inact_cnt.map(RowValues => RowValues.getAs[Any]("cnt")).collect()(0).toString
        }

        //Return metadata to be written to the file
        val result = entity + "," + process + "," + fetch_ins_val + "," + fetch_upd_val + "," + fetch_del_val + "," + tot_val+"," + tot_act_val+"," +tot_inact_val+"," + job_time
        resultArray = resultArray :+ result
      } catch {
        case e: Exception => {
          e.printStackTrace();
          Logger.getLogger(getClass).error("Cannot compute results due to error : " + e.toString + " with stack-trace : " + e)
          System.exit(1)
        }
      }

    }
    //Store the result and Delete file already present in HDFS
    if (fs.exists(new Path(absOutputPath))) {
      try {
        fs.delete(new Path(absOutputPath))
      } catch {
        case e: Exception => {
          e.printStackTrace();
          Logger.getLogger(getClass).error("Cannot remove director because of Exception : " + e.toString + " with stack-trace : " + e)
          System.exit(1)
        }

      }
    }
    val count_per_process = sc.parallelize(resultArray).repartition(1)
    count_per_process.saveAsTextFile(absOutputPath)

  }

}
 