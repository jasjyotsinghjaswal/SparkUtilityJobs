package com.bt
import org.apache.spark.sql._
import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.apache.spark.util.Utils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions
import org.apache.spark.sql.functions.{ concat, lit }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf


case class lookup_col_name ( status_key:String ,status_description:String )

object le_dnb {

  
  def main(args: Array[String]) {
    
    
    try
    {
 val conf = new SparkConf().setAppName("LE_DNB")
    val sc = new SparkContext(conf)
    sc.setLogLevel("DEBUG")
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val _length: String => Int = _.length
    val _lengthUDF = udf(_length)
    //------------------------------first occurance of a string replace---------------------------------------------------------
    val replacefirstoccur = (field: String, search_char: String) => {
      Logger.getLogger(getClass).info("The field is : "+field+" Search Char is : "+search_char)
      var find_first_index = field.indexOf(search_char)
      var mod_str = ""
      if (find_first_index == -1)
        mod_str = ""
      else {
        var string_prt1 = field.substring(0, find_first_index)
        var string_prt2 = field.substring(find_first_index + 1, field.length)
        mod_str = string_prt1 + string_prt2
      }
      mod_str
    }

    val replacefirstoccur_udf = udf(replacefirstoccur)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    //----------------------load odm_crs_load_dnb_crs_int_test lookup file---------------------------------
    val odm_crs_load_dnb_crs_company_status_lookup = sc.textFile("/user/HAASAAP0256_05038/odm_crs_load_dnb_crs_int_test").map(column => lookup_col_name(column.substring(0, 2).trim(), column.substring(2, column.length()).trim())).toDF()
    //--------------------------------------------load all the tables----------------------------------------------------------------------------------------------
    //----------A.read from le_dnb take all feeds----------------------------------------------------------------------------------------------------------------
    val dq_data_le_dnb = hiveContext.sql("select * from HAASAAP0346_05038.dq_data_le_dnb").persist()
    val fil_status_cir_src_cdmc_cir_hub = hiveContext.sql("select * from HAASAAP0346_05038.cdmc_cir_hub where cir_src = 'LE_DNB' and status= 'A'").persist()

    val le_dnb_reg_name_not_blank = hiveContext.sql("select * from HAASAAP0346_05038.sim_le_dnb").filter($"reg_name" !== "").persist()
    val le_dnb_reg_name_blank = hiveContext.sql("select * from HAASAAP0346_05038.sim_le_dnb").filter($"reg_name" === "").rdd;
    le_dnb_reg_name_blank.map(x => x.mkString("\u0001")).saveAsTextFile("/user/HAASAAP0256_05038/le_dnb_crs_test/dnb_input_reg_name_null_data.dat")
    //------------------flow1 dedup on dnb_num-----------------------------------------------------------------------------
    val windowSpec = Window.partitionBy($"dnb_num").orderBy(col("ts_last_update").desc)

    val dedup_on_reg_name_not_blank = le_dnb_reg_name_not_blank.withColumn("maxTS", first("ts_last_update").over(windowSpec)).select("*").filter($"maxTS" === $"ts_last_update").drop("maxTS").dropDuplicates(Array("dnb_num"))
    //-------------------join le_dnb and lookup to find company_status---------------------------------------------------------------

    val jn_odm_crs_lukup_reg_name_not_blank = dedup_on_reg_name_not_blank.join(odm_crs_load_dnb_crs_company_status_lookup, dedup_on_reg_name_not_blank("company_status") === odm_crs_load_dnb_crs_company_status_lookup("status_key"), "Inner")

    val validate_dnb_records_flow1 = jn_odm_crs_lukup_reg_name_not_blank.select(when(trim(dedup_on_reg_name_not_blank("dnb_num")) !== "", concat($"dnb_num", lit("D"))).otherwise(trim(dedup_on_reg_name_not_blank("dnb_num"))).alias("dnb_num"), when((trim(dedup_on_reg_name_not_blank("hq_dnb_num")) !== "") && (_lengthUDF(trim(regexp_replace(dedup_on_reg_name_not_blank("hq_dnb_num"), "0", ""))) !== 0), concat($"hq_dnb_num", lit("D"))).otherwise("").alias("hq_dnb_num"), when(trim(dedup_on_reg_name_not_blank("ult_parent_dnb")) !== "", concat($"ult_parent_dnb", lit("D"))).otherwise(trim(dedup_on_reg_name_not_blank("ult_parent_dnb"))).alias("ult_parent_dnb"), when(trim(dedup_on_reg_name_not_blank("company_status")) !== "", regexp_replace(dedup_on_reg_name_not_blank("company_status"), "[A-Z]|[a-z]", "")).otherwise(trim(dedup_on_reg_name_not_blank("company_status"))).alias("company_status"), when(_lengthUDF(trim(dedup_on_reg_name_not_blank("reg_name"))) !== 0, trim(dedup_on_reg_name_not_blank("reg_name"))).otherwise(dedup_on_reg_name_not_blank("reg_name")).alias("reg_name"), trim(dedup_on_reg_name_not_blank("reg_country_code")).alias("reg_country_code"), trim(dedup_on_reg_name_not_blank("trading_status")).alias("trading_status"), trim(dedup_on_reg_name_not_blank("tel_no")).alias("tel_no"), trim(dedup_on_reg_name_not_blank("reg_street_num")).alias("reg_street_num"), trim(dedup_on_reg_name_not_blank("reg_street_name")).alias("reg_street_name"), trim(dedup_on_reg_name_not_blank("reg_sub_premises")).alias("reg_sub_premises"), trim(dedup_on_reg_name_not_blank("reg_premises_name")).alias("reg_premises_name"), trim(dedup_on_reg_name_not_blank("reg_locality")).alias("reg_locality"), trim(dedup_on_reg_name_not_blank("reg_town")).alias("reg_town"), trim(dedup_on_reg_name_not_blank("reg_county")).alias("reg_county"), trim(dedup_on_reg_name_not_blank("bus_start_year")).alias("bus_start_year"), trim(dedup_on_reg_name_not_blank("cro_prefix")).alias("cro_prefix"), trim(dedup_on_reg_name_not_blank("reg_postcode")).alias("reg_postcode"), when((_lengthUDF(trim(regexp_replace(dedup_on_reg_name_not_blank("cro_number"), "0", ""))) === 0), "").otherwise(trim(dedup_on_reg_name_not_blank("cro_number"))).alias("cro_number"), trim(dedup_on_reg_name_not_blank("ta_name1")).alias("ta_name1"), trim(dedup_on_reg_name_not_blank("ta_name2")).alias("ta_name2"), trim(dedup_on_reg_name_not_blank("ta_name3")).alias("ta_name3"), trim(dedup_on_reg_name_not_blank("ta_name4")).alias("ta_name4"), trim(dedup_on_reg_name_not_blank("company_type")).alias("company_type"), trim(dedup_on_reg_name_not_blank("ta_street_num")).alias("ta_street_num"), trim(dedup_on_reg_name_not_blank("ta_street_name")).alias("ta_street_name"), trim(dedup_on_reg_name_not_blank("ta_sub_premises")).alias("ta_sub_premises"), trim(dedup_on_reg_name_not_blank("ta_premises_name")).alias("ta_premises_name"), trim(dedup_on_reg_name_not_blank("ta_locality")).alias("ta_locality"), trim(dedup_on_reg_name_not_blank("ta_town")).alias("ta_town"), trim(dedup_on_reg_name_not_blank("ta_county")).alias("ta_county"), trim(dedup_on_reg_name_not_blank("ta_postcode")).alias("ta_postcode"), trim(dedup_on_reg_name_not_blank("ta_country_code")).alias("ta_country_code"), trim(dedup_on_reg_name_not_blank("company_form_date")).alias("company_form_date"), trim(dedup_on_reg_name_not_blank("alt_name")).alias("alt_name"), when(trim(dedup_on_reg_name_not_blank("parent_dnb")) !== "", concat($"parent_dnb", lit("D"))).otherwise(trim(dedup_on_reg_name_not_blank("parent_dnb"))).alias("parent_dnb"), trim(dedup_on_reg_name_not_blank("ts_last_update")).alias("ts_last_update"))

    //--------------------check hq_dnb_num blank or not--------------------------------

    val chk_hq_dnb_num_blank = validate_dnb_records_flow1.filter($"hq_dnb_num" === "")

    val chk_hq_dnb_num_not_blank = validate_dnb_records_flow1.filter($"hq_dnb_num" !== "")

    //------flow2-----out.dnb_num :: string_concat(in.dnb_num, "D");out.* :: in.*;---------------------------------------------------------------

    val validate_dnb_records_flow2 = dedup_on_reg_name_not_blank.select(when(trim(dedup_on_reg_name_not_blank("dnb_num")) !== "", concat($"dnb_num", lit("D"))).otherwise(trim(dedup_on_reg_name_not_blank("dnb_num"))).alias("dnb_num"), trim(dedup_on_reg_name_not_blank("hq_dnb_num")).alias("hq_dnb_num"), trim(dedup_on_reg_name_not_blank("ult_parent_dnb")).alias("ult_parent_dnb"), trim(dedup_on_reg_name_not_blank("company_status")).alias("company_status"), trim(dedup_on_reg_name_not_blank("reg_name")).alias("reg_name"), trim(dedup_on_reg_name_not_blank("reg_country_code")).alias("reg_country_code"), trim(dedup_on_reg_name_not_blank("trading_status")).alias("trading_status"), trim(dedup_on_reg_name_not_blank("tel_no")).alias("tel_no"), trim(dedup_on_reg_name_not_blank("reg_street_num")).alias("reg_street_num"), trim(dedup_on_reg_name_not_blank("reg_street_name")).alias("reg_street_name"), trim(dedup_on_reg_name_not_blank("reg_sub_premises")).alias("reg_sub_premises"), trim(dedup_on_reg_name_not_blank("reg_premises_name")).alias("reg_premises_name"), trim(dedup_on_reg_name_not_blank("reg_locality")).alias("reg_locality"), trim(dedup_on_reg_name_not_blank("reg_town")).alias("reg_town"), trim(dedup_on_reg_name_not_blank("reg_county")).alias("reg_county"), trim(dedup_on_reg_name_not_blank("bus_start_year")).alias("bus_start_year"), trim(dedup_on_reg_name_not_blank("cro_prefix")).alias("cro_prefix"), trim(dedup_on_reg_name_not_blank("reg_postcode")).alias("reg_postcode"), trim(dedup_on_reg_name_not_blank("cro_number")).alias("cro_number"), trim(dedup_on_reg_name_not_blank("ta_name1")).alias("ta_name1"), trim(dedup_on_reg_name_not_blank("ta_name2")).alias("ta_name2"), trim(dedup_on_reg_name_not_blank("ta_name3")).alias("ta_name3"), trim(dedup_on_reg_name_not_blank("ta_name4")).alias("ta_name4"), trim(dedup_on_reg_name_not_blank("company_type")).alias("company_type"), trim(dedup_on_reg_name_not_blank("ta_street_num")).alias("ta_street_num"), trim(dedup_on_reg_name_not_blank("ta_street_name")).alias("ta_street_name"), trim(dedup_on_reg_name_not_blank("ta_sub_premises")).alias("ta_sub_premises"), trim(dedup_on_reg_name_not_blank("ta_premises_name")).alias("ta_premises_name"), trim(dedup_on_reg_name_not_blank("ta_locality")).alias("ta_locality"), trim(dedup_on_reg_name_not_blank("ta_town")).alias("ta_town"), trim(dedup_on_reg_name_not_blank("ta_county")).alias("ta_county"), trim(dedup_on_reg_name_not_blank("ta_postcode")).alias("ta_postcode"), trim(dedup_on_reg_name_not_blank("ta_country_code")).alias("ta_country_code"), trim(dedup_on_reg_name_not_blank("company_form_date")).alias("company_form_date"), trim(dedup_on_reg_name_not_blank("alt_name")).alias("alt_name"), trim(dedup_on_reg_name_not_blank("parent_dnb")).alias("parent_dnb"), trim(dedup_on_reg_name_not_blank("ts_last_update")).alias("ts_last_update"))

    //---------left outer join deselect record from flow1(check hq_dnb_num blank or not) and flow2 on hq_dnb_num and dnb_num(Strat) ------------------------- 
    //-------------match record--------------------------------------------------------------------------------------------------- 

    val match_jn_left_hq_dnb_num_not_blank_flow2 = chk_hq_dnb_num_not_blank.join(validate_dnb_records_flow2, chk_hq_dnb_num_not_blank("hq_dnb_num") === validate_dnb_records_flow2("dnb_num"), "leftouter").filter((validate_dnb_records_flow2("dnb_num") !== "") || (validate_dnb_records_flow2("dnb_num") !== null) || (validate_dnb_records_flow2("dnb_num").isNotNull)).drop(validate_dnb_records_flow2("dnb_num")).drop(validate_dnb_records_flow2("hq_dnb_num")).drop(validate_dnb_records_flow2("ult_parent_dnb")).drop(validate_dnb_records_flow2("company_status")).drop(validate_dnb_records_flow2("reg_name")).drop(validate_dnb_records_flow2("reg_country_code")).drop(validate_dnb_records_flow2("trading_status")).drop(validate_dnb_records_flow2("tel_no")).drop(validate_dnb_records_flow2("reg_street_num")).drop(validate_dnb_records_flow2("reg_street_name")).drop(validate_dnb_records_flow2("reg_sub_premises")).drop(validate_dnb_records_flow2("reg_premises_name")).drop(validate_dnb_records_flow2("reg_locality")).drop(validate_dnb_records_flow2("reg_town")).drop(validate_dnb_records_flow2("reg_county")).drop(validate_dnb_records_flow2("bus_start_year")).drop(validate_dnb_records_flow2("cro_prefix")).drop(validate_dnb_records_flow2("reg_postcode")).drop(validate_dnb_records_flow2("cro_number")).drop(validate_dnb_records_flow2("ta_name1")).drop(validate_dnb_records_flow2("ta_name2")).drop(validate_dnb_records_flow2("ta_name3")).drop(validate_dnb_records_flow2("ta_name4")).drop(validate_dnb_records_flow2("company_type")).drop(validate_dnb_records_flow2("ta_street_num")).drop(validate_dnb_records_flow2("ta_street_name")).drop(validate_dnb_records_flow2("ta_sub_premises")).drop(validate_dnb_records_flow2("ta_premises_name")).drop(validate_dnb_records_flow2("ta_locality")).drop(validate_dnb_records_flow2("ta_town")).drop(validate_dnb_records_flow2("ta_county")).drop(validate_dnb_records_flow2("ta_postcode")).drop(validate_dnb_records_flow2("ta_country_code")).drop(validate_dnb_records_flow2("company_form_date")).drop(validate_dnb_records_flow2("alt_name")).drop(validate_dnb_records_flow2("parent_dnb")).drop(validate_dnb_records_flow2("ts_last_update")).persist()

    //---------------------unmatch record-------------------------------------------------------------------------------------------------------------
    val unmatch_jn_left_hq_dnb_num_not_blank_flow2 = chk_hq_dnb_num_not_blank.join(validate_dnb_records_flow2, chk_hq_dnb_num_not_blank("hq_dnb_num") === validate_dnb_records_flow2("dnb_num"), "leftouter").filter((validate_dnb_records_flow2("dnb_num") === "") || (validate_dnb_records_flow2("dnb_num") === null) || (validate_dnb_records_flow2("dnb_num").isNull)).drop(validate_dnb_records_flow2("dnb_num")).drop(validate_dnb_records_flow2("hq_dnb_num")).drop(validate_dnb_records_flow2("ult_parent_dnb")).drop(validate_dnb_records_flow2("company_status")).drop(validate_dnb_records_flow2("reg_name")).drop(validate_dnb_records_flow2("reg_country_code")).drop(validate_dnb_records_flow2("trading_status")).drop(validate_dnb_records_flow2("tel_no")).drop(validate_dnb_records_flow2("reg_street_num")).drop(validate_dnb_records_flow2("reg_street_name")).drop(validate_dnb_records_flow2("reg_sub_premises")).drop(validate_dnb_records_flow2("reg_premises_name")).drop(validate_dnb_records_flow2("reg_locality")).drop(validate_dnb_records_flow2("reg_town")).drop(validate_dnb_records_flow2("reg_county")).drop(validate_dnb_records_flow2("bus_start_year")).drop(validate_dnb_records_flow2("cro_prefix")).drop(validate_dnb_records_flow2("reg_postcode")).drop(validate_dnb_records_flow2("cro_number")).drop(validate_dnb_records_flow2("ta_name1")).drop(validate_dnb_records_flow2("ta_name2")).drop(validate_dnb_records_flow2("ta_name3")).drop(validate_dnb_records_flow2("ta_name4")).drop(validate_dnb_records_flow2("company_type")).drop(validate_dnb_records_flow2("ta_street_num")).drop(validate_dnb_records_flow2("ta_street_name")).drop(validate_dnb_records_flow2("ta_sub_premises")).drop(validate_dnb_records_flow2("ta_premises_name")).drop(validate_dnb_records_flow2("ta_locality")).drop(validate_dnb_records_flow2("ta_town")).drop(validate_dnb_records_flow2("ta_county")).drop(validate_dnb_records_flow2("ta_postcode")).drop(validate_dnb_records_flow2("ta_country_code")).drop(validate_dnb_records_flow2("company_form_date")).drop(validate_dnb_records_flow2("alt_name")).drop(validate_dnb_records_flow2("parent_dnb")).drop(validate_dnb_records_flow2("ts_last_update")).persist()
    //-----------------left outer join deselect record from flow1(check hq_dnb_num blank or not) and flow2 on hq_dnb_num and dnb_num (END)---------------------------
    //---------------select output from flow1(seleced record) and matching records from previous transformation is merged ------------------------------

    val hq_dnb_num_blank_match_jn_left_hq_dnb_num = match_jn_left_hq_dnb_num_not_blank_flow2.unionAll(chk_hq_dnb_num_blank).withColumn("rejected_branch_record", lit("N")).select($"rejected_branch_record", $"dnb_num", $"hq_dnb_num", $"ult_parent_dnb", $"company_status", $"reg_name", $"reg_country_code", replacefirstoccur_udf(col("trading_status"), lit("0")).alias("trading_status"), $"tel_no", $"reg_street_num", $"reg_street_name", $"reg_sub_premises", $"reg_premises_name", $"reg_locality", $"reg_town", $"reg_county", $"bus_start_year", $"cro_prefix", $"reg_postcode", $"cro_number", $"ta_name1", $"ta_name2", $"ta_name3", $"ta_name4", $"company_type", $"ta_street_num", $"ta_street_name", $"ta_sub_premises", $"ta_premises_name", $"ta_locality", $"ta_town", $"ta_county", $"ta_postcode", $"ta_country_code", $"company_form_date", $"alt_name", $"parent_dnb")

    //--------use the unmatch record (161 line(left outer join deselect record from flow1(check hq_dnb_num blank or not) and flow2 on hq_dnb_num and dnb_num)) and add following-----------------

    val refrmt_unmatch_jn_lft_hq_dnb_num_nt_blnk_flw2 = unmatch_jn_left_hq_dnb_num_not_blank_flow2.withColumn("rejected_branch_record", lit("Y")).select($"rejected_branch_record", $"dnb_num", $"hq_dnb_num", $"ult_parent_dnb", $"company_status", $"reg_name", $"reg_country_code", replacefirstoccur_udf(col("trading_status"), lit("0")).alias("trading_status"), $"tel_no", $"reg_street_num", $"reg_street_name", $"reg_sub_premises", $"reg_premises_name", $"reg_locality", $"reg_town", $"reg_county", $"bus_start_year", $"cro_prefix", $"reg_postcode", $"cro_number", $"ta_name1", $"ta_name2", $"ta_name3", $"ta_name4", $"company_type", $"ta_street_num", $"ta_street_name", $"ta_sub_premises", $"ta_premises_name", $"ta_locality", $"ta_town", $"ta_county", $"ta_postcode", $"ta_country_code", $"company_form_date", $"alt_name", $"parent_dnb")

    //----------------merge 163 and 171----(select output from flow1(seleced record) and matching records from previous transformation is merged) and (use the unmatch record use the unmatch record (161 line(left outer join deselect record from flow1(check hq_dnb_num blank or not) and flow2 on hq_dnb_num and dnb_num))-------------- 

    val un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft = hq_dnb_num_blank_match_jn_left_hq_dnb_num.unionAll(refrmt_unmatch_jn_lft_hq_dnb_num_nt_blnk_flw2)
    //-------------------------------get CRS DATA-----------------------------------------------------------------------------------------------------------------
    //-------------------------------join dq_data_le_dnb and cir hub (from above) on cir_key----------------------------------------------------------------------
    val jn_dq_data_le_dnb_cir_hub = dq_data_le_dnb.join(fil_status_cir_src_cdmc_cir_hub, dq_data_le_dnb("cir_key") === fil_status_cir_src_cdmc_cir_hub("cir_key"), "Inner").select(trim(dq_data_le_dnb("cir_key")).alias("cir_key"), trim(fil_status_cir_src_cdmc_cir_hub("status")).alias("status"), trim(fil_status_cir_src_cdmc_cir_hub("src_key1")).alias("dnb_num"), trim(fil_status_cir_src_cdmc_cir_hub("src_key4")).alias("hq_dnb_num"), trim(fil_status_cir_src_cdmc_cir_hub("src_key2")).alias("company_status"), trim(dq_data_le_dnb("reg_name")).alias("reg_name"), trim(dq_data_le_dnb("reg_country_code")).alias("reg_country_code"), trim(replacefirstoccur_udf(dq_data_le_dnb("trading_status"), lit("0"))).alias("trading_status"), trim(dq_data_le_dnb("tel_no")).alias("tel_no"), trim(dq_data_le_dnb("reg_street_num")).alias("reg_street_num"), trim(dq_data_le_dnb("reg_street_name")).alias("reg_street_name"), trim(dq_data_le_dnb("reg_sub_premises")).alias("reg_sub_premises"), trim(dq_data_le_dnb("reg_premises_name")).alias("reg_premises_name"), trim(dq_data_le_dnb("reg_locality")).alias("reg_locality"), trim(dq_data_le_dnb("reg_town")).alias("reg_town"), trim(dq_data_le_dnb("reg_county")).alias("reg_county"), trim(replacefirstoccur_udf(fil_status_cir_src_cdmc_cir_hub("src_key5"), fil_status_cir_src_cdmc_cir_hub("src_key3"))).alias("cro_prefix"), trim(dq_data_le_dnb("reg_postcode")).alias("reg_postcode"), trim(fil_status_cir_src_cdmc_cir_hub("src_key3")).alias("cro_number"), trim(dq_data_le_dnb("ta_name1")).alias("ta_name1"), trim(dq_data_le_dnb("ta_street_num")).alias("ta_street_num"), trim(dq_data_le_dnb("ta_street_name")).alias("ta_street_name"), trim(dq_data_le_dnb("ta_sub_premises")).alias("ta_sub_premises"), trim(dq_data_le_dnb("ta_premises_name")).alias("ta_premises_name"), trim(dq_data_le_dnb("ta_locality")).alias("ta_locality"), trim(dq_data_le_dnb("ta_town")).alias("ta_town"), trim(dq_data_le_dnb("ta_county")).alias("ta_county"), trim(dq_data_le_dnb("ta_postcode")).alias("ta_postcode"), trim(dq_data_le_dnb("ta_country_code")).alias("ta_country_code"), trim(dq_data_le_dnb("ts_last_update")).alias("ts_last_update"), lit("").alias("data_owner"))

    //-----------------------diff process:-----------------START-----------------------------------------------------------------------------------
    //-----------------XXXXXXXXXXXXXXXXXXXXXXXXXXXX----Join on Selected Keys----------------START--------------XXXXXXXXXXXXXXXXXXXXX----------------------------
    val ful_outr_get_crs_an_jn_le_dnb_cir_hub = un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft.join(jn_dq_data_le_dnb_cir_hub,
      (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("dnb_num") === jn_dq_data_le_dnb_cir_hub("dnb_num"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("hq_dnb_num") === jn_dq_data_le_dnb_cir_hub("hq_dnb_num"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("company_status") === jn_dq_data_le_dnb_cir_hub("company_status"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("reg_name") === jn_dq_data_le_dnb_cir_hub("reg_name"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("reg_country_code") === jn_dq_data_le_dnb_cir_hub("reg_country_code"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("trading_status") === jn_dq_data_le_dnb_cir_hub("trading_status"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("tel_no") === jn_dq_data_le_dnb_cir_hub("tel_no"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("reg_street_num") === jn_dq_data_le_dnb_cir_hub("reg_street_num"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("reg_street_name") === jn_dq_data_le_dnb_cir_hub("reg_street_name"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("reg_sub_premises") === jn_dq_data_le_dnb_cir_hub("reg_sub_premises"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("reg_premises_name") === jn_dq_data_le_dnb_cir_hub("reg_premises_name"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("reg_locality") === jn_dq_data_le_dnb_cir_hub("reg_locality"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("reg_town") === jn_dq_data_le_dnb_cir_hub("reg_town"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("reg_county") === jn_dq_data_le_dnb_cir_hub("reg_county"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("cro_prefix") === jn_dq_data_le_dnb_cir_hub("cro_prefix"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("reg_postcode") === jn_dq_data_le_dnb_cir_hub("reg_postcode"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("cro_number") === jn_dq_data_le_dnb_cir_hub("cro_number"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("ta_name1") === jn_dq_data_le_dnb_cir_hub("ta_name1"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("ta_street_num") === jn_dq_data_le_dnb_cir_hub("ta_street_num"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("ta_street_name") === jn_dq_data_le_dnb_cir_hub("ta_street_name"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("ta_sub_premises") === jn_dq_data_le_dnb_cir_hub("ta_sub_premises"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("ta_premises_name") === jn_dq_data_le_dnb_cir_hub("ta_premises_name"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("ta_locality") === jn_dq_data_le_dnb_cir_hub("ta_locality"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("ta_town") === jn_dq_data_le_dnb_cir_hub("ta_town"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("ta_county") === jn_dq_data_le_dnb_cir_hub("ta_county"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("ta_postcode") === jn_dq_data_le_dnb_cir_hub("ta_postcode"))
        && (un_refrmt_hq_dnb_num_blnk_mach_unmach_jn_lft("ta_country_code") === jn_dq_data_le_dnb_cir_hub("ta_country_code")), "fullouter")

        ful_outr_get_crs_an_jn_le_dnb_cir_hub.rdd.map(x=>x.mkString("\u0001"))saveAsTextFile("/user/HAASAAP0256_05038/le_dnb_crs_test/final_result.dat")
    }
     catch {
        case e: Exception => {
          e.printStackTrace();
          Logger.getLogger(getClass).error("Cannot compute results due to error : " + e.toString + " with stack-trace : " + e)
          System.exit(1)
        }
      }
  }

}