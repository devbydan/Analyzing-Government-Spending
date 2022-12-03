package jstut001

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.math.Ordered.orderingToOrdered

object App {
  val workingDir = "data/stripped/"
  //Download Data Link https://files.usaspending.gov/generated_downloads/PrimeAwardSummariesAndSubawards_2022-12-02_H03M31S08942267.zip
  def main(args : Array[String]) {
    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")
    val spark = SparkSession.builder().appName("CS226_Final_Project").config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    var ContractSub = spark.read.format("csv").option("header", "true").load(workingDir + "csa.csv")
      .select("prime_award_unique_key","prime_award_amount","prime_award_base_action_date",
        "prime_award_awarding_agency_code","prime_award_awarding_agency_name","prime_awardee_uei","prime_awardee_name","prime_awardee_zip_code")
      .withColumnRenamed("prime_award_unique_key", "KEY")
      .withColumnRenamed("prime_award_amount", "AMOUNT")
      .withColumnRenamed("prime_award_base_action_date", "DATE")
      .withColumnRenamed("prime_award_awarding_agency_code", "A_ID")
      .withColumnRenamed("prime_award_awarding_agency_name", "A_NAME")
      .withColumnRenamed("prime_awardee_uei", "R_ID")
      .withColumnRenamed("prime_awardee_name", "R_NAME")
      .withColumnRenamed("prime_awardee_zip_code", "R_ZIP")
      .dropDuplicates()
    var AssistantPrime = spark.read.format("csv").option("header", "true").load(workingDir + "apas.csv")
      .select("assistance_award_unique_key","total_obligated_amount","award_base_action_date","awarding_agency_code",
        "awarding_agency_name","recipient_uei","recipient_name","recipient_zip_code")
      .withColumnRenamed("assistance_award_unique_key", "KEY")
      .withColumnRenamed("total_obligated_amount", "AMOUNT")
      .withColumnRenamed("award_base_action_date", "DATE")
      .withColumnRenamed("awarding_agency_code", "A_ID")
      .withColumnRenamed("awarding_agency_name", "A_NAME")
      .withColumnRenamed("recipient_uei", "R_ID")
      .withColumnRenamed("recipient_name", "R_NAME")
      .withColumnRenamed("recipient_zip_code", "R_ZIP")
      .dropDuplicates()
    var AssistantSub = spark.read.format("csv").option("header", "true").load(workingDir + "asa.csv")
      .select("prime_award_unique_key","prime_award_amount","prime_award_base_action_date","prime_award_awarding_agency_code",
        "prime_award_awarding_agency_name","prime_awardee_uei","prime_awardee_name","prime_awardee_zip_code")
      .withColumnRenamed("prime_award_unique_key", "KEY")
      .withColumnRenamed("prime_award_amount", "AMOUNT")
      .withColumnRenamed("prime_award_base_action_date", "DATE")
      .withColumnRenamed("prime_award_awarding_agency_code", "A_ID")
      .withColumnRenamed("prime_award_awarding_agency_name", "A_NAME")
      .withColumnRenamed("prime_awardee_uei", "R_ID")
      .withColumnRenamed("prime_awardee_name", "R_NAME")
      .withColumnRenamed("prime_awardee_zip_code", "R_ZIP")
      .dropDuplicates()
    var ContractPrime = spark.read.format("csv").option("header", "true").load(workingDir + "cpas.csv")
      .select("contract_award_unique_key","total_obligated_amount","award_base_action_date",
        "awarding_agency_code","awarding_agency_name","recipient_uei","recipient_name","recipient_zip_4_code")
      .withColumnRenamed("contract_award_unique_key", "KEY")
      .withColumnRenamed("total_obligated_amount", "AMOUNT")
      .withColumnRenamed("award_base_action_date", "DATE")
      .withColumnRenamed("awarding_agency_code","A_ID")
      .withColumnRenamed("awarding_agency_name", "A_NAME")
      .withColumnRenamed("recipient_uei", "R_ID")
      .withColumnRenamed("recipient_name", "R_NAME")
      .withColumnRenamed("recipient_zip_4_code", "R_ZIP")
      .dropDuplicates()

    // Data has been loaded and reduced to only the relevant columns.  End columns are: KEY, PIID, $$, DATE, AGENCY ID, AGENCY NAME, RECIPIENT ID, RECIPIENT NAME, RECIPIENT ZIP
    // IF transactions have same ID, if the amounts differ then add them.

    var data = ContractSub.union(AssistantSub.union(ContractPrime.union(AssistantPrime))).dropDuplicates()
    data = data.groupBy("KEY", "DATE", "A_ID", "A_NAME", "R_ID", "R_NAME", "R_ZIP").agg(sum(data.col("AMOUNT"))).withColumnRenamed("sum(AMOUNT)", "AMOUNT").dropDuplicates()
//    data.show()

//    data.groupBy("KEY","A_ID", "A_NAME", "R_ID", "R_NAME", "R_ZIP").count().filter(row => {row.getLong(6) > 1}).show() nothing
//    data.groupBy("KEY","DATE", "R_ID", "R_NAME", "R_ZIP").count().filter(row => {row.getLong(5) > 1}).show() // many rows -> money comes from many agencies.  This is fine to leave as is.
//    data.groupBy("KEY","DATE", "A_ID", "A_NAME", "R_ZIP").count().filter(row => {row.getLong(5) > 1}).show() // 1 row
//    data.groupBy("KEY","DATE", "A_ID", "A_NAME", "R_ID", "R_NAME").count().filter(row => {row.getLong(6) > 1}).show() //2 rows
    data.repartition(1).write.csv("Spending.csv")
  }
}