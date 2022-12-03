package jstut001

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import scala.math.Ordered.orderingToOrdered

object App {
  val workingDir = "data/raw/"
  //Download Data Link https://files.usaspending.gov/generated_downloads/PrimeAwardSummariesAndSubawards_2022-12-02_H03M31S08942267.zip
  def main(args : Array[String]) {
    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")
    val spark = SparkSession.builder().appName("CS226_Final_Project").config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val ContractSub = spark.read.format("csv").option("header", "true").option("inferSchema" , "true").load(workingDir + "csa.csv")
    val ContractSubFiltered = ContractSub
      .select("prime_award_unique_key","prime_award_amount","prime_award_base_action_date",
        "prime_award_awarding_agency_code","prime_award_awarding_agency_name","prime_awardee_uei","prime_awardee_name",
        "prime_awardee_country_name","prime_awardee_address_line_1","prime_awardee_city_name","prime_awardee_state_name")
      .withColumnRenamed("prime_award_unique_key", "KEY")
      .withColumnRenamed("prime_award_amount", "AMOUNT")
      .withColumnRenamed("prime_award_base_action_date", "DATE")
      .withColumnRenamed("prime_award_awarding_agency_code", "A_ID")
      .withColumnRenamed("prime_award_awarding_agency_name", "A_NAME")
      .withColumnRenamed("prime_awardee_uei", "R_ID")
      .withColumnRenamed("prime_awardee_name", "R_NAME")
      .withColumnRenamed("prime_awardee_country_name", "R_COUNTRY")
      .withColumnRenamed("prime_awardee_address_line_1", "R_ADDR")
      .withColumnRenamed("prime_awardee_city_name", "R_CITY")
      .withColumnRenamed("prime_awardee_state_name", "R_STATE")
      .filter(row => !row.anyNull)
      .dropDuplicates()
    val AssistantPrime = spark.read.format("csv").option("header", "true").option("inferSchema" , "true").load(workingDir + "apas.csv")
    val AssistantPrimeFiltered = AssistantPrime
    .select("assistance_award_unique_key","total_obligated_amount","award_base_action_date","awarding_agency_code",
        "awarding_agency_name","recipient_uei","recipient_name",
        "recipient_country_name","recipient_address_line_1","recipient_city_name","recipient_state_name")
      .withColumnRenamed("assistance_award_unique_key", "KEY")
      .withColumnRenamed("total_obligated_amount", "AMOUNT")
      .withColumnRenamed("award_base_action_date", "DATE")
      .withColumnRenamed("awarding_agency_code", "A_ID")
      .withColumnRenamed("awarding_agency_name", "A_NAME")
      .withColumnRenamed("recipient_uei", "R_ID")
      .withColumnRenamed("recipient_name", "R_NAME")
      .withColumnRenamed("recipient_country_name", "R_COUNTRY")
      .withColumnRenamed("recipient_address_line_1", "R_ADDR")
      .withColumnRenamed("recipient_city_name", "R_CITY")
      .withColumnRenamed("recipient_state_name", "R_STATE")
      .filter(row => !row.anyNull)
      .dropDuplicates()
    val AssistantSub = spark.read.format("csv").option("header", "true").option("inferSchema" , "true").load(workingDir + "asa.csv")
    val AssistantSubFiltered = AssistantSub
      .select("prime_award_unique_key","prime_award_amount","prime_award_base_action_date","prime_award_awarding_agency_code",
        "prime_award_awarding_agency_name","prime_awardee_uei","prime_awardee_name",
        "prime_awardee_country_name","prime_awardee_address_line_1","prime_awardee_city_name","prime_awardee_state_name")
      .withColumnRenamed("prime_award_unique_key", "KEY")
      .withColumnRenamed("prime_award_amount", "AMOUNT")
      .withColumnRenamed("prime_award_base_action_date", "DATE")
      .withColumnRenamed("prime_award_awarding_agency_code", "A_ID")
      .withColumnRenamed("prime_award_awarding_agency_name", "A_NAME")
      .withColumnRenamed("prime_awardee_uei", "R_ID")
      .withColumnRenamed("prime_awardee_name", "R_NAME")
      .withColumnRenamed("prime_awardee_country_name", "R_COUNTRY")
      .withColumnRenamed("prime_awardee_address_line_1", "R_ADDR")
      .withColumnRenamed("prime_awardee_city_name", "R_CITY")
      .withColumnRenamed("prime_awardee_state_name", "R_STATE")
      .filter(row => !row.anyNull)
      .dropDuplicates()
    val ContractPrime = spark.read.format("csv").option("header", "true").option("inferSchema" , "true").load(workingDir + "cpas.csv")
    val ContractPrimeFiltered = ContractPrime
      .select("contract_award_unique_key","total_obligated_amount","award_base_action_date",
        "awarding_agency_code","awarding_agency_name","recipient_uei","recipient_name",
        "recipient_country_name","recipient_address_line_1","recipient_city_name","recipient_state_name")
      .withColumnRenamed("contract_award_unique_key", "KEY")
      .withColumnRenamed("total_obligated_amount", "AMOUNT")
      .withColumnRenamed("award_base_action_date", "DATE")
      .withColumnRenamed("awarding_agency_code","A_ID")
      .withColumnRenamed("awarding_agency_name", "A_NAME")
      .withColumnRenamed("recipient_uei", "R_ID")
      .withColumnRenamed("recipient_name", "R_NAME")
      .withColumnRenamed("recipient_country_name", "R_COUNTRY")
      .withColumnRenamed("recipient_address_line_1", "R_ADDR")
      .withColumnRenamed("recipient_city_name", "R_CITY")
      .withColumnRenamed("recipient_state_name", "R_STATE")
      .filter(row => !row.anyNull)
      .dropDuplicates()


    // Data has been loaded and reduced to only the relevant columns.  End columns are: KEY, PIID, $$, DATE, AGENCY ID, AGENCY NAME, RECIPIENT ID, RECIPIENT NAME, RECIPIENT ZIP
    // IF transactions have same ID, if the amounts differ then add them.

    var data = ContractSubFiltered.union(AssistantSubFiltered.union(ContractPrimeFiltered.union(AssistantPrimeFiltered))).dropDuplicates()
    data = data.groupBy("KEY", "DATE", "A_ID", "A_NAME", "R_ID", "R_NAME", "R_COUNTRY", "R_ADDR", "R_CITY", "R_STATE")
      .agg(sum(data.col("AMOUNT"))).withColumnRenamed("sum(AMOUNT)", "AMOUNT").dropDuplicates().sort("DATE", "KEY")
    data.show()
    
    data.repartition(1).write.mode(SaveMode.Overwrite).csv("Spending.csv")
  }
}