package jstut001

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SpendingCorrelations {
  val workingDir = "data/raw/"
  def main(args : Array[String]): Unit = {
    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")
    val spark = SparkSession.builder().appName("CS226_Final_Project").config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val schema = StructType(Array(
      StructField("KEY", StringType, nullable = false),
      StructField("DATE", DateType, nullable = false),
      StructField("A_ID", IntegerType, nullable = false),
      StructField("A_NAME", StringType, nullable = false),
      StructField("R_ID", StringType, nullable = false),
      StructField("R_NAME", StringType, nullable = false),
      StructField("R_COUNTRY", StringType, nullable = false),
      StructField("R_ADDR", StringType, nullable = false),
      StructField("R_CITY", StringType, nullable = false),
      StructField("R_STATE", StringType, nullable = false),
      StructField("R_COUNTY", StringType, nullable = false),
      StructField("AMOUNT", DoubleType, nullable = false)
    ))
    var Transactions = spark.read.format("csv").option("header", "true").schema(schema).load("Transaction.csv")
    Transactions.printSchema()
    Transactions = Transactions
      .select("DATE", "R_NAME", "AMOUNT")

    val Companies = Transactions.select("R_NAME").dropDuplicates()
//    Companies.show()
    //For each company, is another company often funded at the same time?
    //Normalize amounts - find max for each company then divide by that amount.
    val CompanyMax = Transactions.select("R_NAME", "AMOUNT").groupBy("R_NAME").max("AMOUNT")

  }
}
