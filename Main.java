package org.example; // Enveloped package

//Apache Spark Libraries
import org.apache.spark.sql.*;

//Java Includes
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class Main {
    String path;

    public static void getTotalAmountAwardedByGroup(SparkSession sparkSession) throws IOException {
        Dataset<Row> df = sparkSession.sql("SELECT recipient_name AS recipient, "
                + "SUM(total_dollars_obligated) AS total "
                + "FROM USA GROUP BY recipient_name");

        FileWriter file = new FileWriter("target/json/TotalAmountAwardedByGroup.json");
        file.write(df.toJSON().collectAsList().toString());
        file.close();
    }

    public static void getNumOfAwardsPerEntity(SparkSession sparkSession) throws IOException {
        Dataset<Row> df = sparkSession.sql("SELECT COUNT(*) AS num_of_awards "
                + "FROM USA GROUP BY recipient_name");

        FileWriter file = new FileWriter("target/json/NumOfAwardsPerEntity.json");
        file.write(df.toJSON().collectAsList().toString());
        file.close();
    }

    public static void getTotalAwardAmountByDateRange(SparkSession sparkSession) throws IOException {

        // Keyboard reader
        Scanner input = new Scanner(System.in);

        // Prompt user for date range
        System.out.print("Enter a starting date (YYYY-MM-DD): ");
        String startDate = input.nextLine();
        System.out.print("Enter an ending date (YYYY-MM-DD): ");
        String endDate = input.nextLine();

        // Query
        Dataset<Row> df = sparkSession.sql("SELECT SUM(total_dollars_obligated) AS total, period_of_performance_start_date"
                + " FROM USA WHERE '" + startDate + "' <= period_of_performance_start_date AND period_of_performance_current_end_date <= '" + endDate
                + "' GROUP BY period_of_performance_start_date"
                + " ORDER BY total DESC;");

        FileWriter file = new FileWriter("target/json/TotalAwardAmountByDateRange.json");
        file.write(df.toJSON().collectAsList().toString());
        file.close();
    }

    public static void getAwardTotalByQuarterOfTheYear(SparkSession sparkSession) throws IOException {

        // Prime the conditional query
        int quarter = getQuarter();

        /* First Quarter => Jan 1 - Mar 31 */
        if (quarter == 1) {
            Dataset<Row> df = quarterOne(sparkSession);

            // January
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-01-01","2022-01-31"));

            FileWriter file = new FileWriter("target/json/AwardTotalByQuarterOfTheYear_Q1Jan.json");
            file.write(df.toJSON().collectAsList().toString());
            file.close();

            // February
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-02-01","2022-02-28"));

            FileWriter file2 = new FileWriter("target/json/AwardTotalByQuarterOfTheYear_Q1Feb.json");
            file.write(df.toJSON().collectAsList().toString());
            file2.close();

            // March
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-03-01","2022-03-31"));

            FileWriter file3 = new FileWriter("target/json/AwardTotalByQuarterOfTheYear_Q1March.json");
            file.write(df.toJSON().collectAsList().toString());
            file3.close();

            // ----------------------------------------------------------------------------------
            /* Second Quarter => Apr 1 - June 30 */
        } else if (quarter == 2) {
            Dataset<Row> df = quarterTwo(sparkSession);

            // April
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-04-01", "2022-04-30"));

            FileWriter file = new FileWriter("target/json/AwardTotalByQuarterOfTheYear_Q2April.json");
            file.write(df.toJSON().collectAsList().toString());
            file.close();

            // May
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-05-01", "2022-05-31"));

            FileWriter file2 = new FileWriter("target/json/AwardTotalByQuarterOfTheYear_Q2May.json");
            file.write(df.toJSON().collectAsList().toString());
            file2.close();

            // June
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-06-01", "2022-06-30"));

            FileWriter file3 = new FileWriter("target/json/AwardTotalByQuarterOfTheYear_Q2June.json");
            file.write(df.toJSON().collectAsList().toString());
            file3.close();
            // ----------------------------------------------------------------------------------
            /* Third Quarter => Jul 1 - Sep 31 */
        } else if (quarter == 3) {
            Dataset<Row> df = quarterThree(sparkSession);

            // July
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-07-01", "2022-07-31"));

            FileWriter file = new FileWriter("target/json/AwardTotalByQuarterOfTheYear_Q3July.json");
            file.write(df.toJSON().collectAsList().toString());
            file.close();

            // August
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-08-01", "2022-08-31"));

            FileWriter file2 = new FileWriter("target/json/AwardTotalByQuarterOfTheYear_Q3Aug.json");
            file.write(df.toJSON().collectAsList().toString());
            file2.close();

            // September
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-09-01", "2022-09-30"));

            FileWriter file3 = new FileWriter("target/json/AwardTotalByQuarterOfTheYear_Q3Sept.json");
            file.write(df.toJSON().collectAsList().toString());
            file3.close();

            // ----------------------------------------------------------------------------------
            /* Fourth Quarter => Oct 1 - Dec 31 */
        } else if (quarter == 4) {
            Dataset<Row> df = quarterFour(sparkSession);

            // October
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-10-01", "2022-10-30"));

            FileWriter file = new FileWriter("target/json/AwardTotalByQuarterOfTheYear_Q4Oct.json");
            file.write(df.toJSON().collectAsList().toString());
            file.close();

            // November
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-11-01", "2022-11-30"));

            FileWriter file2 = new FileWriter("target/json/AwardTotalByQuarterOfTheYear_Q4Nov.json");
            file.write(df.toJSON().collectAsList().toString());
            file2.close();

            // December
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-12-01", "2022-12-31"));

            FileWriter file3 = new FileWriter("target/json/AwardTotalByQuarterOfTheYear_Q4Dec.json");
            file.write(df.toJSON().collectAsList().toString());
            file3.close();
        }// End of quarter conditions ---
    }

    public static void topKAwardsByEntity(SparkSession sparkSession) throws IOException {

        // Keyboard reader
        Scanner input = new Scanner(System.in);

        // Date to query
        System.out.print("Enter a date to evaluate (YYYY-MM-DD): ");
        String date = input.nextLine();

        // Query
        Dataset<Row> df = sparkSession.sql("SELECT recipient_name, total_dollars_obligated, action_date FROM USA WHERE '"
                + date + "' = action_date ORDER BY total_dollars_obligated DESC;");

        FileWriter file = new FileWriter("target/json/TopKAwardsByEntity.json");
        file.write(df.toJSON().collectAsList().toString());
        file.close();
    }

    public static void listTotalQuarterlyReportsByAwardAmount(Dataset<Row> df) throws IOException {

        // Query per quarter respectively
        Dataset<Row> dfQ1 = df.select(functions.sum("total_dollars_obligated").as("Total Funds")).withColumn("Quarter", functions.lit(1)),
                dfQ2 = df.select(functions.sum("total_dollars_obligated").as("Total Funds")).withColumn("Quarter", functions.lit(2)),
                dfQ3 = df.select(functions.sum("total_dollars_obligated").as("Total Funds")).withColumn("Quarter", functions.lit(3)),
                dfQ4 = df.select(functions.sum("total_dollars_obligated").as("Total Funds")).withColumn("Quarter", functions.lit(4));

        // Combine all quarters via UNION
        Dataset<Row> allQ = dfQ1.union(dfQ2.union(dfQ3.union(dfQ4))); // TODO: Test
        allQ.orderBy(allQ.col("Total Funds").desc());

        FileWriter file = new FileWriter("target/json/ListTotalQuarterlyReportsByAwardAmount.json");
        file.write(allQ.toJSON().collectAsList().toString());
        file.close();
    }

    public static void listRecentlyAwardedFunds(SparkSession sparkSession) throws IOException {

        // Query
        Dataset<Row> df = sparkSession.sql("SELECT total_dollars_obligated AS Total_Award, "
                + "recipient_name AS Group_Awarded, action_date as Date_Awarded FROM USA " +
                "ORDER BY action_date DESC;");

        FileWriter file = new FileWriter("target/json/ListRecentlyAwardedFunds.json");
        file.write(df.toJSON().collectAsList().toString());
        file.close();
    }

    public static boolean tryFindEntityByName(SparkSession sparkSession, int enType, String usrIn) {
        String enTStr = "";
        String enTInfStr = "";
        // To expand add more cases
        switch (enType){
            case 1:
                enTStr = "awarding_agency_name"; // different than parent_award_agency_name
                enTInfStr = "Awarding_Agency";
                break;
            case 2:
                enTStr = "recipient_name"; // returns the same thing as recipient_parent_name ig? maybe verify with query
                enTInfStr = "Recipient";
                break;
            default:
                System.out.println("\nIncorrect use of function");
                return false;
        }

        String queryBuilder = "SELECT DISTINCT " + enTStr + " AS " + enTInfStr + " FROM USA WHERE "
                + enTStr + " = \'" + usrIn + '\'';
//        System.out.print("Test: ");
//        System.out.println(queryBuilder);

        Dataset<Row> tempRes = sparkSession.sql(queryBuilder);
        if(tempRes.isEmpty()){ // no exact match
            // print close to
            System.out.println("Could not find an exact match. Did you mean any of these?");
            queryBuilder = "SELECT DISTINCT " + enTStr + " AS " + enTInfStr + " FROM USA WHERE "
                    + enTStr + " LIKE \'%" + usrIn + "%\'";
//            queryBuilder = "SELECT " + enTStr + " AS " + enTInfStr + " FROM USA"; // FOR DEBUG
            sparkSession.sql(queryBuilder).show(false);
            return false;
        }
        else{ // not sure if need 2 options but want to be safe
            if(tempRes.count() == 1){ // found exact
                return true;
            }
            else{ // some error
                System.out.println("Some error");
                tempRes.show(false);
                return false;
            }
        }
    }

    public static void awardGiverInfo(String entName, SparkSession sparkSession) throws IOException {
        Dataset<Row> df = sparkSession.sql("SELECT DISTINCT awarding_agency_name AS Agency_Name," +
                " awarding_agency_code AS Agency_Code," +
                " awarding_office_name AS Office_Name" +
                " FROM USA WHERE awarding_agency_name = \'" + entName + "\'");

        FileWriter file = new FileWriter("target/json/AwardGiverInfo.json");
        file.write(df.toJSON().collectAsList().toString());
        file.close();
    }

    public static void awardGiverTotalMoney(String entName, SparkSession sparkSession) throws IOException {
        Dataset<Row> df = sparkSession.sql("SELECT SUM(current_total_value_of_award) AS Total_Money_Awarded " +
                "FROM USA WHERE awarding_agency_name = \'" + entName + "\'");

        FileWriter file = new FileWriter("target/json/AwardGiverTotalMoney.json");
        file.write(df.toJSON().collectAsList().toString());
        file.close();
    }

    public static void awardGiverTransactions(String entName, SparkSession sparkSession) throws IOException {
        Dataset<Row> df = sparkSession.sql("SELECT contract_transaction_unique_key AS Unique_ID," +
                " recipient_name AS Recipient, current_total_value_of_award as Current_Total, " +
                "award_type as Award_Type, action_date AS Action_Date " +
                "FROM USA WHERE awarding_agency_name = \'" + entName + "\'");

        FileWriter file = new FileWriter("target/json/AwardGiverTransactions.json");
        file.write(df.toJSON().collectAsList().toString());
        file.close();
    }

    public static void recipientInfo(String entName, SparkSession sparkSession) throws IOException {
        Dataset<Row> df = sparkSession.sql("SELECT DISTINCT recipient_name, recipient_uei, recipient_duns, recipient_parent_name, recipient_parent_uei" +
                ", recipient_parent_duns FROM USA WHERE recipient_name = \'" + entName + "\'");

        FileWriter file = new FileWriter("target/json/RecipientInfo.json");
        file.write(df.toJSON().collectAsList().toString());
        file.close();
    }

    public static void recipientAKA(String entName, SparkSession sparkSession) throws IOException {
        Dataset<Row> df = sparkSession.sql("SELECT DISTINCT recipient_doing_business_as_name AS Also_Known_As " +
                "FROM USA WHERE recipient_name = \'" + entName + "\'");

        FileWriter file = new FileWriter("target/json/RecipientAKA.json");
        file.write(df.toJSON().collectAsList().toString());
        file.close();
    }

    public static void recipientLoc(String entName, SparkSession sparkSession) throws IOException {
        Dataset<Row> df = sparkSession.sql("SELECT DISTINCT recipient_country_name AS Country, recipient_address_line_1 AS Address1, " +
                "recipient_address_line_2 AS Address2, recipient_city_name AS City, recipient_county_name AS County, " +
                "recipient_state_name AS State, recipient_zip_4_code AS ZIP " +
                "FROM USA WHERE recipient_name = \'" + entName + "\'");

        FileWriter file = new FileWriter("target/json/RecipientAKA.json");
        file.write(df.toJSON().collectAsList().toString());
        file.close();
    }

    public static void recipientPReg(String entName, SparkSession sparkSession) throws IOException {
        Dataset<Row> df = sparkSession.sql("SELECT DISTINCT primary_place_of_performance_country_name AS Country, " +
                "primary_place_of_performance_city_name AS City, " + "primary_place_of_performance_county_name As County, " +
                "primary_place_of_performance_state_name AS State, primary_place_of_performance_zip_4 AS ZIP," +
                "primary_place_of_performance_congressional_district AS Congr_Distr " +
                "FROM USA WHERE recipient_name = \'" + entName + "\'");

        FileWriter file = new FileWriter("target/json/RecipientPReg.json");
        file.write(df.toJSON().collectAsList().toString());
        file.close();
    }

    private static int getQuarter() {
        Scanner input = new Scanner(System.in);
        System.out.print("Enter a quarter of the year you wish to evaluate (1-4): ");
        int quarter = input.nextInt();

        while (quarter < 1 || quarter > 4) {
            System.out.println("Invalid Value");
            System.out.print("Enter a quarter of the year you wish to evaluate: ");
            quarter = input.nextInt();
        }

        return quarter;
    }

    private static Dataset<Row> quarterOne(SparkSession sparkSession) { // TODO: Fix query, no 'date' col
        Dataset<Row> temp = sparkSession.sql("SELECT * FROM USA WHERE '2022-01-01' <= action_date AND '2022-03-31' >= action_date ORDER BY action_date;");
//        temp.show();
        return temp;
    }

    private static Dataset<Row> quarterTwo(SparkSession sparkSession) { // TODO: Fix query, no 'date' col
        Dataset<Row> temp = sparkSession.sql("SELECT * FROM USA WHERE '2022-04-01' <= action_date AND '2022-06-30' >= action_date ORDER BY action_date;");
//        temp.show();
        return temp;
    }

    private static Dataset<Row> quarterThree(SparkSession sparkSession) { // TODO: Fix query, no 'date' col
        Dataset<Row> temp = sparkSession.sql("SELECT * FROM USA WHERE '2022-07-01' <= action_date AND '2022-09-31' >= action_date ORDER BY action_date;");
//        temp.show();
        return temp;
    }

    private static Dataset<Row> quarterFour(SparkSession sparkSession) { // TODO: Fix query, no 'date' col
        Dataset<Row> temp = sparkSession.sql("SELECT * FROM USA WHERE '2022-10-01' <= action_date AND '2022-12-31' >= action_date ORDER BY action_date;");
//        temp.show();
        return temp;
    }

    public static void quarterHelper(SparkSession sparkSession) { // TODO: fix temporarily public
        Dataset<Row> df1 = quarterOne(sparkSession),
                df2 = quarterTwo(sparkSession),
                df3 = quarterThree(sparkSession),
                df4 = quarterFour(sparkSession);

        /* Quarter 1 */
        Dataset<Row> df1Max = df1.select(functions.sum("total_dollars_obligated").cast("BIGINT").as("Quarterly Reports"));

        /* Quarter 2 */
        Dataset<Row> df2Max = df2.select(functions.sum("total_dollars_obligated").cast("BIGINT").as("Quarterly Reports"));

        /* Quarter 3 */
        Dataset<Row> df3Max = df3.select(functions.sum("total_dollars_obligated").cast("BIGINT").as("Quarterly Reports"));

        /* Quarter 4 */
        Dataset<Row> df4Max = df4.select(functions.sum("total_dollars_obligated").cast("BIGINT").as("Quarterly Reports"));

        /* UNION OF 4 QUARTERS => RETURNS MAX */
        Dataset<Row> MAX = df1Max.union(df2Max.union(df3Max.union(df4Max)));
        MAX.orderBy(MAX.col("Quarterly Reports").desc()).show(false);
    }

    public static void main(String[] args) throws IOException {
        //Main main1 = new Main();

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("CSV Test App")
                .master("local[1]")
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
        Dataset<Row> df = sparkSession.read().format("csv").option("header", "true").load("src/main/java/org/example/data/Contracts_PrimeTransactions_2022-11-23_H17M10S30_1.csv");
        df.createOrReplaceTempView("USA");

        getTotalAmountAwardedByGroup(sparkSession);
        getNumOfAwardsPerEntity(sparkSession);
        getTotalAwardAmountByDateRange(sparkSession);
        getAwardTotalByQuarterOfTheYear(sparkSession);
        topKAwardsByEntity(sparkSession);
        listTotalQuarterlyReportsByAwardAmount(df);
        listRecentlyAwardedFunds(sparkSession);
        //awardGiverInfo(String entName, sparkSession);
        //awardGiverTotalMoney(String entName, sparkSession);
        //awardGiverTransactions(String entName, sparkSession);
        //recipientInfo(String entName, sparkSession);
        //recipientAKA(String entName, sparkSession);
        //recipientLoc(String entName, sparkSession);
        //(String entName, sparkSession);
    }

}// End of US Spending Queries !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!