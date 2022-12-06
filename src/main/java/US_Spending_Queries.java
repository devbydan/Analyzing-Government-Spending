package SparkWorks; // Enveloped package

//Apache Spark Libraries
import org.apache.spark.sql.*;

//Java Includes
import java.io.*;
import java.io.BufferedReader;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class US_Spending_Queries {
    public static Dataset<Row> df; // DataFrame instance

    private static SparkMainApp sparkMenu = new SparkMainApp(); // Used to call helper terminal-menu functions
    private static SparkSession sparkSession; // Spark instance
    private static Scanner input = new Scanner(System.in); // Input reader/scanner

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> US_Spending_Queries(String filePath, SparkSession sparkSession)
     * Purpose  -> Constructor that sets the private data field Dataset<Row>
     *             and SparkSession. Afterwards, create the temporary view of
     *             the csv file that will be looked at.
     * -----------------------------------------------------------------------
     * Receives -> String, SparkSession
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    US_Spending_Queries(String filePath, SparkSession sparkSession)
    {
        this.sparkSession = sparkSession;
        this.df = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(filePath);

        df.createOrReplaceTempView("USA");
    } // ---------------------------------------------------------------------


    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> getTotalAmountAwardedByGroup()
     * Purpose  -> Method which returns the total award each recipient/entity
     *             received.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> int
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    /* /// OPTION 1 /// OPTION 1 /// OPTION 1 /// OPTION 1 /// OPTION 1 /// */
    public static void getTotalAmountAwardedByGroup() throws Exception {
        sparkSession.sql("SELECT recipient_name AS recipient, "
                       + "SUM(total_dollars_obligated) AS total "
                       + "FROM USA GROUP BY recipient_name").show(false);
    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> getTotalAmountAwardedByGroup()
     * Purpose  -> Method which returns the number of award transactions each
     *             entity/recipient has been given
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> int
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    /* /// OPTION 2 /// OPTION 2 /// OPTION 2 /// OPTION 2 /// OPTION 2 /// */
    public static void getNumOfAwardsPerEntity() throws Exception {
        sparkSession.sql("SELECT recipient_name, COUNT(*) AS num_of_awards "
                       + "FROM USA GROUP BY recipient_name").show(false);
    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> getTotalAwardAmountByDateRange()
     * Purpose  -> Method which returns the total award amount within a
     *             specified date range
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> int, date
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    /* /// OPTION 3 /// OPTION 3 /// OPTION 3 /// OPTION 3 /// OPTION 3 /// */
    public static void getTotalAwardAmountByDateRange() throws Exception {

        // Keyboard reader
        Scanner input = new Scanner(System.in);

        // Prompt user for date range
        System.out.print("Enter a starting date (YYYY-MM-DD): ");
        String startDate = input.nextLine();
        System.out.print("Enter an ending date (YYYY-MM-DD): ");
        String endDate = input.nextLine();

        // Query
        sparkSession.sql("SELECT recipient_name, SUM(total_dollars_obligated) AS total, period_of_performance_start_date AS date"
                        + " FROM USA WHERE '" + startDate + "' <= period_of_performance_start_date AND period_of_performance_current_end_date <= '" + endDate
                        + "' GROUP BY date"
                        + " ORDER BY total DESC;").show(1000, false);

    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> getAwardTotalByQuarterOfTheYear()
     * Purpose  -> Method which returns the total award amount within a
     *             specified quarter of the year (1-4)
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> int, date
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    /* /// OPTION 4 /// OPTION 4 /// OPTION 4 /// OPTION 4 /// OPTION 4 /// */
    public static void getAwardTotalByQuarterOfTheYear() throws Exception {

        // Prime the conditional query
        int quarter = getQuarter();

        /* First Quarter => Jan 1 - Mar 31 */
        if (quarter == 1) {
            Dataset<Row> df = quarterOne();

            // January
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-01-01","2022-01-31"))
                    .show(35,false);

            // February
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-02-01","2022-02-28"))
                    .show(35,false);

            // March
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-03-01","2022-03-31"))
                    .show(35,false);

            sparkMenu.waitAndClear();
        // ----------------------------------------------------------------------------------
        /* Second Quarter => Apr 1 - June 30 */
        } else if (quarter == 2) { 
            Dataset<Row> df = quarterTwo();

            // April
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-04-01", "2022-04-30"))
                    .show(35, false);
            TimeUnit.SECONDS.sleep(3);

            // May
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-05-01", "2022-05-31"))
                    .show(35, false);
            TimeUnit.SECONDS.sleep(3);

            // June
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-06-01", "2022-06-30"))
                    .show(35, false);

            sparkMenu.waitAndClear();
        // ----------------------------------------------------------------------------------
        /* Third Quarter => Jul 1 - Sep 31 */
        } else if (quarter == 3) {
            Dataset<Row> df = quarterThree();

            // July
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-07-01", "2022-07-31"))
                    .show(35, false);
            TimeUnit.SECONDS.sleep(3);

            // August
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-08-01", "2022-08-31"))
                    .show(35, false);
            TimeUnit.SECONDS.sleep(3);

            // September
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-09-01", "2022-09-30"))
                    .show(35, false);

            sparkMenu.waitAndClear();
        // ----------------------------------------------------------------------------------
        /* Fourth Quarter => Oct 1 - Dec 31 */
        } else if (quarter == 4) {
            Dataset<Row> df = quarterFour();

            // October
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-10-01", "2022-10-30"))
                    .show(35, false);
            TimeUnit.SECONDS.sleep(3);

            // November
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-11-01", "2022-11-30"))
                    .show(35, false);
            TimeUnit.SECONDS.sleep(3);

            // December
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-12-01", "2022-12-31"))
                    .show(35, false);
            sparkMenu.waitAndClear();
        }// End of quarter conditions ---
    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> topKAwardsByEntity()
     * Purpose  -> Method which returns the top K awarded amounts per entity
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> int, date
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    /* /// OPTION 5 /// OPTION 5 /// OPTION 5 /// OPTION 5 /// OPTION 5 /// */
    public static void topKAwardsByEntity() throws Exception {

        // Keyboard reader
        Scanner input = new Scanner(System.in);

        // Date to query
        System.out.print("Enter a date to evaluate (YYYY-MM-DD): ");
        String date = input.nextLine();

        // K
        System.out.print("Enter the list size you want to see: ");
        int K = input.nextInt();
        while (K < 1) {
            System.out.println("Invalid Input");
            K = input.nextInt();
        }

        // Query
        sparkSession.sql("SELECT recipient_name, total_dollars_obligated, action_date FROM USA WHERE '"
                + date + "' = action_date ORDER BY total_dollars_obligated DESC;").show(K);

        // Terminal pause and clear
        sparkMenu.waitAndClear();
    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> void listTotalQuarterlyReportsByAwardAmount()
     * Purpose  -> Method which returns the total amount awarded and its
     *             respective quarter
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    /* /// OPTION 6 /// OPTION 6 /// OPTION 6 /// OPTION 6 /// OPTION 6 /// */
    public static void listTotalQuarterlyReportsByAwardAmount() throws Exception {

        // Query per quarter respectively
        Dataset<Row> dfQ1 = df.select(functions.sum("total_dollars_obligated").as("Total Funds")).withColumn("Quarter", functions.lit(1)),
                     dfQ2 = df.select(functions.sum("total_dollars_obligated").as("Total Funds")).withColumn("Quarter", functions.lit(2)),
                     dfQ3 = df.select(functions.sum("total_dollars_obligated").as("Total Funds")).withColumn("Quarter", functions.lit(3)),
                     dfQ4 = df.select(functions.sum("total_dollars_obligated").as("Total Funds")).withColumn("Quarter", functions.lit(4));

        // Combine all quarters via UNION
        Dataset<Row> allQ = dfQ1.union(dfQ2.union(dfQ3.union(dfQ4))); // TODO: Test
        allQ.orderBy(allQ.col("Total Funds").desc()).show(false);

        // Terminal pause and clear
        sparkMenu.waitAndClear();
} // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> void listRecentlyAwardedFunds ()
     * Purpose  -> Method to list the recent awards in the USA.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    /* /// OPTION 7 /// OPTION 7 /// OPTION 7 /// OPTION 7 /// OPTION 7 /// */
    public static void listRecentlyAwardedFunds () throws Exception {

        // Query
        sparkSession.sql("SELECT total_dollars_obligated AS Total_Award, "
                       + "recipient_name AS Group_Awarded, action_date as Date_Awarded FROM USA " +
                         "ORDER BY action_date DESC;").show(20);

        // Force hang
        TimeUnit.MILLISECONDS.sleep(500);

        // Terminal pause and clear
        sparkMenu.waitAndClear();

    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Ivann De la Cruz
     * Method   -> tryFindEntityByName()
     * Purpose  -> Using user input, see if entity exists, if not print related
     * -----------------------------------------------------------------------
     * Receives -> int entity type, string user input
     * Returns  -> boolean found or not
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    /* /// OPTION 8 /// OPTION 8 /// OPTION 8 /// OPTION 8 /// OPTION 8 /// */
    public static boolean tryFindEntityByName(int enType, String usrIn) throws Exception{
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
                System.out.println("\nIncorrect use of fucntion");
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

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Ivann De la Cruz
     * Method   -> awardGiverInfo
     * Purpose  -> retrieve info on agency providing awards
     * -----------------------------------------------------------------------
     * Receives -> string entity name
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    public static void awardGiverInfo(String entName){
        sparkSession.sql("SELECT DISTINCT awarding_agency_name AS Agency_Name," +
                " awarding_office_name AS Office_Name" +
                " FROM USA WHERE awarding_agency_name = \'" + entName + "\'").show(false);
    }

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Ivann De la Cruz
     * Method   -> awardGiverTotalMoney
     * Purpose  -> retrieve total money agency has awarded
     * -----------------------------------------------------------------------
     * Receives -> string entity name
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    public static void awardGiverTotalMoney(String entName){
        sparkSession.sql("SELECT SUM(current_total_value_of_award) AS Total_Money_Awarded " +
                "FROM USA WHERE awarding_agency_name = \'" + entName + "\'").show(false);
    }

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Ivann De la Cruz
     * Method   -> awardGiverTransactions
     * Purpose  -> retrieve information on times awards have been given
     *              by this entity
     * -----------------------------------------------------------------------
     * Receives -> string entity name
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    public static void awardGiverTransactions(String entName){
        sparkSession.sql("SELECT contract_transaction_unique_key AS Unique_ID," +
                " recipient_name AS Recipient, current_total_value_of_award as Current_Total, " +
                "award_type as Award_Type, action_date AS Action_Date " +
                "FROM USA WHERE awarding_agency_name = \'" + entName + "\'").show(false);
    }

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Ivann De la Cruz
     * Method   -> recipientInfo
     * Purpose  -> retrieve information on award recipient
     * -----------------------------------------------------------------------
     * Receives -> string entity name
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    public static void recipientInfo(String entName){
        sparkSession.sql("SELECT DISTINCT recipient_name, recipient_uei, recipient_duns, recipient_parent_name, recipient_parent_uei" +
                ", recipient_parent_duns FROM USA WHERE recipient_name = \'" + entName + "\'").show(false);
    }

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Ivann De la Cruz
     * Method   -> recipientAKA
     * Purpose  -> return aliases for recipient
     * -----------------------------------------------------------------------
     * Receives -> string entity name
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */

    public static void recipientAKA(String entName){
        sparkSession.sql("SELECT DISTINCT recipient_doing_business_as_name AS Also_Known_As " +
                "FROM USA WHERE recipient_name = \'" + entName + "\'").show(false);
    }

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Ivann De la Cruz
     * Method   -> recipientLoc
     * Purpose  -> retrieve locations where recipient has stated they are
     * -----------------------------------------------------------------------
     * Receives -> string entity name
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */

    public static void recipientLoc(String entName){
        sparkSession.sql("SELECT DISTINCT recipient_country_name AS Country, recipient_address_line_1 AS Address1, " +
                "recipient_address_line_2 AS Address2, recipient_city_name AS City, recipient_county_name AS County, " +
                "recipient_state_name AS State, recipient_zip_4_code AS ZIP " +
                "FROM USA WHERE recipient_name = \'" + entName + "\'").show(false);
    }

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Ivann De la Cruz
     * Method   -> recipientPReg
     * Purpose  -> IMPORTANT, return areas where money recived is projected
     *              to have an impact
     * -----------------------------------------------------------------------
     * Receives -> string entity name
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    public static void recipientPReg(String entName){
        sparkSession.sql("SELECT DISTINCT primary_place_of_performance_country_name AS Country, " +
                "primary_place_of_performance_city_name AS City, " + "primary_place_of_performance_county_name As County, " + 
                "primary_place_of_performance_state_name AS State, primary_place_of_performance_zip_4 AS ZIP," +
                "primary_place_of_performance_congressional_district AS Congr_Distr " +
                "FROM USA WHERE recipient_name = \'" + entName + "\'").show(false);
    }

    /* ---------------------------------------------------------------------- */
                            /* >>> Helper functions <<< */
    /* ---------------------------------------------------------------------- */

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> getQuarter()
     * Purpose  -> Method which prompts the user to enter a number between
     *             1 and 4.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> int
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
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

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> quarterOne()
     * Purpose  -> Method which runs queries on the first quarter of the year.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> first quarter data
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    private static Dataset<Row> quarterOne() throws Exception { // TODO: Fix query, no 'date' col
        Dataset<Row> temp = sparkSession.sql("SELECT * FROM USA WHERE '2022-01-01' <= action_date AND '2022-03-31' >= action_date ORDER BY action_date;");
//        temp.show();
        return temp;
    }

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> quarterTwo()
     * Purpose  -> Method which runs queries on the second quarter of the year.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> second quarter data
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    private static Dataset<Row> quarterTwo() throws Exception { // TODO: Fix query, no 'date' col
        Dataset<Row> temp = sparkSession.sql("SELECT * FROM USA WHERE '2022-04-01' <= action_date AND '2022-06-30' >= action_date ORDER BY action_date;");
//        temp.show();
        return temp;
    }

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> quarterThree()
     * Purpose  -> Method which runs queries on the third quarter of the year.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> third quarter data
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    private static Dataset<Row> quarterThree() throws Exception { // TODO: Fix query, no 'date' col
        Dataset<Row> temp = sparkSession.sql("SELECT * FROM USA WHERE '2022-07-01' <= action_date AND '2022-09-31' >= action_date ORDER BY action_date;");
//        temp.show();
        return temp;
    }

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> quarterFour()
     * Purpose  -> Method which runs queries on the fourth quarter of the year.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> fourth quarter data
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    private static Dataset<Row> quarterFour() throws Exception { // TODO: Fix query, no 'date' col
        Dataset<Row> temp = sparkSession.sql("SELECT * FROM USA WHERE '2022-10-01' <= action_date AND '2022-12-31' >= action_date ORDER BY action_date;");
//        temp.show();
        return temp;
    }

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> void quarterHelper()
     * Purpose  -> Helper function which allows Quarterly Reports to be shown.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    public static void quarterHelper() throws Exception { // TODO: fix temporarily public
        Dataset<Row> df1 = quarterOne(),
                     df2 = quarterTwo(),
                     df3 = quarterThree(),
                     df4 = quarterFour();

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
    } // ---------------------------------------------------------------------


}// End of US Spending Queries !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
