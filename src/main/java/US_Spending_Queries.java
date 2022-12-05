package SparkWorks; // Enveloped package

//Apache Spark Libraries
import org.apache.spark.sql.*;

//Java Includes
import java.io.*;
import java.io.BufferedReader;
import java.util.Scanner;

public class US_Spending_Queries {
    public static Dataset<Row> df; // TODO: fix temp public

    private static SparkMainApp sparkMenu = new SparkMainApp(); // Used to call helper terminal-menu functions
    private static SparkSession sparkSession;
    private static Scanner input = new Scanner(System.in);

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
                       + "FROM USA GROUP BY recipient_name").show();
    }

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
        sparkSession.sql("SELECT COUNT(*) AS num_of_awards "
                       + "FROM USA GROUP BY recipient_name").show();
    }

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
        sparkSession.sql("SELECT SUM(total_dollars_obligated) AS total, start_date" +
                       + " FROM USA WHERE '" + startDate + "' <= start_date AND end_date <= '" + endDate
                       + "' GROUP BY start_date" +
                       + " ORDER BY total DESC;").show(1000, false);

    }

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
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-01-01","2022-01-31"))
                    .show(35,false);

            df.select(df.col("action_date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported"))
                    .filter(df.col("action_date").between("2022-02-01","2022-02-28"))
                    .show(35,false);

            df.select(df.col("action_date"), df.col("state_name"), df.col("overall_outcome"), df.col("total_results_reported"))
                    .filter(df.col("action_date").between("2022-03-01","2022-03-31"))
                    .show(35,false);
        // ----------------------------------------------------------------------------------
        /* Second Quarter => Apr 1 - June 30 */
        } else if (quarter == 2) { 
            Dataset<Row> df = quarterTwo(state,caseResult);
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-04-01", "2022-04-30"))
                    .show(35, false);
            TimeUnit.SECONDS.sleep(3);

            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-05-01", "2022-05-31"))
                    .show(35, false);
            TimeUnit.SECONDS.sleep(3);

            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-06-01", "2022-06-30"))
                    .show(35, false);

            sparkMenu.waitAndClear();
        // ----------------------------------------------------------------------------------
        /* Third Quarter => Jul 1 - Sep 31 */
        } else if (quarter == 3) {
            Dataset<Row> df = quarterThree();
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-07-01", "2022-07-31"))
                    .show(35, false);
            TimeUnit.SECONDS.sleep(3);

            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("action_date").between("2022-08-01", "2022-08-31"))
                    .show(35, false);
            TimeUnit.SECONDS.sleep(3);

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
                    .filter(df.col("date").between("2022-11-01", "2022-11-30"))
                    .show(35, false);
            TimeUnit.SECONDS.sleep(3);

            // December
            df.select(df.col("action_date"), df.col("recipient_name"), df.col("total_dollars_obligated"))
                    .filter(df.col("date").between("2022-12-01", "2022-12-31"))
                    .show(35, false);
            sparkMenu.waitAndClear();
        }// End of quarter conditions ---
} // ---------------------------------------------------------------------

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
