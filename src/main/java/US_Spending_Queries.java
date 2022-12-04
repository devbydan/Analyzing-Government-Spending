package SparkWorks; // Enveloped package

//Apache Spark Libraries
import org.apache.spark.sql.*;

//Java Includes
import java.io.*;
import java.io.BufferedReader;
import java.util.Scanner;

public class US_Spending_Queries {
    private static Dataset<Row> df;
    private static SparkSession sparkSession;
    private static Scanner input = new Scanner(System.in);

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> US_Spending_Queries(String filePath, SparkSession sparkSession)
     * Purpose  -> Constructor that sets the private data field Dataset<Row>
                   and SparkSession. Afterwards, create the temporary view of
                   the csv file that will be looked at.
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
    public static void getTotalAmountAwardedByGroup() throws Exception {
        sparkSession.sql("SELECT recipient_name AS recipient, SUM(total_dollars_obligated) AS total FROM USA GROUP BY recipient_name").show();
    }

    /* >>> Helper functions <<< */

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> getQuarter()
     * Purpose  -> Method which prompts the user to enter a number between
     *             1 and 4
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
    private static Dataset<Row> quarterOne() throws Exception {
        sparkSession.sql("SELECT * FROM USA WHERE '2022-01-01' <= date AND '2022-03-31' >= date ORDER BY date;");
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
    private static Dataset<Row> quarterTwo() throws Exception {
        sparkSession.sql("SELECT * FROM USA WHERE '2022-04-01' <= date AND '2022-06-30' >= date ORDER BY date;");
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
    private static Dataset<Row> quarterThree() throws Exception {
        sparkSession.sql("SELECT * FROM USA WHERE '2022-07-01' <= date AND '2022-09-31' >= date ORDER BY date;");
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
    private static Dataset<Row> quarterFour() throws Exception {
        sparkSession.sql("SELECT * FROM USA WHERE '2022-10-01' <= date AND '2022-12-31' >= date ORDER BY date;");
    }

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> void quarterHelper(String state, String caseResult)
     * Purpose  -> Helper function which allows Quarterly Reports to be shown.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    private static void quarterHelper() throws Exception {
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
