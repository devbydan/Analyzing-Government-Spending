package SparkWorks; // Enveloped package

//Apache Spark Libraries
import org.apache.spark.sql.*;

//Java Includes
import java.io.*;
import java.io.BufferedReader;
import java.util.Scanner;

public class Queries {
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
    public US_Spending_Queries(String filePath, SparkSession sparkSession)
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


    public static void getTotalAmountAwardedByGroup() throws Exception {
        sparkSession.sql("SELECT SUM(total_dollars_obligated) AS total FROM spending GROUP BY recipient_name ASC;")
    }


}// End of US Spending Queries !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
