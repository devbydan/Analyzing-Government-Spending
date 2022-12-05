package SparkWorks; // Enveloped package

//Apache Spark Libraries
import org.apache.spark.sql.*;

//Java Includes
import java.io.*;
import java.io.BufferedReader;
import java.util.Scanner;

public class US_Spending_Queries {
    public static Dataset<Row> df; // TODO: fix temp public
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
    public static boolean tryFindEntityByName(int enType, String usrIn) throws Exception{
        usrIn = usrIn.toUpperCase(); // this is to fit formating, may as well do it automatically
        String enTStr = "";
        String enTInfStr = "";
        // To expand add more cases
        switch (enType){
            case 1:
                enTStr = "parent_award_agency_name";
                enTInfStr = "Giver";
                break;
            case 2:
                enTStr = "recipient_name";
                enTInfStr = "Receiver";
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

    /* >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Helper functions <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< */

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
