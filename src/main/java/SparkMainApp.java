package SparkWorks; // Enveloped package

//Apache Spark Libraries
import org.apache.spark.sql.*;

//Java Includes
import java.io.IOException;
import java.util.Scanner;

public class SparkMainApp {

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> void greeting()
     * Purpose  -> Method which clears the screen for readability.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    public static void clearScreen() {
        System.out.print("\033[H\033[2J");
        System.out.flush();
    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> void waitUntilEnter()
     * Purpose  -> Method which assists in pausing info printing for better
     *             readability.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    public static void waitUntilEnter() throws IOException {
        System.out.print("Press Enter to Continue . . .\n\n");
        int enter = System.in.read();
        while(enter != 10){
            enter = System.in.read();
        }
    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> void waitAndClear()
     * Purpose  -> Method which assists in user experience;
     *             pausing info printing for better readability and
     *             clearing the screen of the previously run query.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    public static void waitAndClear() throws Exception {
        waitUntilEnter();
        clearScreen();
    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> void greeting()
     * Purpose  -> Method to print a greeting to the console menu.
     *             Purely aesthetic.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    public static void greeting(){
        System.out.println(
                "\n\n**************************************************\n" +
                        "             Analyzing Government Spending      \n" +
                        "**************************************************\n");
    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> void choiceMenu()
     * Purpose  -> Method to print the menu of choices
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    public static void choiceMenu(){
        System.out.println("Please choose what you would like to do:\n" +
                "0. EXIT\n" +
                "1. US Govt Spending\n");
        
    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> void queryMenu()
     * Purpose  -> Method to print the query choices
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    public static void queryMenu(){
        System.out.println("0. None, Go Back\n" +
                "1. Get Total Amount Awarded by Group\n" +
                "2. Get # of Awards Per Entity\n" +
                "3. Get Total Award Amount By Date Range\n"+
                "4. Get Total Award Amount By Quarter\n"+
                "5. Show Top 'K' Awarded Amounts Per Entity\n"+
                "6. List Quarterly Reports\n"+
                "7. Show List of Recent Events\n"+
                "8. Look Up Entity\n");

    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> void mainMenu(SparkSession sparkSession)
     * Purpose  -> Method to initialize the main menu.
     *             Allows the user to choose which dataset to run queries on.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    public static void mainMenu(SparkSession sparkSession) throws Exception {
        Scanner input = new Scanner(System.in); // Grabs the input from the keyboard
        int choice; // User choice from the terminal

        clearScreen(); // Clears the screen
        greeting();    // Title of the project
        choiceMenu();  // Exit or US Spending Data

        // User choice via terminal --- 0 to exit, 1 to access US Spending Data queries
        while((choice = input.nextInt()) != 0) {
            switch (choice) {
                case 1:
                    System.out.println("\nAccessing US Spending Database...\n");
                    System.out.println("Please wait while the data is pre-processed...\n\n");
                    queryUSSpending(sparkSession);
                    break;
                default:
                    System.out.println("Invalid Input");
                    mainMenu(sparkSession);
            }// End of choice switch statement ---

        clearScreen(); // Clears the screen
        greeting();    // Title of the project
        choiceMenu();  // Exit or US Spending Data
        }// End of while loop ---
    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Dan Murphy
     * Method   -> void mainMenu(SparkSession sparkSession)
     * Purpose  -> Method to permit queries to run on the USA.csv dataset.
     * -----------------------------------------------------------------------
     * Receives -> NONE
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    public static void queryUSSpending(SparkSession sparkSession) throws Exception {
        US_Spending_Queries db = new US_Spending_Queries("hdfs://localhost:9000/US-Spending/Contracts_PrimeTransactions_2022-11-23_H17M10S30_1.csv", sparkSession); // Grabs the file from dir
//        // For Testing
//        System.out.println("\nHere is what I loaded");
//        db.df.printSchema();
//        System.out.println("\n");
        Scanner input = new Scanner(System.in); // Grabs the input from the keyboard
        int choice; // User choice from the terminal

        clearScreen(); // Clears the screen
        greeting();    // Title of the project
        queryMenu();   // Prints the choice of queries

        // User choice via terminal --- 0 to exit, 1 to access US Spending Data queries
        while((choice = input.nextInt()) != 0) {
            switch (choice) {
                case 1: db.getTotalAmountAwardedByGroup(); break;
                case 2: db.getNumOfAwardsPerEntity(); break;
                case 3: db.getTotalAwardAmountByDateRange(); break;
                case 4: db.getAwardTotalByQuarterOfTheYear(); break;
                case 5: db.topKAwardsByEntity(); break;
                case 6: db.listTotalQuarterlyReportsByAwardAmount(); break;
                case 7: db.listRecentlyAwardedFunds(); break;
                case 8: entityLookUp(db); break;
                default: System.out.println("Invalid Input");
            }
            greeting();  // Title of the project
            queryMenu(); // Prints the choice of queries
        }
    } // ---------------------------------------------------------------------

     /* =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
             * Author   -> Ivann De la Cruz
     * Method   -> entityLookUp()
     * Purpose  -> Method which provides 1st layer lookup interface for entities
     *              determines what column to look in
     * -----------------------------------------------------------------------
             * Receives -> US_Spending_Queries type
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    public static void entityLookUp(US_Spending_Queries db) throws Exception {
        Scanner entityLkUp_input = new Scanner(System.in); // Grabs the input from the keyboard
        int ent_choice; // User choice from the terminal

        clearScreen(); // Clears the screen
        greeting();    // Title of the project

        // print out options to find entity
        System.out.println("0. Return\n" +
                "1. Search for funding GIVER\n" +
                "2. Search for funding RECEVIER\n");

        while ((ent_choice = entityLkUp_input.nextInt()) != 0) {
            switch (ent_choice) {
                case 1: entityLookUp_QueryType(db, 1); break;
                case 2: entityLookUp_QueryType(db, 2); break;
                default:
                    System.out.println("Invalid Input");
                    break;
            }
            greeting();    // Title of the project

            // print out options to find entity
            System.out.println("0. Return\n" +
                    "1. Search for funding GIVER\n" +
                    "2. Search for funding RECEVIER\n");
        }
    } // ---------------------------------------------------------------------

    /*
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     * Author   -> Ivann De la Cruz
     * Method   -> entityLookUp_QueryType()
     * Purpose  -> Method which provides user lookup interface for entities
     *              level 2, type of query
     * -----------------------------------------------------------------------
     * Receives -> US_Spending_Queries type, integer indicating type giver 1
     *              receiver 2
     * Returns  -> NONE
     * =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
     */
    public static void entityLookUp_QueryType(US_Spending_Queries db, int entType) throws Exception {
        Scanner entityLkUp_input = new Scanner(System.in); // Grabs the input from the keyboard
        String ent_choice = ""; // User choice from the terminal

        clearScreen(); // Clears the screen
        greeting();    // Title of the project

        // inform user of lookup type
        String noticeStr = "Looking for ";
        if(entType == 1){ noticeStr += "giver"; }
        else if(entType == 2){ noticeStr += "receiver"; }
        System.out.println(noticeStr);

        // print out options to find entity
        System.out.println("Enter name or type 0 to return");

        while (!(ent_choice = entityLkUp_input.nextLine()).equals("0")) {
            if(db.tryFindEntityByName(entType, ent_choice)){
                System.out.println("Found exact match!");
                // TODO: lead to new menu
                return; // for now
            }
            else{
                System.out.println("\nEnter name or type 0 to return");
            }
        }
    } // ---------------------------------------------------------------------



    /* MAIN TEST HARNESS */
    public static void main(String[] args) throws Exception {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("CSV Test App")
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
        mainMenu(sparkSession);

        System.out.println("Session Shutting Down");
    } // ---------------------------------------------------------------------


} // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!