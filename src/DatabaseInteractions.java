import java.io.*;
import java.sql.*;
import java.util.Scanner;
import java.util.logging.*;
import java.text.*;

public class DatabaseInteractions {

  // DB variables
  static String db_name = "test_table_n";
  static Connection pos_conn = null;
  static Statement pos_stmt = null;
  static Boolean useServerPostgresDB = false;
  static final String DB_PREFIX = "jdbc:postgresql://";

  // LOCAL Configurations
  static final String local_DB_HOST = "localhost";
  static final String local_DB_NAME = "thesis_data_ingestion";
  static final String local_DB_USER = "postgres";
  static final String local_DB_PASS = "silvia";

  // Configurations to server PostgreSQL database
  static final String DB_HOST = "ironmaiden.inf.unibz.it";
  static final int DB_PORT = 5433;
  static final String DB_NAME = "sfracalossi";
  static String DB_USER;
  static String DB_PASS;

  // Logger formatter
  static SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");

  // Location of file containing data
  String data_file_path;

  // Constructor
  public DatabaseInteractions (String data_file_path, Boolean useServerPostgresDB, String DB_USER, String DB_PASS) {
    this.useServerPostgresDB=useServerPostgresDB;
    this.DB_USER=DB_USER;
    this.DB_PASS=DB_PASS;
    this.data_file_path=data_file_path;
  }

  // Method called from for-loop in main, choosing correct method for insertion
  public void insertNTuples (int N, Logger logger) {
    if (N==0)   return;
    if (N==1)   insertOneTuple(logger);
    else        insertMultipleTuples(N, logger);
  }

  // Iterating through data, inserting it one at a time
  public void insertOneTuple(Logger logger) {

    // Defining variables useful for method
    String[] fields;
    String row;
    int rows_inserted = 0;

    // Timestamp variables
    java.util.Date parsedDate;
    Timestamp timestamp;

    try {

      // Preparing file scanner
      Scanner reader = new Scanner(new File(data_file_path));

      // Defining variables for the insertion
      String insertion_query = "INSERT INTO "+db_name+" (time, value) VALUES (?, ?)";
      PreparedStatement pst = pos_conn.prepareStatement(insertion_query);

      // Signaling start of test
      logger.info("--Start of test--");
      while (reader.hasNextLine()) {

        // Retrieving the data and preparing insertion script
        row = reader.nextLine();
        fields = row.split(",");

        // Casting timestamp
        parsedDate = dateFormat.parse((String)fields[0]);
        timestamp = new Timestamp(parsedDate.getTime());

        // Inserting the variables in the prepared statement
        pst.setTimestamp(1, timestamp);
        pst.setInt(2, Integer.parseInt(fields[1]));

        // Executing the query and checking the result
        if (pst.executeUpdate() != 1) {
            logger.severe("Problem executing the query\n");
        } else {
            rows_inserted++;
            logger.info("Query successfully executed: ("+fields[0]+","+fields[1]+")\n");
        }
      }

      // Closing the file reader
      reader.close();

    } catch (FileNotFoundException e) {
      System.out.println("An error occurred.");
      e.printStackTrace();
      logger.severe("Insertion: \"1\" - problems with the execution");
    } catch (SQLException e) {
      logger.severe("Problem executing the script\n");
      e.printStackTrace();
    } catch (ParseException e) {
      logger.severe("Problem with parsing a timestamp\n");
      e.printStackTrace();
    }
    logger.info("Total rows inserted: "+rows_inserted);
  }


  // Iterating through data, inserting it i at a time
  public void insertMultipleTuples(int N, Logger logger) {

    // Defining variables useful for method
    String[] fields;
    String row;
    int rows_inserted = 0;

    // Defining variables for the insertion
    String original_insert_query = "INSERT INTO "+db_name+" (time, value) VALUES ";
    String insertion_query = original_insert_query;
    String insertions="";

    // Number of tuples inserted in the script but not yet executed
    int no_rows_waiting = 0;

    // Timestamp variables
    java.util.Date parsedDate;
    Timestamp timestamp;

    try {
      Scanner reader = new Scanner(new File(data_file_path));

      // Preparing the insertion query
      for (int i=0; i<N; i++) {
        if (i!=0) {
          insertion_query+=", ";
        }
        insertion_query+="(?, ?)";
      }

      PreparedStatement pst = pos_conn.prepareStatement(insertion_query);

      // Signaling start of test
      logger.info("--Start of test--");
      while (reader.hasNextLine()) {

        // Retrieving the data and preparing insertion script
        row = reader.nextLine();
        fields = row.split(",");
        insertions += "('" + fields[0] + "'," + fields[1] + ") ";

        // Casting timestamp
        parsedDate = dateFormat.parse((String)fields[0]);
        timestamp = new Timestamp(parsedDate.getTime());

        // Inserting the variables in the prepared statement
        int first_column = (no_rows_waiting*2)+1;
        int second_column = (no_rows_waiting+1)*2;
        pst.setTimestamp(first_column, timestamp);
        pst.setInt(second_column, Integer.parseInt(fields[1]));
        no_rows_waiting++;

        // Executing the query and checking the result, if number of rows is enough
        if (no_rows_waiting == N) {
          if (pst.executeUpdate() == no_rows_waiting) {
            rows_inserted+=no_rows_waiting;
            logger.info("Query successfully executed: \n"+insertions);
            insertions = "";
          } else {
            no_rows_waiting = 0;
            logger.severe("Problem executing the following insertion"+insertions+"\n");
            insertions = "";
          }

          // Reset variables for successive tuples
          no_rows_waiting = 0;
        }
      }

      // Last insertion, if N was not reached
      if (no_rows_waiting != 0) {
        original_insert_query+= " "+insertions.replace(") (", "),(");
        if (pos_stmt.executeUpdate(original_insert_query) == no_rows_waiting) {
          rows_inserted+=no_rows_waiting;
          logger.info("Query successfully executed: \n"+insertions);
        } else {
          logger.severe("Problem executing the following insertion"+insertions+"\n");
        }
      }

      // Closing the file reader
      reader.close();

    } catch (FileNotFoundException e) {
      System.out.println("An error occurred.");
      e.printStackTrace();
      logger.severe("Insertion: \"Multiple tuples at a time\" - problems with the execution");
    } catch (SQLException e) {
      logger.severe("Problem executing the following script\n");
      e.printStackTrace();
    } catch (ParseException e) {
      logger.severe("Problem with parsing a timestamp\n");
      e.printStackTrace();
    }

    logger.info("Total rows inserted: "+rows_inserted);
  }

  //----------------------DATABASE UTILITY--------------------------------------

  // Connecting to the PostgreSQL database
  public static void createDBConnection() {
    try {

      // Creating the connection URL
      String pos_complete_url;
      if (useServerPostgresDB) {
         pos_complete_url = DB_PREFIX + DB_HOST + ":" + DB_PORT + "/" + DB_NAME
         + "?user=" + DB_USER + "&password=" + DB_PASS;
      } else {
         pos_complete_url = DB_PREFIX + local_DB_HOST + "/" + local_DB_NAME
         + "?user=" + local_DB_USER +"&password=" + local_DB_PASS;
      }

      // Connecting and creating a statement
      pos_conn = DriverManager.getConnection(pos_complete_url);
      pos_stmt = pos_conn.createStatement();

      // Removing old table
      removeTestTable();
    } catch (SQLException e) {
      System.out.println("Problems with creating the database connection");
      e.printStackTrace();
    }
  }

  // Creating the table "test_table" in the database
  public static boolean createTestTable () {
    try {
      String test_table_creation = "CREATE TABLE "+db_name+" (" +
              "    time timestamp NOT NULL," +
              "    value smallint NOT NULL" +
              ")";
      return (pos_stmt.executeUpdate(test_table_creation) == 0);
    } catch (SQLException e) {
      e.printStackTrace();
    }

    return false;
  }


  // Dropping the table "test_table" from the database
  public static boolean removeTestTable() {
    try {
      String test_table_drop = "DROP TABLE IF EXISTS "+db_name+";";
      return (pos_stmt.executeUpdate(test_table_drop) == 0);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return false;
  }


  // Closing the connections to the database
  public static void closeDBConnection() {
    try{
       if(pos_stmt!=null) pos_stmt.close();
    } catch(SQLException se2) {
        se2.printStackTrace();
    }
    try {
       if(pos_conn!=null) pos_conn.close();
    } catch(SQLException se){
       se.printStackTrace();
    }

    // Nulling the database variables
    pos_conn = null;
    pos_stmt = null;
  }
}
