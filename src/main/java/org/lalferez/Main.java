package org.lalferez;


import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
/*Logic:
* 1. Read file
* 2. Separate */

public class Main {
    public static void main(String[] args) {
        //Name of the file that will read it
        String file = "callRecords.csv";

        BufferedReader reader = null;
        String line = "";
        //
        HashMap<Integer,String> msisdn = new HashMap<>();
        HashMap<String, String> datetime = new HashMap<>();
        HashMap<String, Integer> duration = new HashMap<>();
        HashMap<String, Integer> counter = new HashMap<>();
        int i= 1;
        try {
            //passing file name
            reader = new BufferedReader(new FileReader(file));
            //read each line
            while ((line = reader.readLine()) != null) {
                //Split line to saving into hashmap
                String[] row = line.split(",");
                //summarize values
                if (!row[2].equals("Duration")) {
                    //Here we insert values, (MSISDN, CALL DATE, DURATION, COUNT)
                    //MSISDN
                    if (!msisdn.containsValue(row[0])) {
                        msisdn.put(i, row[0]);
                        i++;
                    }
                    //DURATION
                    if (duration.containsKey(row[0])) {
                        duration.put(row[0], duration.get(row[0]) + Integer.parseInt(row[2]));
                    } else duration.put(row[0], Integer.valueOf(row[2]));
                    //DATE
                    if (!datetime.containsKey(row[0]))//if is absent, then...
                        datetime.put(row[0], (row[1]));
                    else //if is present, then... compare Date and overwrite the earlest
                        if (datetime.get(row[0]).compareTo((row[1])) > 0)
                            datetime.put(row[0], (row[1]));
                    //else is earliest
                    //RecordsType
                    if (!counter.containsKey(row[0]))//if not here, then... Put within a 1
                        counter.put(row[0], 1);
                    else counter.put(row[0], counter.get(row[0]) + 1);
                }

            }
            //Connect to DB
            Connection connection = null;
            Statement statement = null;
            try {
                //Data properties of DB
                String url = "jdbc:oracle:thin:@localhost:1522:XE";
                String username = "system";
                String pass = "system";

                connection = DriverManager.getConnection(url, username, pass);
                statement = connection.createStatement();

                //Query
                //Query needs insert each value for table
                String query = "INSERT INTO CALLRECORDS VALUES('" + datetime.values() + "'," +
                        "'" + msisdn.values() + "'," +
                        "'" + duration.values() + "'," +
                        "'" + counter.values() + "')";
                statement.execute(query);
                System.out.println("Records saved");
                //Creator CSV
                try {
                    PrintWriter printWriter = new PrintWriter(new File("callRecordOutput.csv"));
                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append(msisdn.values());
                    stringBuilder.append(datetime.values());
                    stringBuilder.append(duration.values());
                    stringBuilder.append(counter.values());
                    printWriter.write(stringBuilder.toString());
                    printWriter.close();
                    System.out.println("CSV Created");
                }catch (Exception e){

                }
            } catch (SQLException exception) {
                exception.printStackTrace();
            }

        }catch (Exception exception){
            exception.printStackTrace();
        }finally {
            try {
                reader.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}