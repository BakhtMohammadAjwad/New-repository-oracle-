
package consoleapp.newpackage;

import java.io.*;
import au.com.bytecode.opencsv.CSVReader;
import java.util.*;
import java.sql.*;
        
public class CSVto_orc {
    
    public static void main (String[] args) throws Exception{

        Class.forName("oracle.jbdc.OracleDriver");
        Connection conn = DriverManager.getConnection("jbdc:oracle:thin:@//localhost:1521/xe","hr","hr");
        PreparedStatement sql_statement = null;
        String jbdc_insert_sql = "INSERT INTO CSV_2_ORACLE"+
                "(USER_ID, USER_AGE) VALUES"+ "(?,?)";
        sql_statement = conn.prepareStatement(jbdc_insert_sql);
        
        String inputCSVFile = "inputdata.csv";
        CSVReader reader = new CSVReader(new FileReader(inputCSVFile));
        String [] nextLine;
        int InNum = 0;
    
        while ((nextLine = reader.readNext()) != null){
            InNum++;
            
            sql_statement.setString(1, nextLine[0]);
            
            
            sql_statement.setDouble(2, Double.parseDouble(nextLine[1]));
            
            sql_statement.addBatch();
        }
        
        int[] totalRecords = new int[7];
        try{
            totalRecords = sql_statement.executeBatch();
        }catch(BatchUpdateException e){
        
            totalRecords = e.getUpdateCounts();
        }
        System.out.println("Total records inserted in bulk from CSV file "+totalRecords.length);
        
        sql_statement.close();
        
        conn.commit();
    
        conn.close();
    }
}
