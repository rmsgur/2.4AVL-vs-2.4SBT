package org.hsqldb.sample;

import java.io.*;
import java.util.*;
import java.sql.*;


public class RunScript {
	
	Connection conn;  
	
	public RunScript (String db_file_name_prefix) throws Exception {
        Class.forName("org.hsqldb.jdbc.JDBCDriver");

        conn = DriverManager.getConnection("jdbc:hsqldb:hsql://localhost",    // filenames
                                           "SA",                              // username
                                           "");                               // password
    }
	
	public static void main(String[] args) {
		try {
			RunScript db = new RunScript("db_file");
			db.run();
			
			
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
	}
	
	public synchronized void run() throws SQLException {
		try {
			Scanner tInput = new Scanner(System.in);
			System.out.print("File Name: ");
			File file = new File(tInput.next());
			Scanner input = new Scanner(file);
			input.useDelimiter("\n");
			System.out.println("Start");
			while(input.hasNext()) {
				Statement st = null;
				ResultSet rs = null;
			
				String expression = input.next();
				st = conn.createStatement();
				rs = st.executeQuery(expression);
				dump(rs);
				st.close();
			}
		
			Statement st = conn.createStatement();
			st.execute("SHUTDOWN");
			conn.close();
		}
		catch(Exception e) {
            e.printStackTrace(System.err);
        }
		System.out.println("Done");
	}
	
	public static void dump(ResultSet rs) throws SQLException {
        ResultSetMetaData meta   = rs.getMetaData();
        int               colmax = meta.getColumnCount();
        int               i;
        Object            o = null;

        for (; rs.next(); ) {
            for (i = 0; i < colmax; ++i) {
                o = rs.getObject(i + 1);
                try {
                	System.out.print(o.toString() + " ");
                }catch(Exception e) {
                	if(o == null)System.out.print("null ");
                }
            }

            System.out.println(" ");
        }
    } 
}
