package org.hsqldb.sample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class Testdb {

    Connection conn;

    public Testdb(String db_file_name_prefix) throws Exception {    // note more general exception

        Class.forName("org.hsqldb.jdbc.JDBCDriver");

        conn = DriverManager.getConnection("jdbc:hsqldb:hsql://localhost/",    // filenames
                                           "SA",                     // username
                                           "");                      // password
    }

    public void shutdown() throws SQLException {

        Statement st = conn.createStatement();

        // db writes out to files and performs clean shuts down
        // otherwise there will be an unclean shutdown
        // when program ends
        st.execute("SHUTDOWN");
        conn.close();    // if there are no other open connection
    }

//use for SQL command SELECT
    public synchronized void query(String expression) throws SQLException {

        Statement st = null;
        ResultSet rs = null;

        st = conn.createStatement();
        rs = st.executeQuery(expression);
        dump(rs);
        st.close();
    }

//use for SQL commands CREATE, DROP, INSERT and UPDATE
    public synchronized void update(String expression) throws SQLException {

        Statement st = null;

        st = conn.createStatement();

        int i = st.executeUpdate(expression);

        if (i == -1) {
            System.out.println("db error : " + expression);
        }

        st.close();
    }    // void update()

    public static void dump(ResultSet rs) throws SQLException {

        ResultSetMetaData meta   = rs.getMetaData();
        int               colmax = meta.getColumnCount();
        int               i;
        Object            o = null;

        for (; rs.next(); ) {
            for (i = 0; i < colmax; ++i) {
                o = rs.getObject(i + 1);    // Is SQL the first column is indexed

                // with 1 not 0
                System.out.print(o.toString() + " ");
            }

            System.out.println(" ");
        }
    }

    public static void main(String[] args) {

        Testdb db = null;

        try {
            db = new Testdb("db_file");
        } catch (Exception ex1) {
            ex1.printStackTrace();    // could not start db

            return;                   // bye bye
        }

        System.out.println("Start\n");
        try {
        	db.query("select * from region");
			for(int i = 1995; i <= 1999; i++) {
				System.out.println("Year: " + i + "\n");
				for(int j = 1; j <= 12; j++) {
					System.out.println("Month: " + j + " to " + (j+1));
					long total = 0;
					for(int k = 0; k < 10; k++) {
						long start_time = System.currentTimeMillis();
						System.out.println("select    100.00 * sum(case        when p_type like 'PROMO%'            then l_extendedprice * (1 - l_discount)        else 0    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from    lineitem,    part where    l_partkey = p_partkey    and l_shipdate >= date '" + i + "-" + j + "-01'    and l_shipdate < date '" + i + "-" + j + "-01' + interval '1' month;");
						long end_time = System.currentTimeMillis();
						System.out.println((end_time-start_time) + "ms");
						total += (end_time-start_time);
					}
					System.out.println((total / 10) + "ms" + "\n\n");
				}
			}
        	
        } catch (SQLException ex3) {
            ex3.printStackTrace();
        }
        
        
        System.out.println("Done");
    }
}