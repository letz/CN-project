import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Date;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

public class SQLOutputFormat implements OutputFormat<Text, BirdStatsWritable> {
        
        /**
         * Access credentials (there credentials are public).
         */
        private static String USER = "cnproject";
        private static String URL = "jdbc:postgresql://127.0.0.1:5432/cnprojectdb";
        private static String PASS = "cnproject";
        private static Connection conn = null;
        
        /**
         * Helper to get a connection.
         * @return - an sql connection.
         * @throws IOException - if something goes wrong creating the connection.
         */
        public static Connection getSQLConnection() throws IOException {
            if (conn == null) { 
                try {
                    Class.forName("org.postgresql.Driver");
                    conn = DriverManager.getConnection(URL,USER,PASS); 
                } 
                catch (SQLException e) { throw new IOException(e); } 
                catch (ClassNotFoundException e) { throw new IOException(e); } 
            }
            return conn;
        }
        

        /**
         * Not needed. Empty implementation.
         */
        @Override
        public void checkOutputSpecs(FileSystem arg0, JobConf arg1)
                throws IOException {}

        /**
         * Class defining how to insert information on the database.
         */
        @Override
        public RecordWriter<Text, BirdStatsWritable> getRecordWriter(
                FileSystem arg0, JobConf arg1, String arg2, Progressable arg3)
                throws IOException {
            return new RecordWriter<Text, BirdStatsWritable>() {
                
                @Override
                public void write(Text k, BirdStatsWritable v) throws IOException {         
                    String rawSQL;
                    if (BirdKey.isQ1(k.toString())){
                        String[] q1Keys = BirdKey.q1Keys(k.toString());
                        rawSQL = upsert(q1Keys[0], q1Keys[1], q1Keys[2], v.getWeighSum(), v.getMaxWingSpan());
                        try{
                            getSQLConnection().prepareStatement(rawSQL).execute();
                        }catch(Exception e){ throw new IOException(e); }
                    }
                    else{
                        String bId = BirdKey.getBid(k.toString());
                        //rawSQL = upsert(bId, v.getDate());
                       
                    }
                }
                private String upsert(String tid, String date, String weather,
                        int weighSum, int maxWingSpan) {
                    String insert = 
                            " INSERT INTO q1andq2 " + " (tid, date, weather, sum_weights, max_ws) " +
                            " VALUES ("+ tid + ","
                                       + date + ","
                                       + weather + ","
                                       + weighSum + ","
                                       + maxWingSpan + ")";
                                       return insert;
                }
                public String upsert(String bid, Date date) {

                    return null;
                }
                @Override
                public void close(Reporter arg0) throws IOException {}
            };
        }

    }

