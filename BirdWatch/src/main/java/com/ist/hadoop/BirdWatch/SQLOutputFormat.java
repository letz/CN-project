package com.ist.hadoop.BirdWatch;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;

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
        private static String USER = "ist168211";
        private static String URL = "jdbc:postgresql://db.ist.utl.pt:5432/ist168211";
        private static String PASS = System.getenv("PG_SIGMA");
        private static String TABLE1 = "q1";
        private static String TABLE2 = "q2";
        private static String TABLE3 = "q3";
        private static Connection conn = null;
        private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

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
                    String rawSQL = null;
                    if (BirdKey.isQ1(k.toString())){
                        String[] q1Keys = BirdKey.q1Keys(k.toString());
                        rawSQL = upsert(TABLE1, q1Keys[1], q1Keys[0], 0, v.getMaxWingSpan());
                    } else if (BirdKey.isQ2(k.toString())){
                        String[] q2Keys = BirdKey.q1Keys(k.toString());
                        rawSQL = upsert(TABLE2, q2Keys[0], q2Keys[1], v.getWeighSum(),0);
                    } else{
                        String q3Key = BirdKey.q3Key(k.toString());
                        rawSQL = upsert( TABLE3, q3Key, formatter.format(v.getDate()));
                    }
                    try{
                        getSQLConnection().prepareStatement(rawSQL).execute();
                    }catch(Exception e){ throw new IOException(e); }
                }
                private String upsert(String table,String tid, String date,
                        int weightSum, int maxWingSpan) {
                    String updateValue = "";
                    String insert = "";
                    if (weightSum == 0){
                        updateValue = " max_ws="+"\'"+ maxWingSpan + "\' ";
                        insert =
                                " INSERT INTO "+ table + " (t_id, log_date,max_ws) " +
                                " SELECT " + "\'" + tid +"\',"
                                           + "\'" + date + "\',"
                                           + "\'"+ maxWingSpan +"\'"+
                                " WHERE NOT EXISTS (SELECT 1 FROM "+ table + " WHERE log_date=\'"+ date + "\');";
                    } else if(maxWingSpan == 0){
                        updateValue = " sum_weight="+"\'"+ weightSum + "\' ";
                        insert =
                                " INSERT INTO "+ table + " (t_id, log_date, sum_weight) " +
                                " SELECT " + "\'" + tid +"\',"
                                           + "\'" + date + "\',"
                                           + "\'"+ weightSum +"\'"+
                                " WHERE NOT EXISTS (SELECT 1 FROM "+ table + " WHERE log_date=" + "\'" + date + "\' and t_id=\'"+ tid + "\');";
                    }
                    String update =
                            " UPDATE "+ table + " SET " + updateValue +
                            " WHERE log_date=" + "\'" + date + "\'" + " and t_id=" + "\'"+ tid + "\';";

                    return update + " " + insert;
                }
                private String upsert(String table,String bid, String date) {
                    String updateValue = " last_seen_at="+"\'"+ date + "\' ";
                    String update =
                            " UPDATE "+ table + " SET " + updateValue +
                            " WHERE b_id=" + "\'"+ bid + "\';";

                    String insert =
                            " INSERT INTO "+ table + " (b_id, last_seen_at) " +
                            " SELECT " + "\'" + bid +"\',"
                                       + "\'" + date + "\'" +
                            " WHERE NOT EXISTS (SELECT 1 FROM "+ table + " WHERE b_id=\'"+ bid + "\');";
                    return update + " " + insert;
                }

                @Override
                public void close(Reporter arg0) throws IOException {}
            };
        }

    }

