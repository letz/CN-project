package com.ist.hadoop.BirdWatch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;


public class DynamoDBOutputFormat implements OutputFormat<Text, BirdStatsWritable> {
        /**
         * Access credentials (there credentials are public).
         */
        private static String USER = "nice_try";
        private static String URL = "dynamodb.us-west-2.amazonaws.com";
        private static String PASS = "nice_try";
        private static AmazonDynamoDBClient conn = null;        
        
        /**
         * Helper to get a connection.
         * @return - a DynamoDB connection.
         * @throws IOException - if something goes wrong creating the connection.
         */
        public static AmazonDynamoDBClient getDynamoDBConnection() throws IOException {
            if(conn == null) {
                BasicAWSCredentials credentials = new BasicAWSCredentials(USER,PASS);
                conn = new AmazonDynamoDBClient(credentials);
                conn.setEndpoint(URL);
            }
            return conn;

        }

        @Override
        public void checkOutputSpecs(FileSystem arg0, JobConf arg1)
                throws IOException {
            // TODO Auto-generated method stub

        }

        @Override
        public RecordWriter<Text, BirdStatsWritable> getRecordWriter(
                FileSystem arg0, JobConf arg1, String arg2, Progressable arg3)
                throws IOException {
            return new RecordWriter<Text, BirdStatsWritable>() {

                @Override
                public void write(Text k, BirdStatsWritable v)
                        throws IOException {
                   getDynamoDBConnection();
                   if (BirdKey.isQ1(k.toString())){
                       String[] q1Keys = BirdKey.q1Keys(k.toString());
                       System.out.println("----------->>>>"+q1Keys[0] +":"+ q1Keys[1] +":"+ v.getMaxWingSpan());
                       Map<String, AttributeValue> item = newItem(q1Keys[0], q1Keys[1], v.getMaxWingSpan());
                       dynamoInsert("query1", item);
                    }
                    else {
                    }
                }

                /**
                 * Temporary empty implementation.
                 * It could be useful to store a batch of sql updates in order
                 * to commit all of them in just one step.
                 */
                @Override
                public void close(Reporter arg0) throws IOException {}

                public void dynamoInsert(String table,Map<String, AttributeValue> item){
                    try{
                        PutItemRequest putItemRequest = new PutItemRequest(table, item);
                        PutItemResult putItemResult = conn.putItem(putItemRequest);
                        System.out.println("Result: " + putItemResult);

                    } catch (AmazonServiceException ase) {

                        System.out.println("Caught an AmazonServiceException, which means your request made it "
                                + "to AWS, but was rejected with an error response for some reason.");
                        System.out.println("Error Message:    " + ase.getMessage());
                        System.out.println("HTTP Status Code: " + ase.getStatusCode());
                        System.out.println("AWS Error Code:   " + ase.getErrorCode());
                        System.out.println("Error Type:       " + ase.getErrorType());
                        System.out.println("Request ID:       " + ase.getRequestId());

                    } catch (AmazonClientException ace) {
                        System.out.println("Caught an AmazonClientException, which means the client encountered "
                                + "a serious internal problem while trying to communicate with AWS, "
                                + "such as not being able to access the network.");
                        System.out.println("Error Message: " + ace.getMessage());
                    }

                }
            };
        }

        private static Map<String, AttributeValue> newItem(String date, String tid,int maxWingspan) {
            Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
            item.put("date", new AttributeValue(date));
            item.put("tower_id", new AttributeValue(tid));
            item.put("max_ws",new AttributeValue().withN(Integer.toString(maxWingspan)));
            return item;
        }


    }
