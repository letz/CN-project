package com.ist.hadoop.BirdWatch;

import java.io.IOException;
import java.text.SimpleDateFormat;
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
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;


public class DynamoDBOutputFormat implements OutputFormat<Text, BirdStatsWritable> {
        /**
         * Access credentials (there credentials are public).
         */
        private static String USER = "";
        private static String URL = "dynamodb.us-west-2.amazonaws.com";
        private static String PASS = "";
        private static String TABLE_1 = "query1";
        private static String TABLE_2 = "query2";
        private static String TABLE_3 = "query3";
        private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
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
                   String query;
                   Map<String, AttributeValueUpdate > item ;
                   HashMap<String, AttributeValue> key;

                   if (BirdKey.isQ1(k.toString())){
                       String[] q1Keys = BirdKey.q1Keys(k.toString());
                       query = TABLE_1;
                       //key -> date:tower_id
                       System.out.println("----------->>>>"+q1Keys[0] +":"+ q1Keys[1] +":"+ v.getMaxWingSpan());
                       item = newItem("max_ws",v.getMaxWingSpan()+"");
                       key = new HashMap<String, AttributeValue>();
                       key.put("date", new AttributeValue(q1Keys[0]));
                       key.put("tower_id",new AttributeValue(q1Keys[1]));
                    }
                   else if (BirdKey.isQ2(k.toString())){
                       String[] q2Keys = BirdKey.q1Keys(k.toString());
                       query = TABLE_2;
                       //key -> tower_id:date
                       System.out.println("----------->>>>"+q2Keys[0] +":"+ q2Keys[1] +":"+ v.getWeighSum());
                       item = newItem("weight_sum",v.getWeighSum()+"");
                       key = new HashMap<String, AttributeValue>();
                       key.put("date", new AttributeValue(q2Keys[0]));
                       key.put("tower_id",new AttributeValue(q2Keys[1]));
                    }
                    else {
                        //key -> b_id
                        String q3Key = BirdKey.q3Key(k.toString());
                        query = TABLE_3;
                        System.out.println("----------->>>>"+q3Key+":"+ formatter.format(v.getDate()));
                        item = null;
                        key = new HashMap<String, AttributeValue>();
                        key.put("b_id", new AttributeValue(q3Key));
                        key.put("date",new AttributeValue(formatter.format(v.getDate())));
                    }
                   dynamoInsert(query, key,item);
                }

                /**
                 * Temporary empty implementation.
                 * It could be useful to store a batch of sql updates in order
                 * to commit all of them in just one step.
                 */
                @Override
                public void close(Reporter arg0) throws IOException {}

                public void dynamoInsert(String table, Map<String, AttributeValue> key,Map<String, AttributeValueUpdate> item){
                    try{
                        UpdateItemRequest updateItemRequest = new UpdateItemRequest().withTableName(table).withKey(key).withAttributeUpdates(item);
                        UpdateItemResult putItemResult = conn.updateItem(updateItemRequest);
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

        private static Map<String, AttributeValueUpdate> newItem(String key, String value) {
            Map<String, AttributeValueUpdate> item = new HashMap<String, AttributeValueUpdate>();
            item.put(key,new AttributeValueUpdate().withValue(new AttributeValue(value)));
            return item;
        }


    }
