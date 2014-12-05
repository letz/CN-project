package com.ist.hadoop.BirdWatch;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class BirdWatchMapReducer {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, BirdDataWritable> {

        private Log log;

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, BirdDataWritable> output, Reporter reporter) throws IOException {
            StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("\\s+",""));
            BirdDataWritable mapvalue;
            Text mapkey;
            while (itr.hasMoreTokens()) {
                log = new Log(itr.nextToken());
                //Q1
                if(log.getWeather() == 2){
                    mapkey = new Text(BirdKey.makeKey(log.getDate(), "1"));
                    mapvalue = new BirdDataWritable(log.getTowerId(), log.getWingSpan());
                    output.collect(mapkey, mapvalue);
                }
                //Q2
                mapkey = new Text(BirdKey.makeKey(log.getTowerId(), log.getDate(), "2"));
                mapvalue = new BirdDataWritable(log.getWeight());
                output.collect(mapkey, mapvalue);
                //Q3
                mapkey = new Text(BirdKey.makeKey(log.getBirdId(),"3"));
                mapvalue = new BirdDataWritable(log.getDate());
                output.collect(mapkey, mapvalue);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, BirdDataWritable, Text, BirdStatsWritable> {

        private int mMaxWs;
        private int mSumWeight;
        private Date mDate;
        private String mTid;
        @Override
        public void reduce(Text key, Iterator<BirdDataWritable> values, OutputCollector<Text, BirdStatsWritable> output, Reporter reporter) throws IOException {

            BirdDataWritable val;
            boolean isQ1 = false;
            boolean isQ2 = false;
            mMaxWs = 0;
            mSumWeight = 0;
            mDate = new Date(0);

            while(values.hasNext()) {
                val = values.next();
                isQ1 = BirdKey.isQ1(key.toString());
                isQ2 = BirdKey.isQ2(key.toString());
                if(isQ1) {
                    handleQ1(key, val);

                }
                else if(isQ2) {

                    handleQ2(key, val);
                }
                else {

                    handleQ3(key, val);
                }
            }

            if(isQ1) {
                output.collect(new Text(key + "," + mTid), new BirdStatsWritable(mMaxWs, "Q1"));
            }
            else if(isQ2) {
                output.collect(key, new BirdStatsWritable(mSumWeight, "Q2"));
            }
            else {
                output.collect(key, new BirdStatsWritable(mDate));
            }

        }

        public void handleQ1(Text key, BirdDataWritable val) {
            if (val.getWingSpan() > mMaxWs) {
                mMaxWs = val.getWingSpan();
                mTid = val.getTower();
            }
        }

        public void handleQ2(Text key, BirdDataWritable val)  {
            mSumWeight += val.getWeight();
        }

        public void handleQ3(Text key, BirdDataWritable value)  {
            Date dateval = value.getRealDate();
            if (mDate.getTime() < dateval.getTime()) {
                mDate = dateval;
            }
        }
    }

    public static class Combiner extends MapReduceBase implements Reducer<Text, BirdDataWritable, Text, BirdDataWritable> {

        private int mMaxWs;
        private int mSumWeight;
        private Date mDate;
        private String mTid;
        private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

        @Override
        public void reduce(Text key, Iterator<BirdDataWritable> values,
                OutputCollector<Text, BirdDataWritable> output,
                Reporter reporter) throws IOException  {

            BirdDataWritable val;
            boolean isQ1 = false;
            boolean isQ2 = false;
            mMaxWs = 0;
            mSumWeight = 0;
            mDate = new Date(0);
            BirdDataWritable mapvalue;
            Text mapkey;

            while(values.hasNext()) {
                val = values.next();
                isQ1 = BirdKey.isQ1(key.toString());
                isQ2 = BirdKey.isQ2(key.toString());
                if(isQ1) {
                    handleQ1(key, val);

                }
                else if(isQ2) {

                    handleQ2(key, val);
                }
                else {

                    handleQ3(key, val);
                }
            }

            if(isQ1) {
                mapkey = new Text(BirdKey.makeKey(key.toString(), "1"));
                mapvalue = new BirdDataWritable(mTid, mMaxWs);
            }
            else if(isQ2) {
                mapkey = new Text(BirdKey.makeKey(mTid, key.toString(), "2"));
                mapvalue = new BirdDataWritable(mSumWeight);

            }
            else {
                //Q3
                mapkey = new Text(BirdKey.makeKey(key.toString(),"3"));
                mapvalue = new BirdDataWritable( mDate.toString());
            }
            output.collect(mapkey, mapvalue);

        }

        public void handleQ1(Text key, BirdDataWritable val) {
            if (val.getWingSpan() > mMaxWs) {
                mMaxWs = val.getWingSpan();
                mTid = val.getTower();
            }
        }

        public void handleQ2(Text key, BirdDataWritable val)  {
            mSumWeight += val.getWeight();
        }

        public void handleQ3(Text key, BirdDataWritable value)  {
            Date dateval = value.getRealDate();
            if (mDate.getTime() < dateval.getTime()) {
                mDate = dateval;
            }
        }
    }



    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(BirdWatchMapReducer.class);
        conf.setJobName(BirdWatchMapReducer.class.getName());

        //Mapper Outputs
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(BirdDataWritable.class);

        //Reducer Outputs
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(BirdStatsWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Combiner.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        //conf.setOutputFormat(DynamoDBOutputFormat.class);
        //conf.setOutputFormat(SQLOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}

