import java.io.IOException;
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
                mapkey = new Text(BirdKey.makeKey(log.getTowerId(), log.getDate(), log.getWeather()));
                mapvalue = new BirdDataWritable(log.getWeight(), log.getWingSpan());
                output.collect(mapkey, mapvalue);
                mapkey = new Text(BirdKey.makeKey(log.getBirdId()));
                mapvalue = new BirdDataWritable(log.getDate());
                output.collect(mapkey, mapvalue);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, BirdDataWritable, Text, BirdStatsWritable> {

        private int mMaxWs = 0;
        private int mSumWeight = 0;
        private Date mDate = new Date(0);
        @Override
        public void reduce(Text key, Iterator<BirdDataWritable> values, OutputCollector<Text, BirdStatsWritable> output, Reporter reporter) throws IOException {
            
            BirdDataWritable val;
            boolean isQ1 = false;
            while(values.hasNext()) {
                val = values.next();
                isQ1 = BirdKey.isQ1(key.toString());
                if(isQ1) {
                    handleQ1(key, val);
                }
                else {
                    handleQ2(key, val);
                }
            }
            
            if(isQ1) {
                output.collect(key, new BirdStatsWritable(mSumWeight, mMaxWs));
            } else {
                output.collect(key, new BirdStatsWritable(mDate)); 
            }
            
        }
        
        public void handleQ1(Text key, BirdDataWritable val) {
            if (val.getWingSpan() > mMaxWs) {
                mMaxWs = val.getWingSpan();
            }
            mSumWeight += val.getWeight();
        }
        
        public void handleQ2(Text key, BirdDataWritable value)  {
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
        //conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        //conf.setOutputFormat(SQLOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}

