/** 
 * This file contains a map reduce impementation to Rank the top 10 
 * airlines by on-time arrival performance. the airport using the 
 * cleaned EBS data on airline travel.
 * 
 * @author Stephen Dimig
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.*;

/** 
 * This class configures the map reduce job to run under hadoop and
 * contains inner classes that actually implement the map-reduce. In
 * this case there is a seperate map-reuce to average the arrival delay
 * by carrier and one to sort the carriers by ascending order of 
 * arrival delay average so that the best carrier is first.
 */
public class TopCarriers extends Configured implements Tool {

	// Each constant corresponds to a row in the cleaned dataset.
    static final int RECORD_NO = 0;
    static final int FLIGHT_DATE = 1;
    static final int FLIGHT_NUM = 2;
    static final int ORIGIN = 3;
    static final int DEST = 4;
    static final int UNIQUE_CARRIER = 5;
    static final int CARRIER = 6;
    static final int ARRIVAL_TIME = 7;
    static final int ARRIVAL_DELAY = 8;
    static final int ARRIVAL_DELAY_MINUTES = 9;
    static final int DEP_TIME = 10;
    static final int DEP_DELAY = 11;
    static final int DEP_DELAY_MINUTES = 12;
    static final int DAY_OF_WEEK = 13;

    /**
     * Main method
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopCarriers(), args);
        System.exit(res);
    }

    /**
     * This method is invoked to run the map reduce job.
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Carrier Count");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(CarrierCountMap.class);
        jobA.setReducerClass(CarrierCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopCarriers.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Carriers");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(FloatWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(TopCarriersMap.class);
        jobB.setReducerClass(TopCarriersReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopCarriers.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * This method reads the cleaned dataset from HDFS.
     */
    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    /**
     * Inner class to implement a TextArrayWritable type.
     */
    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }

    /**
     * Inner class to implement the map portion of the job. The map
     * part will output a key/value pair with the cariier and the 
     * arrival delay time.
     */
    public static class CarrierCountMap extends Mapper<Object, Text, Text, IntWritable> {
    	/**
    	 * This method sets up the map job.
    	 */
    	@Override
        protected void setup(Context context) throws IOException,InterruptedException {

            Configuration conf = context.getConfiguration();

        }

    	/**
    	 * This method processes a flight and produces the key/value 
    	 * pair.
    	 */
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	// TODO
        	String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, ",");
            int i = 0;
            String carrier = null;

            while (tokenizer.hasMoreTokens()) {
            	String nextToken = tokenizer.nextToken().trim();
            	switch(i)
            	{
            	case UNIQUE_CARRIER:
            		carrier = new String(nextToken);
            		break;
            	case ARRIVAL_DELAY_MINUTES:
            		if(null != carrier)
            		{
            			int delay = Integer.parseInt(nextToken);
            			context.write(new Text(carrier), new IntWritable(delay));
            		}
            		break;
            	default:
            		break;
            	}
            	++i;
            }
	    
        }

    }
    
    /**
	 * This is an inner class that implements the reduce part of the
	 * job to average all of the arrival delays by carrier.
	 */
    public static class CarrierCountReduce extends Reducer<Text, IntWritable, Text, FloatWritable> {
        /**
         * This method averages all of the arrival delays associated
         * with a carrier.
         */
    	@Override
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // TODO
        	int sum = 0;
        	int count = 0;
            for (IntWritable val : values) {
            	sum += val.get();
            	++count;
            }
            if(count > 0)
            {
            	context.write(key, new FloatWritable((float)sum / (float)count));
            }
        }
    }
    
    /**
	 * This is an inner class that implements the map part of a job
	 * that will sort the carriers that have been previously averaged.
	 * It gathers all of the pairs associated with a carrier and 
	 * outputs the top 10 ten to a reduce job for final sorting.
	 */
    public static class TopCarriersMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
        Integer N;
        private TreeSet<Pair<Float, String>> countToCarrierMap = new TreeSet<Pair<Float, String>>();
	
        /**
    	 * This method performs setup for the map job.
    	 */
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
        	Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }	
	
        /**
    	 * This method performs the actual map part of the job.
    	 */
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	Float count = Float.parseFloat(value.toString());
        	String word = key.toString();
        	countToCarrierMap.add(new Pair<Float, String>(count, word));
        	if (countToCarrierMap.size() > N) {
        		countToCarrierMap.remove(countToCarrierMap.last());
        	}
        }
	
        /**
    	 * This method outputs the top 10 that it has seen to the 
    	 * reduce task.
    	 */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	for (Pair<Float, String> item : countToCarrierMap) {
        		String[] strings = {item.second, item.first.toString()};
        		TextArrayWritable val = new
        				TextArrayWritable(strings);
        		context.write(NullWritable.get(), val);
        	}
        }
    }

    /**
	 * This is an inner class that implements the final reduce. The 
	 * totals for all carriers are sent to it for a final sort and
	 * the top 10 are output.
	 */
    public static class TopCarriersReduce extends Reducer<NullWritable, TextArrayWritable, Text, FloatWritable> {
        Integer N;
        private TreeSet<Pair<Float, String>> countToCarrierMap = new TreeSet<Pair<Float, String>>();
	
        /**
         * This method performs setup for the reduce job.
         */
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        /**
         * This method performs the actual reduce.
         */
        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        	for (TextArrayWritable val: values) {
        		Text[] pair= (Text[]) val.toArray();
        		String word = pair[0].toString();
        		Float count = Float.parseFloat(pair[1].toString());
        		countToCarrierMap.add(new Pair<Float, String>(count, word));
        		if (countToCarrierMap.size() > N) {
        			countToCarrierMap.remove(countToCarrierMap.last());
        		}
        	}
        	for (Pair<Float, String> item: countToCarrierMap) {
        		Text word = new Text(item.second);
        		FloatWritable value = new FloatWritable(item.first);
        		context.write(word, value);
        	}
        }
    }
    
}

/**
 * The following class is lifted directly from the CCA course.
 */
class Pair<A extends Comparable<? super A>,
		     B extends Comparable<? super B>>
    implements Comparable<Pair<A, B>> {
    
    public final A first;
    public final B second;
    
    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }
    
    public static <A extends Comparable<? super A>,
	B extends Comparable<? super B>>
	Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }
    
    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }
    
    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
	    && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
// <<< Don't Change