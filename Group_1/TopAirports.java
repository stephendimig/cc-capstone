/** 
 * This file contains a map reduce impementation to Rank the top 10 
 * most popular airports by numbers of flights to/from the airport
 * using the cleaned EBS data on airline travel.
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
 * this case there is a seperate map-reuce to sum the arrivals and
 * departures at an airport and one to sort the airports by the total
 * number of arrivals and departures.
 */
public class TopAirports extends Configured implements Tool {

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
        int res = ToolRunner.run(new Configuration(), new TopAirports(), args);
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

        Job jobA = Job.getInstance(conf, "Airport Count");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(AirportCountMap.class);
        jobA.setReducerClass(AirportCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopAirports.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Airports");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(TopAirportsMap.class);
        jobB.setReducerClass(TopAirportsReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopAirports.class);
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
     * part will output a separate key/value pair for the origin and
     * destination airports in a flight. The value for each pair will
     * be 1.
     */
    public static class AirportCountMap extends Mapper<Object, Text, Text, IntWritable> {
        
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
        	String line = value.toString();
            
        	// Tokenize by the comma in the csv.
        	StringTokenizer tokenizer = new StringTokenizer(line, ",");
            int i = 0;
            
            while (tokenizer.hasMoreTokens()) {
                String nextToken = tokenizer.nextToken().trim();
                
                // A seperate key/value pair will be produced for the
                // origin and dest. The value will be 1.
                switch(i)
                {
                case ORIGIN:
                	// Key/value pair for the origin
                	context.write(new Text(nextToken), new IntWritable(1));
                	break;
                case DEST:
                	// Key/value pair for the destination.
                	context.write(new Text(nextToken), new IntWritable(1));
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
	 * job to count all arrivals and departures to an airport.
	 */
    public static class AirportCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    	
    	@Override
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
    
    /**
	 * This is an inner class that implements the map part of a job
	 * that will sort the airports that have been previously counted.
	 * It gathers all of the pairs associated with an airport and 
	 * outputs the top 10 ten to a reduce job for final sorting.
	 */
    public static class TopAirportsMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
    	Integer N;
    	private TreeSet<Pair<Integer, String>> countToAirportMap = new TreeSet<Pair<Integer, String>>();
	
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
    		Integer count = Integer.parseInt(value.toString());
    		String word = key.toString();
    		countToAirportMap.add(new Pair<Integer, String>(count, word));
    		if (countToAirportMap.size() > N) {
    			countToAirportMap.remove(countToAirportMap.first());
    		}
        }
	
    	/**
    	 * This method outputs the top 10 that it has seen to the 
    	 * reduce task.
    	 */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	for (Pair<Integer, String> item : countToAirportMap) {
        		String[] strings = {item.second, item.first.toString()};
        		TextArrayWritable val = new
        				TextArrayWritable(strings);
        		context.write(NullWritable.get(), val);
        	}
        }
    }

    /**
	 * This is an inner class that implements the final reduce. The 
	 * totals for all airports are sent to it for a final sort and
	 * the top 10 are output.
	 */
    public static class TopAirportsReduce extends Reducer<NullWritable, TextArrayWritable, Text, IntWritable> {
        Integer N;
        private TreeSet<Pair<Integer, String>> countToAirportMap = new TreeSet<Pair<Integer, String>>(Collections.reverseOrder());
	
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
        		Integer count = Integer.parseInt(pair[1].toString());
        		countToAirportMap.add(new Pair<Integer, String>(count, word));
        		if (countToAirportMap.size() > N) {
        			countToAirportMap.remove(countToAirportMap.first());
        		}
        	}
        	for (Pair<Integer, String> item: countToAirportMap) {
        		Text word = new Text(item.second);
        		IntWritable value = new IntWritable(item.first);
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
