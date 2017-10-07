package hw1;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q1UniqueBuzCategory {
	public static class Map extends Mapper<Object, Text, Text, NullWritable> {
		String city;

		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			city = conf.get("inputCity");
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] columns = value.toString().split("::");

			if (columns[1].contains(city)) {
				columns[2] = columns[2].replace("List(", "").replace(")", "");
				if (columns[2] != "") {
					context.write(new Text(columns[2]), NullWritable.get());
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, NullWritable, Text, NullWritable> {
		Set<String> set = new HashSet<>();

		public void reduce(Text key, Iterable<NullWritable> intermediateValues, Context context)
				throws IOException, InterruptedException {
			String[] allCategories = key.toString().split(",");

			for (String singleCat : allCategories) {
				if (!set.contains(singleCat.trim())) {
					set.add(singleCat.trim());
					context.write(new Text(singleCat.trim()), NullWritable.get());
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: Q1UniqueBuzCategory <in> <out>");
			System.exit(2);
		}

		// set city value
		conf.set("inputCity", "Palo Alto");

		// create a job with name "wordcount"
		Job job = Job.getInstance(conf, "Q1UniqueBuzCategory");
		job.setJarByClass(Q1UniqueBuzCategory.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// set output key type
		job.setOutputKeyClass(Text.class);
		// set map output value class
		job.setMapOutputValueClass(NullWritable.class);
		// set output value type
		job.setOutputValueClass(NullWritable.class);

		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileSystem hdfsFs = FileSystem.get(conf);
		if (hdfsFs.exists(new Path(otherArgs[1]))) {
			hdfsFs.delete(new Path(otherArgs[1]), true);
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
