package hw1;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q2BestRatedBuz10 {

	public static class Map_Q2 extends Mapper<Object, Text, Text, FloatWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] columns = value.toString().split("::");
			float myRating = Float.parseFloat(columns[3]);
			context.write(new Text(columns[2]), new FloatWritable(myRating));
		}
	}

	public static class Reducer_Q2 extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		Map<String, Float> map = new HashMap<String, Float>();

		public void reduce(Text buz_id, Iterable<FloatWritable> ratings, Context context)
				throws IOException, InterruptedException {
			int frequency = 0;
			float sum = 0;
			for (FloatWritable value : ratings) {
				sum += value.get();
				frequency++;
			}
			float avg = sum / frequency;
			map.put(buz_id.toString(), avg);
		}

		public void cleanup(Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {

			Map<String, Float> sorted = sortByValues(map);

			int i = 0;
			for (Map.Entry<String, Float> entity : sorted.entrySet()) {

				if (++i == 10)
					break;
				context.write(new Text(entity.getKey()), new FloatWritable(entity.getValue()));

			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Q2BestRatedBuz10");
		job.setJarByClass(Q2BestRatedBuz10.class);
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Q2BestRatedBuz10 <in> <out>");
			System.exit(2);
		}

		job.setMapperClass(Map_Q2.class);
		job.setReducerClass(Reducer_Q2.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

		FileSystem hdfsFS = FileSystem.get(conf);
		if (hdfsFS.exists(new Path(otherArgs[1]))) {
			hdfsFS.delete(new Path(otherArgs[1]), true);
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static <K, V extends Comparable<V>> java.util.Map<K, V> sortByValues(final java.util.Map<K, V> map) {
		Comparator<K> valueComparator = new Comparator<K>() {
			public int compare(K k1, K k2) {
				V valueA = (V) map.get(k1);
				V valueB = (V) map.get(k2);

				int compare = valueB.compareTo(valueA);

				if (compare == 0)
					return 1;
				return compare;
			}
		};
		java.util.Map<K, V> sortedByValues = new TreeMap<K, V>(valueComparator);
		sortedByValues.putAll(map);
		return sortedByValues;
	}
}
