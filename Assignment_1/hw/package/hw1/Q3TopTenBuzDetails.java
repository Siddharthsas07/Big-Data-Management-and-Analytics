package hw1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q3TopTenBuzDetails {

	public static class Map_Q3_Reviews extends Mapper<Object, Text, Text, FloatWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] columns = value.toString().split("::");
			float rating = Float.parseFloat(columns[3]);
			context.write(new Text(columns[2]), new FloatWritable(rating));
		}
	}

	public static class Map_Q3_Buz extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] columns = value.toString().split("::");
			String buz_id = columns[0];
			String buz_info = "Det:" + "\t" + columns[1] + "\t" + columns[2];
			context.write(new Text(buz_id), new Text(buz_info));
		}
	}

	public static class Map_Q3_Top10 extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] columns = value.toString().split("\t");
			String rating = columns[1];
			String buz_id = columns[0];
			context.write(new Text(buz_id), new Text(rating));
		}
	}

	public static class Reduc_Q3_Reviews extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		HashMap<String, Float> map = new HashMap<String, Float>();

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

			Map<String, Float> sortedOutputs = Q2BestRatedBuz10.sortByValues(map);
			sortedOutputs.putAll(map);

			int i = 0;
			for (Map.Entry<String, Float> entity : sortedOutputs.entrySet()) {
				context.write(new Text(entity.getKey()), new FloatWritable(entity.getValue()));
				if (++i == 10)
					break;
			}
		}
	}

	public static class Reduce_Join extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text buz_id, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String rating = "";
			String informations = "";
			boolean flag = false;
			for (Text value : values) {
				String data = value.toString();
				if (data.contains("Det:")) {
					data = data.replace("Det:", "");
					informations = data;
				} else {
					rating = data;
					flag = true;
				}
			}

			if (!flag) return;
			context.write(new Text(buz_id), new Text(rating + "\t" + informations));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err.println("Usage: Q3TopTenBuzDetails <input1> <input2> <intermideatePath> <output>");
			System.exit(2);
		}
		
		Job job1 = Job.getInstance(conf, "Q2BestRatedBuz10");
		job1.setJarByClass(Q2BestRatedBuz10.class);

		job1.setMapperClass(Map_Q3_Reviews.class);
		job1.setReducerClass(Reduc_Q3_Reviews.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(FloatWritable.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(FloatWritable.class);
		job1.setNumReduceTasks(1);

		Path inputReviewPath = new Path(otherArgs[0]);
		Path inputDetailsPath = new Path(otherArgs[1]);
		Path intermediatePath = new Path(otherArgs[2]);
		Path outputPath = new Path(otherArgs[3]);

		FileInputFormat.addInputPath(job1, inputReviewPath);
		FileOutputFormat.setOutputPath(job1, intermediatePath);
		job1.waitForCompletion(true);

		Job job2 = Job.getInstance(conf, "Q3TopTenBuzDetails");
		job2.setJarByClass(Q3TopTenBuzDetails.class);
		MultipleInputs.addInputPath(job2, inputDetailsPath, TextInputFormat.class, Map_Q3_Buz.class);
		MultipleInputs.addInputPath(job2, intermediatePath, TextInputFormat.class, Map_Q3_Top10.class);

		job2.setReducerClass(Reduce_Join.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.setMinInputSplitSize(job2, 500000000);

		FileSystem hdfsFS = FileSystem.get(conf);
		if (hdfsFS.exists(outputPath)) {
			hdfsFS.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job2, outputPath);

		job2.waitForCompletion(true);

		hdfsFS.delete(intermediatePath, true);

	}
}
