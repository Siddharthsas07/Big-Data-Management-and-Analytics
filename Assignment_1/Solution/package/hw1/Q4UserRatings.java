package hw1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q4UserRatings {

	public static class Map_Q4 extends Mapper<Object, Text, Text, Text> {
		Set<String> set = new HashSet<String>();
		String input;

		public void setup(Context context) throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			input = conf.get("input");
			
			try {
				URI[] locations = context.getCacheFiles();
				URI myUri = locations[0];
				BufferedReader br = new BufferedReader(new FileReader(new File(myUri.getPath()).getName()));
				String inputString = br.readLine();
				while (inputString != null) {
					if (inputString.contains(input)) {
						set.add(inputString.split("::")[0]);
					}
					inputString = br.readLine();
				}
				br.close();
			} catch (Exception e) {
				System.out.println(e);
			}
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] columns = value.toString().split("::");
			String rating = columns[3];
			String buz_id = columns[2];
			String user_id = columns[1];
			if (set.contains(buz_id)) {
				context.write(new Text(user_id.toString()), new Text(rating));
			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: Q4UserRatings <in1> <in2> <out>");
			System.exit(2);
		}
		conf.set("input", "Stanford,");
		
		Job job = Job.getInstance(conf, "Q4UserRatings");
		job.setJarByClass(Q4UserRatings.class);

		job.setMapperClass(Map_Q4.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		Path input = new Path(otherArgs[0]);
		Path cacheFilePath = new Path(otherArgs[1]);
		Path output = new Path(otherArgs[2]);

		FileInputFormat.addInputPath(job, input);

		job.addCacheFile(cacheFilePath.toUri());

		FileSystem hdfsFS = FileSystem.get(conf);
		if (hdfsFS.exists(output)) {
			hdfsFS.delete(output, true);
		}
		FileOutputFormat.setOutputPath(job, output);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
