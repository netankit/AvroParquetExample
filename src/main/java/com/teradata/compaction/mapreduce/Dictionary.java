package com.teradata.compaction.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Dictionary {
	public static class WordMapper extends
			Mapper<LongWritable, LongWritable, LongWritable, Text> {
		private final Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " ");
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(key, word);
			}
		}
	}

	public static class AllTranslationsReducer extends
			Reducer<LongWritable, Text, LongWritable, Text> {
		private final Text result = new Text();

		@Override
		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			String translations = "";
			for (Text val : values) {
				translations += " | " + val.toString();
			}
			result.set(translations);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "dictionary");
		job.setJarByClass(Dictionary.class);
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(AllTranslationsReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(
				"/home/ankit/workspace/AvroExample/sampleinput/"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}