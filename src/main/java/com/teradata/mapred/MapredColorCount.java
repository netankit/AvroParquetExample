package com.teradata.mapred;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import example.avro.User;

public class MapredColorCount extends Configured implements Tool {

	public static class ColorCountMapper extends
			AvroMapper<User, Pair<CharSequence, Integer>> {
		@Override
		public void map(User user,
				AvroCollector<Pair<CharSequence, Integer>> collector,
				Reporter reporter) throws IOException {
			CharSequence color = user.getFavoriteColor();
			// We need this check because the User.favorite_color field has type
			// ["string", "null"]
			if (color == null) {
				color = "none";
			}
			collector.collect(new Pair<CharSequence, Integer>(color, 1));
		}
	}

	public static class ColorCountReducer extends
			AvroReducer<CharSequence, Integer, Pair<CharSequence, Integer>> {
		@Override
		public void reduce(CharSequence key, Iterable<Integer> values,
				AvroCollector<Pair<CharSequence, Integer>> collector,
				Reporter reporter) throws IOException {
			int sum = 0;
			for (Integer value : values) {
				sum += value;
			}
			collector.collect(new Pair<CharSequence, Integer>(key, sum));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("Usage: MapredColorCount <input path> <output path>");
			return -1;
		}

		JobConf conf = new JobConf(getConf(), MapredColorCount.class);
		conf.setJobName("colorcount");

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		AvroJob.setMapperClass(conf, ColorCountMapper.class);
		AvroJob.setReducerClass(conf, ColorCountReducer.class);

		// Note that AvroJob.setInputSchema and AvroJob.setOutputSchema set
		// relevant config options such as input/output format, map output
		// classes, and output key class.
		AvroJob.setInputSchema(conf, User.getClassSchema());
		AvroJob.setOutputSchema(
				conf,
				Pair.getPairSchema(Schema.create(Type.STRING),
						Schema.create(Type.INT)));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MapredColorCount(),
				args);
		System.exit(res);
	}
}