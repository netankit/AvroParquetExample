package com.teradata.compaction.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import parquet.example.data.Group;

public class MergeParquetFilesWriteMapper extends
		Mapper<Void, Group, LongWritable, Text> {
	@Override
	protected void map(Void key, Group value,
			Mapper<Void, Group, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// context.write(new LongWritable(value.getInteger("line", 0)), new
		// Text(
		// value.getString("content", 0)));
	}

}
