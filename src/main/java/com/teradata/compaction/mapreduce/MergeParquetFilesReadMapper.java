package com.teradata.compaction.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.util.ContextUtil;

public class MergeParquetFilesReadMapper extends
		Mapper<LongWritable, Text, Void, Group> {
	private SimpleGroupFactory factory;

	@Override
	protected void setup(
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Void, Group>.Context context)
			throws java.io.IOException, InterruptedException {
		factory = new SimpleGroupFactory(
				GroupWriteSupport.getSchema(ContextUtil
						.getConfiguration(context)));
	}

	;

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Void, Group>.Context context)
			throws java.io.IOException, InterruptedException {
		// Group group = factory.newGroup().append("line", (int) key.get())
		// .append("content", value.toString());
		// context.write(null, group);
	}
}
