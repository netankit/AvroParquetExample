package com.teradata.compaction.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MergeParquetFilesMapper extends MapReduceBase implements
		Mapper<String, NullWritable, NullWritable, NullWritable> {

	@Override
	public void configure(JobConf arg0) {
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public void map(String directorypath, NullWritable arg1,
			OutputCollector<NullWritable, NullWritable> arg2, Reporter arg3)
			throws IOException {

		ParquetMergeSingle.runmergejob(directorypath);
	}
}
