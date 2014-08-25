package com.teradata.compaction.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MergeParquetFilesMapper extends
		Mapper<String, NullWritable, NullWritable, NullWritable> {

	public void map(String directorypath) throws IOException {
		ParquetMergeSingle pms = new ParquetMergeSingle();
		pms.runmergejob(directorypath);
	}
}
