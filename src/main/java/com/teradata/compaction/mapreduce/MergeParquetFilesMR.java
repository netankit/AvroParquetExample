package com.teradata.compaction.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import parquet.avro.AvroParquetReader;
import parquet.hadoop.ParquetReader;

/**
 * This script can be used to merge all Parquet files present in a given
 * directory, corresponding to a given schema, using map-reduce.
 * 
 * @author ankit
 *
 */
public class MergeParquetFilesMR {
	public static class SampleParquetMapper extends
			Mapper<LongWritable, LongWritable, LongWritable, Text> {

		public void map(NullWritable key, IndexedRecord values, Context context)
				throws IOException, InterruptedException {

		}
	}

	public static class SampleParquetReducer extends
			Reducer<LongWritable, Text, LongWritable, Text> {

		public void reduce(NullWritable key, IndexedRecord values,
				Context context) throws IOException, InterruptedException {

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "MergeParquet");

		final Path inputPath = new Path(
				"/home/ankit/workspace/AvroExample/userprofiles/");

		Schema schemaParquetFile = getBaseSchema(inputPath, conf);
		job.setJarByClass(MergeParquetFilesMR.class);

		job.setMapperClass(SampleParquetMapper.class);
		job.setReducerClass(SampleParquetReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(IndexedRecord.class);

		// Set Schema for various phases
		AvroJob.setInputValueSchema(job, schemaParquetFile);
		AvroJob.setInputKeySchema(job, null);
		AvroJob.setMapOutputKeySchema(job, null);
		AvroJob.setMapOutputValueSchema(job, schemaParquetFile);
		AvroJob.setOutputKeySchema(job, null);
		AvroJob.setOutputValueSchema(job, schemaParquetFile);
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path("output_userprofiles"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	private static Schema getBaseSchema(final Path pathToParqetFiles,
			Configuration conf) throws IOException {
		Schema fileSchema = null;
		FileSystem fsys = pathToParqetFiles.getFileSystem(conf);
		FileStatus[] fst = fsys.listStatus(pathToParqetFiles);

		ParquetReader<GenericRecord> reader_schema = new AvroParquetReader<GenericRecord>(
				fst[3].getPath());
		GenericRecord tmp_schema = reader_schema.read();
		fileSchema = tmp_schema.getSchema();
		reader_schema.close();
		return fileSchema;
	}
}
