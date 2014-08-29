package com.teradata.compaction.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroParquetOutputFormat;
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
	private static Schema fileSchema;

	public static class SampleParquetMapper extends
			Mapper<LongWritable, IndexedRecord, LongWritable, Text> {
		@Override
		public void map(LongWritable key, IndexedRecord values, Context context)
				throws IOException, InterruptedException {
			Text mapvalues = new Text(values.toString());
			// Gives: NullPointerException
			context.write(new LongWritable(2), mapvalues);
			// Gives: java.lang.ClassCastException:
			// org.apache.hadoop.io.LongWritable
			// cannot be cast to java.lang.Void
			// context.write(null, mapvalues);

		}
	}

	public static class SampleParquetReducer extends
			Reducer<LongWritable, Text, Void, Text> {

		public void reduce(LongWritable key, Text values, Context context)
				throws IOException, InterruptedException {
			context.write(null, values);

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "MergeParquet");
		final Path inputPath = new Path(
				"/home/ankit/workspace/AvroExample/newtest/");
		final Path out = new Path("output_newtest");
		Schema schemaParquetFile = getBaseSchema(inputPath, conf);
		job.setJarByClass(MergeParquetFilesMR.class);
		job.setMapperClass(SampleParquetMapper.class);
		job.setReducerClass(SampleParquetReducer.class);
		// job.setOutputKeyClass(NullWritable.class);
		// job.setOutputValueClass(IndexedRecord.class);
		job.setInputFormatClass(AvroParquetInputFormat.class);
		job.setOutputFormatClass(AvroParquetOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		// Set Schema for various phases
		// AvroJob.setInputValueSchema(job, schemaParquetFile);
		// AvroJob.setInputKeySchema(job, null);
		// AvroJob.setMapOutputKeySchema(job, null);
		// AvroJob.setMapOutputValueSchema(job, schemaParquetFile);
		// AvroJob.setOutputKeySchema(job, null);
		// AvroJob.setOutputValueSchema(job, schemaParquetFile);
		AvroParquetOutputFormat.setSchema(job, schemaParquetFile);
		FileInputFormat.addInputPath(job, inputPath);
		AvroParquetOutputFormat.setOutputPath(job, out);
		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	private static Schema getBaseSchema(final Path pathToParqetFiles,
			Configuration conf) throws IOException {
		fileSchema = null;
		FileSystem fsystem = pathToParqetFiles.getFileSystem(conf);
		FileStatus fstatus = fsystem.getFileStatus(pathToParqetFiles);

		if (fstatus.isDir()) {
			FileStatus[] files = fsystem.listStatus(fstatus.getPath());
			for (FileStatus file : files) {
				if (file.isDir()) {
					continue;
				} else {
					ParquetReader<GenericRecord> reader_schema = new AvroParquetReader<GenericRecord>(
							file.getPath());
					GenericRecord tmp_schema = reader_schema.read();
					fileSchema = tmp_schema.getSchema();
					reader_schema.close();
					break;
				}
			}
		}
		System.out.println(fileSchema.toString());
		return fileSchema;
	}
}
