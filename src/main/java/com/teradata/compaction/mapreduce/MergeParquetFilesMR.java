package com.teradata.compaction.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
			Mapper<Void, IndexedRecord, NullWritable, AvroValue<IndexedRecord>> {
		@Override
		public void map(Void key, IndexedRecord values, Context context)
				throws IOException, InterruptedException {

			context.write(NullWritable.get(), new AvroValue<IndexedRecord>(
					values));

		}
	}

	public static class SampleParquetReducer
			extends
			Reducer<NullWritable, AvroValue<IndexedRecord>, Void, IndexedRecord> {

		@Override
		public void reduce(NullWritable key,
				Iterable<AvroValue<IndexedRecord>> records, Context context)
				throws IOException, InterruptedException {
			for (AvroValue<IndexedRecord> avroValue : records) {
				context.write(null, avroValue.datum());
			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "MergeParquet");

		if (args.length != 2) {
			System.err
					.println("Usage: java -jar MergeParquetFilesMR path_to_input_folder path_to_output_folder ");
			System.exit(0);
		}

		final Path inputPath = new Path(args[0]);
		final Path out = new Path(args[1]);

		Schema schemaParquetFile = getBaseSchema(inputPath, conf);
		job.setJarByClass(MergeParquetFilesMR.class);
		job.setMapperClass(SampleParquetMapper.class);
		job.setReducerClass(SampleParquetReducer.class);
		job.setInputFormatClass(AvroParquetInputFormat.class);
		job.setOutputFormatClass(AvroParquetOutputFormat.class);
		job.setMapOutputKeyClass(NullWritable.class);

		AvroJob.setMapOutputValueSchema(job, schemaParquetFile);
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
					if (file.getPath().toString().toUpperCase().toLowerCase()
							.endsWith(".parquet")) {
						ParquetReader<GenericRecord> reader_schema = new AvroParquetReader<GenericRecord>(
								file.getPath());
						GenericRecord tmp_schema = reader_schema.read();
						fileSchema = tmp_schema.getSchema();
						reader_schema.close();
						break;
					} else {
						continue;
					}
				}
			}
		}
		// Print the Schema of one of the parquet files, which will be used as
		// schema for the final file!
		// System.out.println(fileSchema.toString());
		return fileSchema;
	}
}
