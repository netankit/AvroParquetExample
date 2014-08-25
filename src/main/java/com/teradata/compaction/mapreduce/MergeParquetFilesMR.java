package com.teradata.compaction.mapreduce;

import static java.lang.Thread.sleep;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import parquet.Log;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.example.ExampleInputFormat;
import parquet.hadoop.example.ExampleOutputFormat;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageTypeParser;

/**
 * This script can be used to merge all Parquet files present in a given
 * directory, corresponding to a given schema, using map-reduce.
 * 
 * @author ankit
 *
 */
public class MergeParquetFilesMR {

	private static final Log LOG = Log.getLog(MergeParquetFilesMR.class);

	final Path parquetPath = new Path(
			"target/mapredexample/MergeParquetFilesMR/parquet");
	final Path inputPath = new Path(
			"src/main/java/com/teradata/compaction/mapreduce/MergeParquetFilesMR.java");
	final Path outputPath = new Path(
			"target/mapredexample/MergeParquetFilesMR/out");

	Job writeJob;
	Job readJob;
	private String writeSchema;
	private String readSchema;
	private Configuration conf;

	private final Class mergeParquetFilesReadMapperClass = MergeParquetFilesReadMapper.class;
	private final Class mergeParquetFilesWriteMapperClass = MergeParquetFilesWriteMapper.class;
	private final Class mergeParquetFilesReducerClass = MergeParquetFilesReducer.class;

	@SuppressWarnings("unused")
	private void runMapReduceJob(CompressionCodecName codec)
			throws IOException, ClassNotFoundException, InterruptedException {

		final FileSystem fileSystem = parquetPath.getFileSystem(conf);
		fileSystem.delete(parquetPath, true);
		fileSystem.delete(outputPath, true);
		{
			writeJob = new Job(conf, "write");
			TextInputFormat.addInputPath(writeJob, inputPath);
			writeJob.setInputFormatClass(TextInputFormat.class);
			writeJob.setNumReduceTasks(1);
			ExampleOutputFormat.setCompression(writeJob, codec);
			ExampleOutputFormat.setOutputPath(writeJob, parquetPath);
			writeJob.setOutputFormatClass(ExampleOutputFormat.class);
			writeJob.setMapperClass(mergeParquetFilesReadMapperClass);

			ExampleOutputFormat.setSchema(writeJob,
					MessageTypeParser.parseMessageType(writeSchema));
			writeJob.submit();
			waitForJob(writeJob);
		}
		{

			conf.set(ReadSupport.PARQUET_READ_SCHEMA, readSchema);
			readJob = new Job(conf, "read");

			readJob.setInputFormatClass(ExampleInputFormat.class);

			ExampleInputFormat.setInputPaths(readJob, parquetPath);
			readJob.setOutputFormatClass(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(readJob, outputPath);
			readJob.setMapperClass(mergeParquetFilesWriteMapperClass);
			readJob.setNumReduceTasks(1);
			readJob.submit();
			waitForJob(readJob);
		}
	}

	private void waitForJob(Job job) throws InterruptedException, IOException {
		while (!job.isComplete()) {
			LOG.debug("waiting for job " + job.getJobName());
			sleep(100);
		}
		LOG.info("status for job " + job.getJobName() + ": "
				+ (job.isSuccessful() ? "SUCCESS" : "FAILURE"));
		if (!job.isSuccessful()) {
			throw new RuntimeException("job failed " + job.getJobName());
		}
	}

	public static void main(String[] args) {

	}

}
