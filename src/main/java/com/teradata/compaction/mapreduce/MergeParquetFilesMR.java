package com.teradata.compaction.mapreduce;

import static java.lang.Thread.sleep;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapreduce.Job;

import parquet.Log;

/**
 * This script can be used to merge all Parquet files present in a given
 * directory, corresponding to a given schema, using map-reduce.
 * 
 * @author ankit
 *
 */
public class MergeParquetFilesMR {

	private static final Log LOG = Log.getLog(MergeParquetFilesMR.class);

	final Path inputDirectoryPath = new Path(
			"/home/ankit/workspace/AvroExample/inputforhadoop");

	Job mergeJob;
	JobConf mergeconf;

	private final Class<MergeParquetFilesMapper> mergeParquetFilesMapperClass = MergeParquetFilesMapper.class;

	private void runMapReduceJob() throws IOException, ClassNotFoundException,
			InterruptedException {
		mergeconf = new JobConf(MergeParquetFilesMR.class);
		FileInputFormat.setInputPaths(mergeconf, inputDirectoryPath);
		mergeconf
				.setMapperClass((Class<? extends Mapper>) MergeParquetFilesMapper.class);
		// mergeJob = new Job(mergeconf);
		// mergeJob.setNumReduceTasks(0);
		// mergeJob.submit();
		// waitForJob(mergeJob);
		mergeconf.setNumReduceTasks(0);
		JobClient.runJob(mergeconf);

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

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {

		MergeParquetFilesMR mrobj = new MergeParquetFilesMR();
		mrobj.runMapReduceJob();
	}
}
