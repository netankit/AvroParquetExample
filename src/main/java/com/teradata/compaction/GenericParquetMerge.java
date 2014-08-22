package com.teradata.compaction;

import static parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;

import parquet.avro.AvroParquetReader;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

/**
 * This script can be used to merge all Parquet files present in a given
 * directory, irrespective of a given schema, without using map-reduce.This
 * script differs from SimpleMergeParquetFiles as it doesn't require an already
 * compiled Avro object in order to make it more generic to unknown Avro
 * objects.
 * 
 * @author ankit
 *
 */

public class GenericParquetMerge {
	// static ParquetWriter<GenericRecord> writer;

	public static void main(String[] args) throws IOException {

		String directory_path = "/home/ankit/workspace/AvroExample/BulkParquetFiles1";
		final File folder = new File(directory_path);

		String outputDirectoryPath = directory_path + "/output";
		createFolder(outputDirectoryPath);
		String outputPath = outputDirectoryPath + "/final_0000.parquet";
		Path parquet_output_file_path = new Path(outputPath);

		Schema fileSchema = null;
		fileSchema = getBaseSchema(folder, fileSchema);

		File outputFile = new File(outputPath);
		if (outputFile.exists()) {
			outputFile.delete();
		}

		ParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(
				parquet_output_file_path, fileSchema,
				CompressionCodecName.UNCOMPRESSED, DEFAULT_BLOCK_SIZE,
				DEFAULT_PAGE_SIZE, false);

		for (File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				continue;
			} else {
				System.out.println("Current File: " + fileEntry);
				Path parquet_file_path = new Path(fileEntry.getPath());
				ParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(
						parquet_file_path);
				GenericRecord tmp;
				while ((tmp = reader.read()) != null) {
					// System.out.println(tmp.toString());
					writer.write(tmp);
				}
				reader.close();

			}
		}
		writer.close();
		System.out.println("Merging of Files - Finished");
	}

	private static Schema getBaseSchema(final File folder, Schema fileSchema)
			throws IOException {
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				continue;
			} else {
				Path parquet_file_path = new Path(fileEntry.getPath());

				ParquetReader<GenericRecord> reader_schema = new AvroParquetReader<GenericRecord>(
						parquet_file_path);

				GenericRecord tmp_schema = reader_schema.read();
				fileSchema = tmp_schema.getSchema();
				reader_schema.close();
				break;

			}
		}
		return fileSchema;
	}

	private static boolean createFolder(String theFilePath) {
		boolean result = false;

		File directory = new File(theFilePath);

		if (directory.exists()) {
			System.out.println("Folder already exists");
		} else {
			result = directory.mkdirs();
		}

		return result;
	}
}
