package com.teradata.compaction;

import static parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;

import java.io.File;
import java.io.IOException;

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
	static ParquetWriter<GenericRecord> writer;

	public static void main(String[] args) throws IOException {

		String directory_path = "BulkParquetFiles1";
		final File folder = new File(directory_path);
		String output_filename = "final.parquet";
		Path parquet_output_file_path = new Path(output_filename);

		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				continue;
			} else {
				Path parquet_file_path = new Path(fileEntry.getPath());

				ParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(
						parquet_file_path);

				GenericRecord tmp = reader.read();
				System.out.println("Schema: " + tmp.getSchema());

				File fileptr = new File(output_filename);
				if (!fileptr.exists()) {
					writer = new AvroParquetWriter<GenericRecord>(
							parquet_output_file_path, tmp.getSchema(),
							CompressionCodecName.UNCOMPRESSED,
							DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, false);
				}
				while ((tmp = reader.read()) != null) {
					// System.out.println(tmp.toString());
					writer.write(tmp);
				}
				reader.close();
			}
		}
		writer.close();

	}
}
