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

public class sampleParquetMerge {

	public static void main(String[] args) throws IOException {

		Path parquet_file_path = new Path(
				"/home/ankit/workspace/AvroExample/BulkParquetFiles/sample1.parquet");
		Path parquet_output_file_path = new Path(
				"/home/ankit/workspace/AvroExample/BulkParquetFiles/final.parquet");
		ParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(
				parquet_file_path);

		GenericRecord tmp = reader.read();

		System.out.println("Schema: " + tmp.getSchema());

		File outputFile = new File(
				"/home/ankit/workspace/AvroExample/BulkParquetFiles/final.parquet");
		if (outputFile.exists()) {
			outputFile.delete();
		}

		ParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(
				parquet_output_file_path, tmp.getSchema(),
				CompressionCodecName.UNCOMPRESSED, DEFAULT_BLOCK_SIZE,
				DEFAULT_PAGE_SIZE, false);

		while ((tmp = reader.read()) != null) {
			System.out.println(tmp.toString());
			writer.write(tmp);
		}
		reader.close();
		writer.close();

	}
}
