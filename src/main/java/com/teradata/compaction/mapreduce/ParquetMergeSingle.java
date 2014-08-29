package com.teradata.compaction.mapreduce;

/* @Copyright 2014 Teradata, GMBH.
 *  LICENCE INFORMATION
 * --------------------- 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 *  * */

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
 * directory, with the same given schema, without using map-reduce.This script
 * differs from SimpleMergeParquetFiles as it doesn't require an already
 * compiled Avro object.
 * 
 * Usage: javac GenericParquetMerge directory_path_of_bulk_input_files
 * Usage(JAR): java -jar pmerge directory_path_of_bulk_input_files
 * 
 * Working: The script creates a new folder "output_000". And within it creates
 * a final_0000.parquet.
 * 
 * WARNING: If the directory_path_of_bulk_input_files already contains a folder
 * named "output_000", the script exits!
 * 
 * @author Ankit Bahuguna <ankit.bahuguna@cs.tum.edu>
 * @version 1.0
 * @date 22 August, 2014
 *
 */

public class ParquetMergeSingle {

	public static void main(String[] args) throws IOException {
		String fileInputPath = null;
		runmergejob(fileInputPath);
	}

	public static void runmergejob(String fileInputPath) throws IOException {

		String directory_path = fileInputPath;
		final File folder = new File(directory_path);

		String outputDirectoryPath = directory_path + "/output_000";
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
				if (fileEntry.toString().toUpperCase().toLowerCase()
						.endsWith(".parquet")) {
					// System.out.println("Current File: " + fileEntry);
					Path parquet_file_path = new Path(fileEntry.getPath());
					ParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(
							parquet_file_path);
					GenericRecord tmp;

					while ((tmp = reader.read()) != null) {
						// System.out.println(tmp.toString());
						writer.write(tmp);
					}
					reader.close();
				} else {
					continue;
				}
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
			System.out
					.println("Output folder already exists in the base input directory!");
			System.exit(0);
		} else {
			result = directory.mkdirs();
		}

		return result;
	}
}
