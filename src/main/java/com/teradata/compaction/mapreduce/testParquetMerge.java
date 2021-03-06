/* 
 * @Copyright 2014 Teradata, GMBH.
 * 
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
package com.teradata.compaction.mapreduce;

import java.io.File;
import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;

import parquet.avro.AvroParquetReader;
import parquet.hadoop.ParquetReader;

/**
 * This script can be used to test the ParquetFileMerge. It calculates the
 * number of records which are there in the input parquet files present in a
 * given directory, sums them up and then calculates the number of records in
 * the given output parquet file generated after the merge operation. At the
 * end, it matches both counts and verifies whether or not all records are
 * written to the output parquet file.
 * 
 * Usage: java -jar testParquetMerge path_to_input_folder path_to_output_file
 * 
 * @author Ankit Bahuguna <ankit.bahuguna@cs.tum.edu>
 * @version 1.0
 * @date 3 September, 2014
 *
 */

public class testParquetMerge {

	public static void main(String[] args) {

		if (args.length != 2) {
			System.err
					.println("Usage: java -jar testParquetMerge path_to_input_folder path_to_output_file ");
			System.exit(0);
		}

		Path pathToOutputFolder = new Path(args[1]);
		String directory_path_input = args[0];
		final File folder = new File(directory_path_input);

		int totalNumberOfRecordsInputFile = 0;

		for (File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				continue;
			} else {
				if (fileEntry.toString().toUpperCase().toLowerCase()
						.endsWith(".parquet")) {
					Path filepath = new Path(fileEntry.getPath());
					int count = 0;
					try {
						count = readParquetFile(filepath);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					totalNumberOfRecordsInputFile += count;
				}
			}
		}
		System.out
				.println("\n\n**********************************************");
		System.out.println("Total Number of Records(Input): "
				+ totalNumberOfRecordsInputFile);
		try {

			int totalNumberOfRecordsOutputFile = readParquetFile(pathToOutputFolder);
			System.out.println("Total Number of Records(Output): "
					+ totalNumberOfRecordsOutputFile);

			if ((totalNumberOfRecordsInputFile) == totalNumberOfRecordsOutputFile) {
				System.out.println("All Records Match: True");
			} else {
				System.out.println("All Records Match: False");
			}
			System.out
					.println("**********************************************\n");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param parquet_file_path
	 * @return count: Total Number of Records read off a Parquet File
	 * @throws IOException
	 */
	private static int readParquetFile(Path parquet_file_path)
			throws IOException {
		ParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(
				parquet_file_path);
		// System.out.println("Read from Parquet File: ");
		GenericRecord tmp;
		int count = 0;
		while ((tmp = reader.read()) != null) {
			// Print the individual record on screen!
			// System.out.println(tmp.toString());
			count++;
		}
		reader.close();
		return count;
	}

}
