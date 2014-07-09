package com.teradata.avro;

import static parquet.filter.ColumnPredicates.equalTo;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;

import parquet.avro.AvroParquetReader;
import parquet.filter.ColumnRecordFilter;
import parquet.hadoop.ParquetReader;
import example.avro.User;

public class parquetReadEvaluation {

	public static void main(String[] args) {

		File parquet_file = new File("BulkFile_15M_Records.parquet");
		Path parquet_file_path = new Path(parquet_file.getPath());
		System.out.println("Reading a Specific Record from Parquet:");
		System.out.println("########################################");
		ParquetReader<User> reader_single;
		try {

			// Test 1
			System.out
					.println("------------------ TEST #1  ------------------");
			long startTime = System.nanoTime();
			reader_single = new AvroParquetReader<User>(parquet_file_path,
					ColumnRecordFilter.column("name", equalTo("FRED")));
			long endTime = System.nanoTime();
			long duration = (endTime - startTime) / 1000000;
			System.out.println("Query: SCAN FOR SPECIFIC NAME");
			System.out.println("Query Output: " + reader_single.read());
			System.out.println("Query Time in milli Seconds: " + duration);
			System.out.println("-------------------------------------------");

			// Test 2
			System.out
					.println("------------------ TEST #2  ------------------");
			long startTime1 = System.nanoTime();
			reader_single = new AvroParquetReader<User>(parquet_file_path,
					ColumnRecordFilter
							.column("favorite_color", equalTo("blue")));
			long endTime1 = System.nanoTime();
			long duration1 = (endTime1 - startTime1) / 1000000;
			System.out.println("Query: SCAN FOR SPECIFIC COLOR");
			System.out.println("Query Output: " + reader_single.read());
			System.out.println("Query Time in milli Seconds: " + duration1);
			System.out.println("-------------------------------------------");

			// Test 3
			System.out
					.println("------------------ TEST #3  ------------------");
			long startTime2 = System.nanoTime();
			reader_single = new AvroParquetReader<User>(parquet_file_path,
					ColumnRecordFilter.column("favorite_number", equalTo("1")));
			long endTime2 = System.nanoTime();
			long duration2 = (endTime2 - startTime2) / 1000000;
			System.out.println("Query: SCAN FOR SPECIFIC NUMBER");
			System.out.println("Query Output: " + reader_single.read());
			System.out.println("Query Time in milli Seconds: " + duration2);
			System.out.println("-------------------------------------------");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
