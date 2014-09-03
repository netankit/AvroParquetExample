package com.teradata.avro;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import parquet.avro.AvroParquetReader;
import parquet.hadoop.ParquetReader;
import example.avro.User;

public class readEntireParquetFile {

	public static void main(String[] args) {
		Path filepath = new Path(
				"/home/ankit/workspace/AvroExample/newtest1/sample2.parquet");
		try {
			readParquetFile(filepath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Reads the Parquet File
	 * 
	 * @param parquet_file_path
	 *            : Path of the Parquet File created on Disk
	 * @throws IOException
	 */
	private static void readParquetFile(Path parquet_file_path)
			throws IOException {
		ParquetReader<User> reader = new AvroParquetReader<User>(
				parquet_file_path);
		System.out.println("Read from Parquet File: ");
		User user = null;
		int count = 0;
		while ((user = reader.read()) != null) {
			System.out.println(", Name : " + user.getName());
			System.out.print("Favourite Colour : " + user.getFavoriteColor());
			System.out
					.print(", Favourite Number : " + user.getFavoriteNumber());
			count++;
		}
		System.out.println("Total Number of Records: " + count);
		reader.close();
	}

}
