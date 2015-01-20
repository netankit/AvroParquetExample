package com.teradata.avro;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;

import parquet.avro.AvroParquetReader;
import parquet.hadoop.ParquetReader;
import example.avro.User;

/*
 * 
 * Testing Reading of files writing using Parquet 1.6 RC4 API.
 * 
 * */
public class readParquetFileFull {

	public static void main(String[] args) {

		Path filepath1 = new Path(
				"/home/ankit/workspace/AvroExampleRC4/userprofiles/user.0.26.3398.588858.parquet");

		try {

			int count1 = readParquetFile(filepath1);
			System.out.println("Total Number of Records (Input File 1): "
					+ count1);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Reads the Parquet File, provided a custom user schema.
	 * 
	 * @param parquet_file_path
	 *            : Path of the Parquet File created on Disk
	 * @return count: Total Number of Records read off a Parquet File
	 * @throws IOException
	 * 
	 */
	@SuppressWarnings("unused")
	private static int readCustomParquetFile(Path parquet_file_path)
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
		reader.close();
		return count;
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
		System.out.println("Read from Parquet File: ");
		GenericRecord tmp;
		int count = 0;
		while ((tmp = reader.read()) != null) {
			// Print the individual record on screen!
			System.out.println(tmp.toString());
			count++;
		}
		reader.close();
		return count;
	}

}
