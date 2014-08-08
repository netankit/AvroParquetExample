package com.teradata.compaction;

import static parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;

import parquet.avro.AvroParquetReader;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import example.avro.User;

/**
 * This script can be used to merge all Parquet files present in a given
 * directory, corresponding to a given schema, without using map-reduce.
 * 
 * @author ankit
 *
 */
public class SimpleMergeParquetFiles {

	public static ArrayList<User> readParquetFilesInFolderToAvro(
			final File folder) throws IOException {
		ArrayList<User> user_final = new ArrayList<User>();

		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				readParquetFilesInFolderToAvro(fileEntry);
			} else {
				// System.out.println(fileEntry.getName());
				Path parquet_file_path = new Path(fileEntry.getPath());
				ParquetReader<User> reader = new AvroParquetReader<User>(
						parquet_file_path);
				System.out.println("Read from Parquet File: ");
				User user_tmp = null;

				while ((user_tmp = reader.read()) != null) {
					User user_builder = new User(user_tmp.getName(),
							user_tmp.getFavoriteNumber(),
							user_tmp.getFavoriteColor());
					user_final.add(user_builder);

				}
				reader.close();

			}
		}
		return user_final;
	}

	private static void writeAvroToParquet(ArrayList<User> user)
			throws IOException {
		System.out.println("Starting wrting to Parquet");
		File tmp = new File("MergedParquet.parquet");
		if (tmp.exists()) {
			tmp.delete();
		}
		Path file = new Path(tmp.getPath());
		ParquetWriter<User> writer = new AvroParquetWriter<User>(file,
				User.SCHEMA$, CompressionCodecName.UNCOMPRESSED,
				DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, false);
		int count = 1;
		for (Iterator<User> iterator = user.iterator(); iterator.hasNext();) {
			Object object = iterator.next();
			writer.write((User) object);
			System.out.println("Record writen to parquet file: " + count);
			count++;
		}

		writer.close();
		System.out.println("Writing to Parquet Done!");
	}

	public static void main(String[] args) throws IOException {
		String directory_path = "BulkParquetFiles";
		ArrayList<User> all_users;
		final File folder = new File(directory_path);
		all_users = readParquetFilesInFolderToAvro(folder);
		writeAvroToParquet(all_users);
	}

}
