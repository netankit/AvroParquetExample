package com.teradata.avro;

import static parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.fs.Path;

import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import example.avro.User;

public class writeBulkParquet {

	public static void main(String[] args) throws IOException {

		int NUMBER_OF_RECORDS = 5000000;
		// Constructs a User object (avro), consisting of 1.5 Million Records,
		// with random data for name, Color and Favorite number
		// Construct via builder
		System.out.println("Starting.....");
		ArrayList<User> user = new ArrayList<User>();
		ArrayList<String> names = new ArrayList<String>();
		ArrayList<String> colors = new ArrayList<String>();

		for (int i = 0; i < NUMBER_OF_RECORDS; i++) {

			User user_builder = new User(getRandomName(names),
					getRandomNumber(), getRandomColor(colors));

			user.add(user_builder);

			System.out.println("Record added to user list: " + i);
		}
		User user_builder1 = new User("Ankit", 22, "Green");
		User user_builder2 = new User("Ankit", 22, "Green");
		User user_builder3 = new User("Ankit Bahuguna", 23, "Green");

		user.add(user_builder1);
		user.add(user_builder2);
		user.add(user_builder3);

		System.out
				.println("Avro user object populated with 15 million records");

		writeAvroToParquet(user);
		System.out.println("Everything done!");
	}

	private static void writeAvroToParquet(ArrayList<User> user)
			throws IOException {
		System.out.println("Starting wrting to Parquet");
		File tmp = new File("sample.parquet");
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

	private static String getRandomColor(ArrayList<String> colors) {
		if (colors.size() == 0) {
			try {
				// Open the file that is the first
				// command line parameter
				FileInputStream fstream = new FileInputStream("all_colors.txt");
				// Get the object of DataInputStream
				DataInputStream in = new DataInputStream(fstream);
				BufferedReader br = new BufferedReader(
						new InputStreamReader(in));
				String strLine;
				// Read File Line By Line
				while ((strLine = br.readLine()) != null) {
					// Print the content on the console
					colors.add(strLine.toLowerCase());
				}
				// Close the input stream
				in.close();
			} catch (Exception e) {// Catch exception if any
				System.err.println("Error: " + e.getMessage());
			}
		}
		int index = new Random().nextInt(colors.size());
		String random_color = colors.get(index);
		return random_color;
	}

	private static int getRandomNumber() {
		int min = 0;
		int max = 2500000;

		// Usually this should be a field rather than a method variable so
		// that it is not re-seeded every call.
		Random rand = new Random();

		// nextInt is normally exclusive of the top value,
		// so add 1 to make it inclusive
		int randomNum = rand.nextInt((max - min) + 1) + min;

		return randomNum;
	}

	private static String getRandomName(ArrayList<String> names) {
		if (names.size() == 0) {
			try {
				// Open the file that is the first
				// command line parameter
				FileInputStream fstream = new FileInputStream("all_names.txt");
				// Get the object of DataInputStream
				DataInputStream in = new DataInputStream(fstream);
				BufferedReader br = new BufferedReader(
						new InputStreamReader(in));
				String strLine;
				// Read File Line By Line
				while ((strLine = br.readLine()) != null) {
					// Print the content on the console
					names.add(strLine);
				}
				// Close the input stream
				in.close();
			} catch (Exception e) {// Catch exception if any
				System.err.println("Error: " + e.getMessage());
			}
		}
		int index = new Random().nextInt(names.size());
		String random_name = names.get(index);
		return random_name;
	}

}
