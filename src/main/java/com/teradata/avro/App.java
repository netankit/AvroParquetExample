package com.teradata.avro;

import static parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.Path;

import parquet.avro.AvroParquetReader;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import example.avro.User;

/**
 * Hello world!
 * 
 */
public class App {

	public static void main(String[] args) throws IOException {
		System.out.println("Example Code : Avro Adding Users");
		System.out.println("--------------------------------");

		// User 1
		User user1 = new User();
		user1.setName("Ankit");
		user1.setFavoriteNumber(011);

		User user2 = new User("Patick", 21, "Black");

		// Construct via builder
		User user3 = User.newBuilder().setName("Chris")
				.setFavoriteColor("Green").setFavoriteNumber(null).build();

		// Adds user to Avro object and Serialize it.
		addUsersAndSerialize(user1, user2, user3);

		File file = new File("users.avro");
		File parquet_file = new File("App.parquet");

		Path parquet_file_path = new Path(parquet_file.getPath());

		// Deserialization
		deserializeExample(file);

		readWriteDataWithAvro(user1, user2, user3);
		writeAvroToParquet(user1, user2, user3);
		readParquetFile(parquet_file_path);

	}

	private static void readParquetFile(Path parquet_file_path)
			throws IOException {
		ParquetReader<User> reader = new AvroParquetReader<User>(
				parquet_file_path);
		System.out.println("Read from Parquet File: ");

		User user = null;
		while ((user = reader.read()) != null) {
			System.out.println("Name : " + user.getName());
			System.out.println("Favourite Colour : " + user.getFavoriteColor());
			System.out.println("Fav Number : " + user.getFavoriteNumber());
		}
	}

	private static void writeAvroToParquet(User user1, User user2, User user3)
			throws IOException {
		File tmp = new File(App.class.getSimpleName() + ".parquet");
		if (tmp.exists()) {
			tmp.delete();
		}
		Path file = new Path(tmp.getPath());
		ParquetWriter<User> writer = new AvroParquetWriter<User>(file,
				User.SCHEMA$, CompressionCodecName.UNCOMPRESSED,
				DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, false);
		writer.write(user1);
		writer.write(user2);
		writer.write(user3);
		writer.close();
	}

	private static void addUsersAndSerialize(User user1, User user2, User user3)
			throws IOException {
		// Serializing
		serializeExample(user1, user2, user3);
	}

	private static void deserializeExample(File file) throws IOException {
		// Deserialize Users from disk
		DatumReader<User> userDatumReader = new SpecificDatumReader<User>(
				User.class);
		DataFileReader<User> dataFileReader = new DataFileReader<User>(file,
				userDatumReader);
		User user = null;
		System.out.println("Deserialized User Details:");
		while (dataFileReader.hasNext()) {
			// Reuse user object by passing it to next(). This saves us from
			// allocating and garbage collecting many objects for files with
			// many items.
			user = dataFileReader.next(user);
			System.out.println(user);
		}
	}

	private static void serializeExample(User user1, User user2, User user3)
			throws IOException {
		// Serialize user1 and user2 to disk

		DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(
				User.class);
		DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(
				userDatumWriter);
		dataFileWriter.create(user1.getSchema(), new File("users.avro"));
		dataFileWriter.append(user1);
		dataFileWriter.append(user2);
		dataFileWriter.append(user3);
		dataFileWriter.close();
	}

	private static void readWriteDataWithAvro(User user1, User user2, User user3)
			throws IOException {
		// Serialize user1 and user2 to disk
		File file = new File("users.avro");
		DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(
				User.class);
		DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(
				userDatumWriter);
		dataFileWriter.create(user1.getSchema(), new File("users.avro"));
		dataFileWriter.append(user1);
		dataFileWriter.append(user2);
		dataFileWriter.append(user3);
		dataFileWriter.close();

		// Deserialize Users from disk
		DatumReader<User> userDatumReader = new SpecificDatumReader<User>(
				User.class);
		DataFileReader<User> dataFileReader = new DataFileReader<User>(file,
				userDatumReader);
		User user = null;
		while (dataFileReader.hasNext()) {
			// Reuse user object by passing it to next(). This saves us from
			// allocating and garbage collecting many objects for files with
			// many items.
			user = dataFileReader.next(user);
			// System.out.println(user);
		}
	}
}
