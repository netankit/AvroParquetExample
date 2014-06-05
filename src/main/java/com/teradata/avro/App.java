package com.teradata.avro;

/**
 * Hello world!
 * 
 */
public class App {

	public static void main(String[] args) {
		System.out.println("Hello World!");
		User user1 = new User();
		user1.setName("Alyssa");
		user1.setFavoriteNumber(256);
		// Leave favorite color null

		// Alternate constructor
		User user2 = new User("Ben", 7, "red");

		// Construct via builder
		User user3 = User.newBuilder().setName("Charlie")
				.setFavoriteColor("blue").setFavoriteNumber(null).build();

	}
}
