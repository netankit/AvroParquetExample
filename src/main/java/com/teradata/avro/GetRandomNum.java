package com.teradata.avro;

import java.util.Random;

public class GetRandomNum {

	public static void main(String args[]) {
		System.out.println(getRandomNumber());
	}

	private static Integer getRandomNumber() {
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

}
