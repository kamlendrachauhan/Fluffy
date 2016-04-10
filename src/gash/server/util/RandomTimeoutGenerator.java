package gash.server.util;

import java.util.Random;

public class RandomTimeoutGenerator {
	// Time in milliseconds
	private static int MINIMUM_TIMOUT = 1500;
	private static int MAXIMUM_TIMOUT = 4000;
	private static final int BASE_TIMEOUT = 1000;

	public static int randTimeout() {

		Random random = new Random();

		int randomNumber = BASE_TIMEOUT + random.nextInt(MAXIMUM_TIMOUT) + random.nextInt(MINIMUM_TIMOUT);

		return randomNumber;
	}
}
