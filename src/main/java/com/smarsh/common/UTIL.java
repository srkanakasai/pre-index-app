package com.smarsh.common;

import static com.smarsh.common.Constants.ROUND_OFF_FACTOR;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.Date;
import java.util.stream.Stream;

public class UTIL {

	public static String getDateForIndex(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int monthDate = calendar.get(Calendar.DAY_OF_MONTH);
		int month = calendar.get(Calendar.MONTH)+1;
		int year = calendar.get(Calendar.YEAR);

		String monthStr = month<10?"0"+month:""+month;
		String dateStr = monthDate<10?"0"+monthDate:""+monthDate;

		return String.format("%d%s%s", year,monthStr,dateStr);
	}

	public static long getMaxSizePerIndexInGB(int maxNumOfShardsPerIndex, int shardSizeInGB, int fillPercentage) {
		int maxIndexMemoryAvailable = maxNumOfShardsPerIndex*shardSizeInGB;
		int maxAllowedSizePerIndex = ((maxIndexMemoryAvailable*fillPercentage)/100);
		return (ROUND_OFF_FACTOR*(Math.round((double)maxAllowedSizePerIndex/ROUND_OFF_FACTOR)));
	}

	public static Stream<String> readFile(String fileName, Object o) throws IOException {
		InputStream inputStream = o.getClass().getResourceAsStream("/"+fileName);
		InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
		Stream<String> lines = bufferedReader.lines();
		return lines;
	}
}
