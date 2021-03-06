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
	
	/**
	 * If D2 year is greater than D1 then return 1
	 * if D2 year is less than D1 then return -1
	 * if Both the years are same return 0;
	 * @param d1
	 * @param d2
	 * @return
	 */
	public static int compareYears(Date d1, Date d2) {
		Calendar calendar1 = Calendar.getInstance();
		calendar1.setTime(d1);
		int year1 = calendar1.get(Calendar.YEAR);
		
		Calendar calendar2 = Calendar.getInstance();
		calendar2.setTime(d2);
		int year2 = calendar2.get(Calendar.YEAR);
		
		return (year2>year1)?1:(year1>year2)?-1:0;
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
