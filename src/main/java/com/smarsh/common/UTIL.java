package com.smarsh.common;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

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
	
	public static void main(String[] args) throws ParseException {
		System.out.println(Constants.dateFormat.get().parse("2021-08-20"));
		System.out.println(getDateForIndex(Constants.dateFormat.get().parse("2021-08-20")));
	}

}
