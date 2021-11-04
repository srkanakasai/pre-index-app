package com.smarsh.common;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Constants {
	public static final ThreadLocal<SimpleDateFormat> dateFormat = new ThreadLocal<SimpleDateFormat>() {
		@Override
		public SimpleDateFormat get() {
			return new SimpleDateFormat("yyyy-MM-dd");
		}
	};
	
	public static final Date parse(String date) {
		try {
			return dateFormat.get().parse("2010-01-01");
		} catch (ParseException e) {
			return null;
		}
	}
	
	/**
	 * TODO : dymanic ingestion
	 */
	public static final int MAX_NUM_OF_SHARDS_PER_INDEX = 12;
	public static final int SHARD_SIZE_IN_GB = 20;
	public static final int OCCUPENCY_PERCENTAGE = 80;
	
	public static final int MAX_SIZE_PER_INDEX = 200000000;
	public static final BigDecimal IndexToDataRatio = BigDecimal.valueOf((double)20/100);
}
