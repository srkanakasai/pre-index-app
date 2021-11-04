package com.smarsh.pojo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import com.smarsh.common.Constants;
import com.smarsh.common.Region;

public class HistogramData {

	private Region region;
	private Date date;
	private BigDecimal sizeInKb;
	private Integer hour;
	private double indexSize;
	public HistogramData(Region region, Date date, BigDecimal sizeInKb, Integer hour, double indexSize) {
		super();
		this.region = region;
		this.date = date;
		this.sizeInKb = sizeInKb;
		this.hour = hour;
		this.indexSize = indexSize;
	}
	public Region getRegion() {
		return region;
	}
	public Date getDate() {
		return date;
	}
	public String getDateInString() {
		return Constants.dateFormat.get().format(date);
	}
	public BigDecimal getSizeInKb() {
		return sizeInKb;
	}
	public Integer getHour() {
		return hour;
	}
	public double getIndexSize() {
		return indexSize;
	}
	@Override
	public String toString() {
		return "HistogramData [date=" + Constants.dateFormat.get().format(date) + ", sizeInKb=" + sizeInKb.intValue() + ", hour=" + hour + ", indexSize=" + indexSize
				+ "]";
	}
}
