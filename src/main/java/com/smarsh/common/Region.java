package com.smarsh.common;

import java.util.Date;

public enum Region {
	APAC("APACHistogram.txt", Constants.parse("2010-01-01"), Constants.parse("2021-12-31")),
	EMEA("EMEAHistogram.txt", Constants.parse("2010-01-01"), Constants.parse("2021-12-31")),
	NAM("NAMHistogram.txt", Constants.parse("2010-01-01"), Constants.parse("2021-12-31"));
	
	private String fileName;
	private Date lowerBound;
	private Date upperBound;
	
	private Region(String fileName, Date lb, Date ub) {
		this.fileName = fileName;
		this.lowerBound = lb;
		this.upperBound = ub;
	}

	public String getFileName() {
		return fileName;
	}

	public Date getLowerBound() {
		return lowerBound;
	}

	public Date getUpperBound() {
		return upperBound;
	}
}
