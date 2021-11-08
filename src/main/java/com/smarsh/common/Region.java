package com.smarsh.common;

import java.util.Date;

public enum Region {
	APAC("APACHistogram.txt", new Date[] {Constants.parse("2012-01-01"), Constants.parse("2021-12-31")}),
	EMEA("EMEAHistogram.txt", new Date[] {Constants.parse("2009-01-01"), Constants.parse("2021-12-31")}),
	NAM("NAMHistogram.txt", new Date[] {Constants.parse("2006-01-01"), Constants.parse("2021-12-31")});
	
	private String fileName;
	private Date[] outliers;
	
	private Region(String fileName, Date[] outliers) {
		this.outliers = outliers;
		this.fileName = fileName;
	}

	public String getFileName() {
		return fileName;
	}

	public Date[] getOutliers() {
		return outliers;
	}

	public Date getLowerBound() {
		return outliers[0];
	}
	
	public Date getUpperBound() {
		return outliers[1];
	}
	
	
}
