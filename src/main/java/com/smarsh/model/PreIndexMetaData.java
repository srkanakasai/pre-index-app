package com.smarsh.model;

import java.math.BigDecimal;

public class PreIndexMetaData {
	
	private String tenant;
	private int maxNumOfShardsPerIndex;
	private int shardSizeInGB;
	private int fillPercentage;
	private BigDecimal indexToDocMemRatio;
	
	public String getTenant() {
		return tenant;
	}
	public void setTenant(String tenant) {
		this.tenant = tenant;
	}
	public int getMaxNumOfShardsPerIndex() {
		return maxNumOfShardsPerIndex;
	}
	public void setMaxNumOfShardsPerIndex(int maxNumOfShardsPerIndex) {
		this.maxNumOfShardsPerIndex = maxNumOfShardsPerIndex;
	}
	public int getShardSizeInGB() {
		return shardSizeInGB;
	}
	public void setShardSizeInGB(int shardSizeInGB) {
		this.shardSizeInGB = shardSizeInGB;
	}
	public int getFillPercentage() {
		return fillPercentage;
	}
	public void setFillPercentage(int fillPercentage) {
		this.fillPercentage = fillPercentage;
	}
	public BigDecimal getIndexToDocMemRatio() {
		return indexToDocMemRatio;
	}
	public void setIndexToDocMemRatio(BigDecimal indexToDocMemRatio) {
		this.indexToDocMemRatio = indexToDocMemRatio;
	}
	
	@Override
	public String toString() {
		return "PreIndexMetaData [maxNumOfShardsPerIndex=" + maxNumOfShardsPerIndex + ", shardSizeInGB=" + shardSizeInGB
				+ ", fillPercentage=" + fillPercentage + ", indexToDocMemRatio=" + indexToDocMemRatio + "]";
	}
}
