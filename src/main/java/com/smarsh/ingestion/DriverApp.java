package com.smarsh.ingestion;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import com.smarsh.common.Constants;
import com.smarsh.common.Region;
import com.smarsh.model.PreIndexMetaData;

public class DriverApp {
	/**
	 * Shards=12
	 * ShardSize=20
	 * FillThreahold=80
	 * IndexToDocMem=20
	 * 
	 * @param args
	 * @throws IOException
	 */
	
	private static final String SHARDS = "Shards";
	private static final String SHARD_SIZE = "ShardSize";
	private static final String UPPER_BOUND_THRESHOLD = "FillThreshold";
	private static final String INDEX_TO_DOC_MEM_RATIO = "IndexToDocMem";
	private static final String TENANT = "tenant";
	
	public static void main(String[] args) throws IOException {
		
		PreIndexMetaData preIndexMetaData = new PreIndexMetaData();
		preIndexMetaData.setTenant(System.getProperty(TENANT));
		preIndexMetaData.setMaxNumOfShardsPerIndex(Integer.parseInt(System.getProperty(SHARDS)));
		preIndexMetaData.setShardSizeInGB(Integer.parseInt(System.getProperty(SHARD_SIZE)));
		preIndexMetaData.setFillPercentage(Integer.parseInt(System.getProperty(UPPER_BOUND_THRESHOLD)));
		preIndexMetaData.setIndexToDocMemRatio(Constants.IndexToDataRatio(Integer.parseInt(System.getProperty(INDEX_TO_DOC_MEM_RATIO))).get());
		
		Properties properties = System.getProperties();
		
		IndexMetaGeneratorService app = new IndexMetaGeneratorService(properties);
		app.generatePreIndexes(preIndexMetaData, Arrays.asList(Region.values()));
	}
}
