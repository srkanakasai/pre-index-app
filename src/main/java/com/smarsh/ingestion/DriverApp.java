package com.smarsh.ingestion;

import java.io.IOException;
import java.util.Arrays;

import com.smarsh.common.Constants;
import com.smarsh.common.Region;
import com.smarsh.pojo.PreIndexMetaData;

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
		int i = 0;
		while(i < args.length) {
			String argName = args[i];
			switch (argName) {
			case TENANT:
				i++;
				preIndexMetaData.setTenant(args[i++]);
				break;
			case SHARDS:
				i++;
				preIndexMetaData.setMaxNumOfShardsPerIndex(Integer.parseInt(args[i++]));
				break;
			case SHARD_SIZE:
				i++;
				preIndexMetaData.setShardSizeInGB(Integer.parseInt(args[i++]));
				break;
			case UPPER_BOUND_THRESHOLD:
				i++;
				preIndexMetaData.setFillPercentage(Integer.parseInt(args[i++]));
				break;
			case INDEX_TO_DOC_MEM_RATIO:
				i++;
				preIndexMetaData.setIndexToDocMemRatio(Constants.IndexToDataRatio(Integer.parseInt(args[i++])).get());
				break;
			default:
				break;
			}
		}
		
		
		IndexMetaGeneratorService app = new IndexMetaGeneratorService();
		app.generatePreIndexes(preIndexMetaData, Arrays.asList(Region.values()));
	}
}
