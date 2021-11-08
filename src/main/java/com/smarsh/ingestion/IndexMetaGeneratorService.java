package com.smarsh.ingestion;

import static com.smarsh.common.Constants.dateFormat;
import static com.smarsh.common.Constants.mega;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.smarsh.common.Region;
import com.smarsh.common.UTIL;
import com.smarsh.model.HistogramData;
import com.smarsh.model.Pair;
import com.smarsh.model.PreIndexMetaData;

public class IndexMetaGeneratorService {

	private static Logger logger = LogManager.getLogger(IndexMetaGeneratorService.class);
	private static Long MAX_SIZE_PER_INDEX = 200000000l;
	private final Properties properties;

	public IndexMetaGeneratorService(Properties properties) {
		this.properties = properties;
	}

	public void generatePreIndexes(PreIndexMetaData metaData, List<Region> regions) {
		logger.info("***generatePreIndexes STARTS****");
		StringBuilder summary = new StringBuilder("\n**\tIndex Summary\t**\n");
		try {

			MAX_SIZE_PER_INDEX = UTIL.getMaxSizePerIndexInGB(
					metaData.getMaxNumOfShardsPerIndex(),
					metaData.getShardSizeInGB(),
					metaData.getFillPercentage())*mega;
			
			for(Region region : regions) {
				try {
					Stream<String> lines = UTIL.readFile(region.getFileName(),this);

					ArrayList<Pair<Double, List<HistogramData>>> groupByIndexSumMax = this.groupByOutliersAndSize(
							lines, 
							metaData,
							region);

					generateIndexSnapshot(groupByIndexSumMax, metaData, region);

					summary.append(String.format("\tRegion : %s, Indexes Required : %d\n", region.name(), groupByIndexSumMax.size()));
				} catch (IOException ioe) {
					logger.info("Exception when processing "+region.getFileName(), ioe);
					summary.append(String.format("\tRegion : %s, Index creation Exception", region.name()));
				}
			}
			summary.append("**\tEnd Of Summary\t**");
		} catch(Exception e) {
			logger.error(e.getCause().getClass().getSimpleName(), e);
		}
		finally {
			logger.info(summary.toString());
			logger.info("***generatePreIndexes END****");
		}
	}

	private void generateIndexSnapshot(ArrayList<Pair<Double, List<HistogramData>>> groupByIndexSumMax, PreIndexMetaData preIndexMetaData, Region region) {

		String outputFileName = region+"_indexes.txt";
		File newFile = new File(outputFileName);
		if(newFile.exists())
			newFile.delete();

		try {
			if(newFile.createNewFile()) {
				FileWriter fw = new FileWriter(outputFileName);

				logger.debug("Total No. of Indexes for: ******* "+region+" : "+groupByIndexSumMax.size()+" *******");
				fw.write("Total No. of Indexes for: ******* "+region+" : "+groupByIndexSumMax.size()+" *******\n");

				for(int i = 0; i<groupByIndexSumMax.size(); i++) {
					List<HistogramData> subSet = groupByIndexSumMax.get(i).getRight();
					HistogramData startRange = subSet.get(0);
					HistogramData endRange = subSet.get(subSet.size()-1);
					Integer indexMemSize = groupByIndexSumMax.get(i).getLeft().intValue();
					Date startDate = startRange.getDate();
					String indexID = String.format("BNY_%s_data_%s_1000_archive.av5", 
							region.name(), UTIL.getDateForIndex(startDate));

					logger.debug(String.format("IndexID:%s, StartDate:%s, EndDate:%s, MemoryConsumption:%d(KB)|%d(GB), ",
							indexID, startRange.getDateInString(), endRange.getDateInString(), indexMemSize.intValue(), indexMemSize.intValue()/mega));
					fw.write(String.format("[%s -to- %s]\t:\tIndexID:%s, MemoryConsumption:%d(KB)|%d(GB)\n",
							startRange.getDateInString(), endRange.getDateInString(), indexID, indexMemSize.intValue(), indexMemSize.intValue()/mega));
				}

				fw.write("************** END ***********************\n");
				logger.debug("************** END ***********************");

				fw.close();
			}
		} catch (IOException e) {
			logger.error("Exception in writing the details to output file", e);
		}
	}

	private ArrayList<Pair<Double, List<HistogramData>>> groupByOutliersAndSize(Stream<String> lines, PreIndexMetaData preIndexMetaData, Region region) {

		ArrayList<Pair<Double, List<HistogramData>>> groupByIndexSumMax = lines
				.skip(1)
				.map(line -> {
					String[] data = line.split(",");
					Date date = null;
					try {
						date = dateFormat.get().parse(data[0]);
					} catch (ParseException e) {
						e.printStackTrace();
					}
					BigDecimal sizeInKB = new BigDecimal(data[2]);
					Integer hour = Integer.parseInt(data[1]);
					BigDecimal indexSizeV2 = sizeInKB.multiply(preIndexMetaData.getIndexToDocMemRatio());
					HistogramData histogramData = new HistogramData(region, date, sizeInKB, hour, indexSizeV2.doubleValue());
					return histogramData;
				})
				.sorted((h1,h2)->Long.valueOf(h1.getDate().getTime()).compareTo(h2.getDate().getTime()))
				.collect(ArrayList<Pair<Double, List<HistogramData>>>::new, Accumulator::indexAggregator, (x, y) -> {});

		return groupByIndexSumMax;
	}

	private static class Accumulator {
		public static void indexAggregator(List<Pair<Double, List<HistogramData>>> lPair, HistogramData histo) {
			Pair<Double, List<HistogramData>> lastPair = lPair.isEmpty() ? null : lPair.get(lPair.size() - 1);
			Double indexSize = histo.getIndexSize();
			Region region = histo.getRegion();

			Boolean useOutliers = Boolean.parseBoolean(System.getProperty("outliers", "false"));

			if(!useOutliers) {
				if( Objects.isNull(lastPair) || lastPair.left + indexSize > MAX_SIZE_PER_INDEX) {
					lPair.add(
							new Pair<Double, List<HistogramData>>(indexSize,
									Arrays.asList(histo)));
				} else {
					List<HistogramData> newList = new ArrayList<>();
					newList.addAll(lastPair.getRight());
					newList.add(histo);
					lastPair.setLeft(lastPair.getLeft() + indexSize);
					lastPair.setRight(newList);
				}
			} else {
				if( Objects.isNull(lastPair)) {
					lPair.add(
							new Pair<Double, List<HistogramData>>(indexSize,
									Arrays.asList(histo)));
				} else if (histo.getDate().before(region.getLowerBound())) {
					if(lastPair.left + indexSize > MAX_SIZE_PER_INDEX) {
						lPair.add(
								new Pair<Double, List<HistogramData>>(
										indexSize,
										Arrays.asList(histo)));
					} else {
						List<HistogramData> newList = new ArrayList<>();
						newList.addAll(lastPair.getRight());
						newList.add(histo);
						lastPair.setLeft(lastPair.getLeft() + indexSize);
						lastPair.setRight(newList);
					}
				} else if(histo.getDate().after(region.getUpperBound())) {
					HistogramData lastDataMarkedAsComplete = lastPair.right.get(lastPair.right.size()-1);
					if( (lastDataMarkedAsComplete.getDate().before(region.getUpperBound()))
							|| (lastPair.left + indexSize > MAX_SIZE_PER_INDEX)) {
						lPair.add(
								new Pair<Double, List<HistogramData>>(indexSize,
										Arrays.asList(histo)));
					} else {
						List<HistogramData> newList = new ArrayList<>();
						newList.addAll(lastPair.getRight());
						newList.add(histo);
						lastPair.setLeft(lastPair.getLeft() + indexSize);
						lastPair.setRight(newList);
					}
				}
				else {
					HistogramData lastDataMarkedAsComplete = lastPair.right.get(lastPair.right.size()-1);
					if( (lastDataMarkedAsComplete.getDate().before(region.getLowerBound()))
							|| (lastPair.left + indexSize > MAX_SIZE_PER_INDEX) // Size check
							|| (UTIL.compareYears(lastDataMarkedAsComplete.getDate(), histo.getDate())>0)) { // YEAR wise partition
						lPair.add(new Pair<Double, List<HistogramData>>(
								indexSize,
								Arrays.asList(histo)));
					} else {
						List<HistogramData> newList = new ArrayList<>();
						newList.addAll(lastPair.getRight());
						newList.add(histo);
						lastPair.setLeft(lastPair.getLeft() + indexSize);
						lastPair.setRight(newList);
					}
				}
			}
		}
	}
}
