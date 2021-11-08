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
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.smarsh.common.Region;
import com.smarsh.common.UTIL;
import com.smarsh.pojo.HistogramData;
import com.smarsh.pojo.Pair;
import com.smarsh.pojo.PreIndexMetaData;

public class IndexMetaGeneratorService {

	private static Logger logger = LogManager.getLogger(IndexMetaGeneratorService.class);
	private static Long MAX_SIZE_PER_INDEX = 200000000l;

	public void generatePreIndexes(PreIndexMetaData metaData, List<Region> regions) {
		logger.info("***generatePreIndexes STARTS****");
		try {
			
			MAX_SIZE_PER_INDEX = UTIL.getMaxSizePerIndexInGB(
										metaData.getMaxNumOfShardsPerIndex(),
										metaData.getShardSizeInGB(),
										metaData.getFillPercentage())*mega;
			
			for(Region region : regions) {
				Stream<String> lines = UTIL.readFile(region.getFileName(),this);

				ArrayList<Pair<Double, List<HistogramData>>> groupByIndexSumMax = this.groupByOutliersAndSize(
						lines, 
						metaData,
						region);

				generateIndexSnapshot(groupByIndexSumMax, metaData, region);
			}
		} catch(IOException e) {
			logger.error("Exception in reading the histogram files", e);
		} finally {
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

				logger.debug("Total No. of Indexs for: ******* "+region+" : "+groupByIndexSumMax.size()+" *******");
				fw.write("Total No. of Indexes for: ******* "+region+" : "+groupByIndexSumMax.size()+" *******\n");

				for(int i = 0; i<groupByIndexSumMax.size(); i++) {
					List<HistogramData> subSet = groupByIndexSumMax.get(i).getRight();
					HistogramData startRange = subSet.get(0);
					HistogramData endRange = subSet.get(subSet.size()-1);
					Integer indexMemSize = groupByIndexSumMax.get(i).getLeft().intValue();
					Date startDate = startRange.getDate();
					String indexID = String.format("BNY_%s_data_%s_1000_archive.av5", 
							region.name(), UTIL.getDateForIndex(startDate));

					logger.debug(String.format("IndexID:%s, StartDate:%s, EndDate:%s, MemoryConsumption:%d",
							indexID, startRange.getDateInString(), endRange.getDateInString(), indexMemSize.intValue()));
					fw.write(String.format("[%s-%s]\t:\tIndexID:%s, MemoryConsumption:%d\n",
							startRange.getDateInString(), endRange.getDateInString(), indexID, indexMemSize.intValue()));
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
			
			
			/*else if (histo.getDate().before(region.getLowerBound())) {
				if(lastPair.left + indexSize > MAX_SIZE_PER_INDEX) {
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
			} else if(histo.getDate().after(region.getUpperBound())) {
				HistogramData lastDataMarkedAsComplete = lastPair.right.get(lastPair.right.size()-1);
				if(lastDataMarkedAsComplete.getDate().before(region.getUpperBound())) {
					lPair.add(
							new Pair<Double, List<HistogramData>>(indexSize,
									Arrays.asList(histo)));
				} else if(lastPair.left + indexSize > MAX_SIZE_PER_INDEX) {
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
				HistogramData lastDataMarkedAsComplete = lastPair.right.get(lastPair.right.size()-1);
				if(lastDataMarkedAsComplete.getDate().before(region.getLowerBound())) {
					lPair.add(
							new Pair<Double, List<HistogramData>>(indexSize,
									Arrays.asList(histo)));
				} else if(lastPair.left + indexSize > MAX_SIZE_PER_INDEX) {
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
			}*/
		}
	}
}
