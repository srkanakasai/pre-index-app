package com.smarsh.ingestion;

import static com.smarsh.common.Constants.IndexToDataRatio;
import static com.smarsh.common.Constants.MAX_SIZE_PER_INDEX;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import com.smarsh.common.Constants;
import com.smarsh.common.Region;
import com.smarsh.common.UTIL;
import com.smarsh.pojo.HistogramData;
import com.smarsh.pojo.Pair;

public class IndexMetaGeneratorApp {

	private Stream<String> readFile(String fileName) throws IOException{
		InputStream inputStream = this.getClass().getResourceAsStream("/"+fileName);
		InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
		Stream<String> lines = bufferedReader.lines();
		return lines;
	}

	private static void generateIndexeSnapshot(ArrayList<Pair<Double, List<HistogramData>>> groupByIndexSumMax, Region region) {

		String outputFileName = region+"_indexes.txt";
		File newFile = new File(outputFileName);
		if(newFile.exists())
			newFile.delete();
		try {
			if(newFile.createNewFile()) {
				FileWriter fw = new FileWriter(outputFileName);

				System.out.println("Total No. of Indexs for: ******* "+region+" : "+groupByIndexSumMax.size()+" *******");
				fw.write("Total No. of Indexes for: ******* "+region+" : "+groupByIndexSumMax.size()+" *******\n");

				for(int i = 0; i<groupByIndexSumMax.size(); i++) {
					List<HistogramData> subSet = groupByIndexSumMax.get(i).getRight();
					HistogramData startRange = subSet.get(0);
					HistogramData endRange = subSet.get(subSet.size()-1);
					Integer indexMemSize = groupByIndexSumMax.get(i).getLeft().intValue();
					Date startDate = startRange.getDate();
					String indexID = String.format("BNY_%s_data_%s_1000_archive.av5", 
							region.name(), UTIL.getDateForIndex(startDate));

					System.out.println(String.format("IndexID:%s, StartDate:%s, EndDate:%s, MemoryConsumption:%d",
							indexID, startRange.getDateInString(), endRange.getDateInString(), indexMemSize.intValue()));
					fw.write(String.format("IndexID:%s, StartDate:%s, EndDate:%s, MemoryConsumption:%d\n",
							indexID, startRange.getDateInString(), endRange.getDateInString(), indexMemSize.intValue()));
				}

				fw.write("************** END ***********************\n");
				System.out.println("************** END ***********************");

				fw.close();
			}
		} catch (IOException e) {
			System.out.println("Exception in writing the details to output file");
			e.printStackTrace();
		}
	}

	private ArrayList<Pair<Double, List<HistogramData>>> groupByOutliersAndSize(Stream<String> lines, Region region) {
		ArrayList<Pair<Double, List<HistogramData>>> groupByIndexSumMax = lines
				.skip(1)
				.map(line -> {
					String[] data = line.split(",");
					Date date = null;
					try {
						date = Constants.dateFormat.get().parse(data[0]);
					} catch (ParseException e) {
						e.printStackTrace();
					}
					BigDecimal sizeInKB = new BigDecimal(data[2]);
					Integer hour = Integer.parseInt(data[1]);
					BigDecimal indexSizeV2 = sizeInKB.multiply(IndexToDataRatio);
					HistogramData histogramData = new HistogramData(region, date, sizeInKB, hour, indexSizeV2.doubleValue());
					return histogramData;
				})
				.collect(ArrayList<Pair<Double, List<HistogramData>>>::new, Accumulator::apacAggregator, (x, y) -> {});
		return groupByIndexSumMax;
	}

	static class Accumulator {
		public static void apacAggregator(List<Pair<Double, List<HistogramData>>> lPair, HistogramData histo) {
			Pair<Double, List<HistogramData>> lastPair = lPair.isEmpty() ? null : lPair.get(lPair.size() - 1);
			Double indexSize = histo.getIndexSize();
			Region region = histo.getRegion();

			if( Objects.isNull(lastPair)) {
				lPair.add(
						new Pair<Double, List<HistogramData>>(indexSize,
								Arrays.asList(histo)));
			} else if (histo.getDate().before(region .getLowerBound())) {
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
			}
		}
	}

	public static void main(String[] args) throws IOException {
		IndexMetaGeneratorApp app = new IndexMetaGeneratorApp();

		for(Region region : Region.values()) {
			Stream<String> lines = app.readFile(region.getFileName());

			ArrayList<Pair<Double, List<HistogramData>>> groupByIndexSumMax = app.groupByOutliersAndSize(lines, region);

			generateIndexeSnapshot(groupByIndexSumMax, region);
		}


	}

}
