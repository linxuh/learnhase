import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.dtools.ini.IniSection;

import iniFile.ReadUpdateIni;
import tileRowkeyutils.Rowkey;
import tileutils.TileMetaData;
import tileutils.TileReadWrite;
import txtutil.TxtTranslate;
import utils.HBaseHelper;

/**
 * Created by Liebing Yu (huleryo.ylb@cug.edu.cn) on 2018/9/28
 */
public class Main {

	public static void main(String[] args) throws Exception {
		HBaseHelper.connect();
		long startTime = System.currentTimeMillis();
		// String[] parameters = TxtTranslate.readTxt(args[0]);
		String filename = "example.ini";
		if (args.length > 0) {
			filename = args[0];
		}
		IniSection parameters = ReadUpdateIni.readIniFile(filename, "Query");
		explainParameter(parameters);
		long endTime = System.currentTimeMillis(); // 获取结束时间
		System.out.println("QueryTime:" + (endTime - startTime) + "ms"); // 输出程序运行时间
		HBaseHelper.close();
		System.exit(0);
	}

	public static double MAXLAT = 85.05112878;
	public static double MAXLON = 180;
	public static double MINLAT = -85.05112878;
	public static double MINLON = -180;

	public static void explainParameter(IniSection parameters) throws IOException, ParseException {
		// 查询
		int level = Integer.valueOf(parameters.getItem("Level").getValue());
		String datasetName = parameters.getItem("DatasetName").getValue();
		int[] levelbound = TileMetaData.getLevel(datasetName);
		if (level < levelbound[0] || level > levelbound[1]) {
			System.out.println("There is no tile in level " + level + " ,please input level number between "
					+ levelbound[0] + "and " + levelbound[1]);
			System.exit(0);
		}
		File writeName = new File("result/QueryOutput.txt");
		FileWriter writer = new FileWriter(writeName, true);
		BufferedWriter out = new BufferedWriter(writer);
		Date date = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
		long startTime = System.currentTimeMillis();
		if (parameters.hasItem("Row") && parameters.hasItem("Column") && !parameters.hasItem("Time")) {
			out.write("==================================================================\r\nSingle point Query\t"
					+ dateFormat.format(date).toString() + "\r\n");
			String row = parameters.getItem("Row").getValue();
			String column = parameters.getItem("Column").getValue();
			Table tb = HBaseHelper.getTableObject(TileMetaData.getDataTableName(datasetName)[0]);
			byte[] bs = TileReadWrite.getLTile(tb, String.valueOf(level), row, column);
			String filename = String.valueOf(level) + "_" + row + "_" + column;
			if (bs != null && bs.length > 334) {
				TileReadWrite.write("result/" + filename + ".png", bs);
				out.write("Tilelevel: " + level + ", Tilerow:" + row + ", Tilecolumn:" + column + "\r\n");
				System.out.println("Get tile successfully! Stored in /home/tiletest/result/" + filename + ".png");
			} else {
				System.out.println("Can't find results under these conditions!");
				System.exit(0);
			}

		} else if (parameters.hasItem("Row") && parameters.hasItem("Column") && parameters.hasItem("Time")) {
			out.write("==================================================================\r\nSingle point Query\t"
					+ dateFormat.format(date).toString() + "\r\n");
			String row = parameters.getItem("Row").getValue();
			String column = parameters.getItem("Column").getValue();
			String time = parameters.getItem("Time").getValue();
			String timeVersion = TileMetaData.getTimeVersion(datasetName, time);
			Table tb = HBaseHelper.getTableObject(TileMetaData.getDataTableName(datasetName)[1]);
			byte[] bs = null;
			if (timeVersion != null) {
				bs = TileReadWrite.getHTile(tb, timeVersion, String.valueOf(level), row, column);
				out.write("Tilelevel: " + level + ", Tilerow:" + row + ", Tilecolumn:" + column + ", TimeVersion:"
						+ time + "\r\n");
			}
			if (bs != null && bs.length > 334) {
				String filename = String.valueOf(level) + "_" + row + "_" + column;
				TileReadWrite.write("result/" + filename + ".png", bs);
				System.out.println("Get tile successfully! Stored in /home/tiletest/result/" + filename + ".png");
			} else {
				System.out.println("Can't find results under these conditions!");
				System.exit(0);
			}

		} else if (parameters.hasItem("MaxLat") && parameters.hasItem("MinLon") && parameters.hasItem("MaxLon")
				&& parameters.hasItem("MinLat") && !parameters.hasItem("EndTime") && !parameters.hasItem("StartTime")) {
			out.write("==================================================================\r\nSpatial Query\t"
					+ dateFormat.format(date).toString() + "\r\n");
			Table tb = HBaseHelper.getTableObject(TileMetaData.getDataTableName(datasetName)[0]);
			double MinLat = Double.valueOf(parameters.getItem("MinLat").getValue());
			double MinLon = Double.valueOf(parameters.getItem("MinLon").getValue());
			double MaxLat = Double.valueOf(parameters.getItem("MaxLat").getValue());
			double MaxLon = Double.valueOf(parameters.getItem("MaxLon").getValue());
			double[] box = judgeQuery(datasetName, MinLat, MaxLat, MinLon, MaxLon);
			TxtTranslate.writeTxt("/var/www/wmts/tilequery.html", level, box[0], box[1], box[2], box[3]);
			out.write("BoundingBox:[" + box[2] + "," + box[0] + "] ==> [" + box[3] + "," + box[1]
					+ "]\r\n\nQueryResults:\n");
			spatialQuery(tb, level, box[0], box[1], box[2], box[3], out);
			System.out.println("Get tiles successfully! Stored in \"/home/tiletest/result/\"");

		} else if (parameters.hasItem("MaxLat") && parameters.hasItem("MinLon") && parameters.hasItem("MaxLon")
				&& parameters.hasItem("MinLat") && parameters.hasItem("EndTime") && parameters.hasItem("StartTime")) {
			double MinLat = Double.valueOf(parameters.getItem("MinLat").getValue());
			double MinLon = Double.valueOf(parameters.getItem("MinLon").getValue());
			double MaxLat = Double.valueOf(parameters.getItem("MaxLat").getValue());
			double MaxLon = Double.valueOf(parameters.getItem("MaxLon").getValue());
			String starttime = parameters.getItem("StartTime").getValue();
			String endtime = parameters.getItem("EndTime").getValue();
			Map<String, String> versionList = TileMetaData.getTimeVersion(datasetName, starttime, endtime);
			out.write("==================================================================\r\nTime-Spatial Query\t"
					+ dateFormat.format(date).toString() + "\r\nQuery TimeRange:" + starttime + " ==> " + endtime
					+ "\r\n");
			Table tb = HBaseHelper.getTableObject(TileMetaData.getDataTableName(datasetName)[1]);
			double[] box = judgeQuery(datasetName, MinLat, MaxLat, MinLon, MaxLon);
			TxtTranslate.writeTxt("/var/www/wmts/tilequery.html", level, box[0], box[1], box[2], box[3]);
			out.write("BoundingBox:[" + box[2] + "," + box[0] + "] ==> [" + box[3] + "," + box[1]
					+ "]\r\n\nQueryResults:\n");
			timeSpatialQuery(tb, level, box[0], box[1], box[2], box[3], versionList, out);
			System.out.println("Get tiles successfully! Stored in \"/home/tiletest/result/\"");
		} else {
			System.out.println("Input parameters error!\n Please check example.ini file! ");
			System.exit(0);
		}
		long endTime = System.currentTimeMillis(); // 获取结束时间
		out.write("QueryTime:" + (endTime - startTime) + "ms\r\n");
		out.close();
	}

	public static void spatialQuery(Table table, int level, double minLat, double maxLat, double minLon, double maxLon,
			BufferedWriter out) throws IOException {
		Map<String, String> map = new HashMap<String, String>();
		int count = 0;
		double[] queryresultbox = new double[] { (minLat + maxLat) / 2, (minLat + maxLat) / 2, (minLon + maxLon) / 2,
				(minLon + maxLon) / 2 };
		int[] rowcolmin = Rowkey.getTileNumber(level, maxLat, minLon);
		int[] rowcolmax = Rowkey.getTileNumber(level, minLat, maxLon);
		for (int row = rowcolmin[0]; row <= rowcolmax[0]; row++) {
			for (int col = rowcolmin[1]; col <= rowcolmax[1]; col++) {
				map.put(Rowkey.getRowkey(level, row, col),
						String.valueOf(level) + "_" + String.valueOf(row) + "_" + String.valueOf(col));
			}
		}
		Result[] results = HBaseHelper.batchGetData(table, map.keySet().toArray(new String[map.size()]));
		for (Result re : results) {
			if (re.value() != null && re.value().length > 334) {
				count++;
				String[] levelrowcol = map.get(new String(re.getRow())).split("_");
				double[] range = Rowkey.tile2boundingBox(Integer.parseInt(levelrowcol[2]),
						Integer.parseInt(levelrowcol[1]), level);
				TileReadWrite.write("result/" + map.get(new String(re.getRow())) + ".png", re.value());
				queryresultbox = getResultBox(queryresultbox, range);
				out.write("Tilelevel: " + level + ", Tilerow:" + levelrowcol[1] + ", Tilecolumn:" + levelrowcol[2]
						+ ", TileRange: [" + range[2] + "," + range[0] + "]==>[" + range[3] + "," + range[1] + "]\r\n");
			}
		}
		if (count == 0) {
			System.out.println("Can't find results under these conditions!");
			System.exit(0);
		} else {
			System.out.println("Total number of query results :" + count);
			out.write("Total number of query results :" + count + "\r\nScope of the query results: ["
					+ queryresultbox[2] + "," + queryresultbox[0] + "]==>[" + queryresultbox[3] + ","
					+ queryresultbox[1] + "]\r\n");
		}
		out.flush();
	}

	public static void timeSpatialQuery(Table table, int level, double minLat, double maxLat, double minLon,
			double maxLon, Map<String, String> timeVersion, BufferedWriter out) throws IOException {
		Map<String, String> rowkey = new HashMap<String, String>();
		double[] queryresultbox = new double[] { (minLat + maxLat) / 2, (minLat + maxLat) / 2, (minLon + maxLon) / 2,
				(minLon + maxLon) / 2 };
		int[] rowcolmin = Rowkey.getTileNumber(level, maxLat, minLon);
		int[] rowcolmax = Rowkey.getTileNumber(level, minLat, maxLon);
		int count = 0;
		for (Entry<String, String> version : timeVersion.entrySet()) {
			for (int row = rowcolmin[0]; row <= rowcolmax[0]; row++) {
				for (int col = rowcolmin[1]; col <= rowcolmax[1]; col++) {
					rowkey.put(Rowkey.getRowkey(level, row, col),
							String.valueOf(level) + "_" + String.valueOf(row) + "_" + String.valueOf(col));
				}
			}
			Result[] result = HBaseHelper.batchGetData(table, rowkey.keySet().toArray(new String[rowkey.size()]),
					TileReadWrite.colFamily[1], version.getKey());

			for (Result re : result) {
				byte[] b = re.value();
				if (b != null && b.length > 334) {
					count++;
					String[] levelrowcol = rowkey.get(new String(re.getRow())).split("_");
					double[] range = Rowkey.tile2boundingBox(Integer.parseInt(levelrowcol[2]),
							Integer.parseInt(levelrowcol[1]), level);
					TileReadWrite.write(
							"result/" + rowkey.get(new String(re.getRow())) + "_" + version.getValue() + ".png",
							re.value());
					queryresultbox = getResultBox(queryresultbox, range);
					out.write("Tilelevel: " + level + ", Tilerow:" + levelrowcol[1] + ", Tilecolumn:" + levelrowcol[2]
							+ ", Tiletime:" + version.getValue() + ", TileRange: [" + range[2] + "," + range[0]
							+ "]==>[" + range[3] + "," + range[1] + "]\r\n");

				}
			}
		}
		if (count == 0) {
			System.out.println("Can't find results under these conditions!");
			System.exit(0);
		} else {
			System.out.println("Total number of query results :" + count);
			out.write("Total number of query results :" + count + "\r\nScope of the query results: ["
					+ queryresultbox[2] + "," + queryresultbox[0] + "]==>[" + queryresultbox[3] + ","
					+ queryresultbox[1] + "]\r\n");
		}
		out.flush();

	}

	public static double[] judgeQuery(String datasetName, double minLat, double maxLat, double minLon, double maxLon) {
		if (minLat > maxLat) {
			double tmp = minLat;
			minLat = maxLat;
			maxLat = tmp;
		}
		if (minLon > maxLon) {
			double tmp = minLon;
			minLon = maxLon;
			maxLon = tmp;
		}
		if (minLat > MAXLAT || maxLat < MINLAT || minLon > MAXLON || maxLon < MINLON) {
			System.out.println("The input latitude and longitude is outside the bounding box:[" + MINLAT + "," + MINLON
					+ "],[" + MAXLAT + "," + MAXLON + "]");
			System.exit(0);
		}
		String[] scope = TileMetaData.getScope(datasetName).split("\\(|,|\\)|\\s+");
		if (minLat > Double.parseDouble(scope[6]) || maxLat < Double.parseDouble(scope[5])
				|| minLon > Double.parseDouble(scope[2]) || maxLon < Double.parseDouble(scope[1])) {
			System.out.println("The input latitude and longitude is outside the dataset scope:[" + scope[5] + ","
					+ scope[1] + "],[" + scope[6] + "," + scope[2] + "]");
			System.exit(0);
		}
		if (maxLat > Double.parseDouble(scope[6])) {
			maxLat = Double.parseDouble(scope[6]);
		}
		if (maxLon > Double.parseDouble(scope[2])) {
			maxLon = Double.parseDouble(scope[2]);
		}
		if (minLat < Double.parseDouble(scope[5])) {
			minLat = Double.parseDouble(scope[5]);
		}
		if (minLon < Double.parseDouble(scope[1])) {
			minLon = Double.parseDouble(scope[1]);
		}
		return new double[] { minLat, maxLat, minLon, maxLon };
	}

	public static double[] getResultBox(double[] resultBox, double[] range) {
		resultBox[0] = resultBox[0] > range[0] ? range[0] : resultBox[0];
		resultBox[1] = resultBox[1] < range[1] ? range[1] : resultBox[1];
		resultBox[2] = resultBox[2] > range[2] ? range[2] : resultBox[2];
		resultBox[3] = resultBox[3] < range[3] ? range[3] : resultBox[3];
		return resultBox;
	}
}
