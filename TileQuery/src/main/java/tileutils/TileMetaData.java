package tileutils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import utils.HBaseHelper;

/**
 * Created by Linxu Han on 2018/11/05
 */
public class TileMetaData {

	public static final String tableName = "TileMetaDataTable";

	public static final String[] colFamily = { "MetaDataCF", "TimeVersionCF" };

	public static final String[] quaMetaDataCF = { "Scope", "MaxLevel", "MinLevel" };

	public static final String[] quaTimeVersionCF = { "CreateTime", "VersionNum", "1" };

	/**
	 * Create a table to storage metadata
	 * 
	 * @return boolean
	 * @throws IOException
	 */
	public static boolean createMetaDataTable() throws IOException {
		if (HBaseHelper.tableExist(tableName)) {
			return true;
		} else {
			return HBaseHelper.createTable(tableName, colFamily);
		}
	}

	/**
	 * Get metadata of a tile dataset
	 * 
	 * @param row:row key of dataset
	 * @return Result
	 */
	public static String getScope(String row) {
		Result result = HBaseHelper.getData(tableName, row, colFamily[0], quaMetaDataCF[0]);
		/*
		 * for (Cell C : result.listCells()) { System.out.print("family:" +
		 * Bytes.toString(CellUtil.cloneFamily(C)) + " "); System.out.print("qualifier:"
		 * + Bytes.toString(CellUtil.cloneQualifier(C)) + " ");
		 * System.out.print("value:" + Bytes.toString(CellUtil.cloneValue(C)) + "\n"); }
		 */
		return new String(result.value());
	}

	public static int[] getLevel(String row) {
		Result result = HBaseHelper.getData(tableName, row);
		if (result.isEmpty()) {
			System.out.println("There is no dataset named " + row + " in HBase!");
			System.exit(0);
		}
		int maxLevel = Integer.parseInt(
				Bytes.toString(result.getValue(Bytes.toBytes(colFamily[0]), Bytes.toBytes(quaMetaDataCF[1]))));
		int minLevel = Integer.parseInt(
				Bytes.toString(result.getValue(Bytes.toBytes(colFamily[0]), Bytes.toBytes(quaMetaDataCF[2]))));
		if (minLevel > maxLevel) {
			int tmp = minLevel;
			minLevel = maxLevel;
			maxLevel = tmp;
		}
		return new int[] { minLevel, maxLevel };
	}

	/**
	 * Get time version list of a tile dataset
	 * 
	 * @param row:row key of dataset
	 * @return String[]
	 */
	public static String[] getAllTimeVersion(String row) {
		Result result = HBaseHelper.getData(tableName, row, colFamily[1]);
		List<String> versionList = new ArrayList<String>();
		for (Cell C : result.listCells()) {
			String str = Bytes.toString(CellUtil.cloneQualifier(C));
			if (!str.equals(quaTimeVersionCF[0]) && !str.equals(quaTimeVersionCF[1])) {
				versionList.add(Bytes.toString(CellUtil.cloneValue(C)));
			}
		}
		return versionList.toArray(new String[versionList.size()]);
	}

	public static String getCurrentVersion(String row) {
		String versionNum = new String(HBaseHelper.getData(tableName, row, colFamily[1], quaTimeVersionCF[1]).value());
		return versionNum;
	}

	/**
	 * Insert metadata and time version data
	 * 
	 * @param row:row key of data
	 * @param DSName: data set name
	 * @param scope:coverage range of this dataset
	 * @param Maxlevel:Maxlevel of tile
	 * @param Minlevel:Minlevel of tile
	 * @param CreateTime:Insert time of this dataset
	 * @param DataTime:Creation time of this version
	 * @return void
	 */
	public static void insertMetaData(String row, String Scope, String MaxLevel, String MinLevel, String CreateTime,
			String DataTime) {
		String[] MetaData = { Scope, MaxLevel, MinLevel };
		String[] TimeVersion = { CreateTime, "1", DataTime };
		for (int i = 0; i < MetaData.length; i++) {
			HBaseHelper.insertRow(tableName, row, colFamily[0], quaMetaDataCF[i], Bytes.toBytes(MetaData[i]));
		}
		for (int i = 0; i < TimeVersion.length; i++) {
			HBaseHelper.insertRow(tableName, row, colFamily[1], quaTimeVersionCF[i], Bytes.toBytes(TimeVersion[i]));
		}
	}

	/**
	 * Update time version
	 * 
	 * @param row:row key of dataset
	 * @param DataTime:Creation time of this version
	 * @return boolean
	 */
	public static boolean updateTimeVersion(String row, String DataTime) {
		Result result = HBaseHelper.getData(tableName, row, colFamily[1], quaTimeVersionCF[1]);
		if (result.isEmpty()) {
			HBaseHelper.insertRow(tableName, row, colFamily[1], quaTimeVersionCF[1], Bytes.toBytes("1"));
			return HBaseHelper.insertRow(tableName, row, colFamily[1], "1", Bytes.toBytes(DataTime));
		} else {
			int VersionNum = Integer.parseInt(new String(result.value())) + 1;
			HBaseHelper.insertRow(tableName, row, colFamily[1], quaTimeVersionCF[1],
					Bytes.toBytes(String.valueOf(VersionNum)));
			return HBaseHelper.insertRow(tableName, row, colFamily[1], String.valueOf(VersionNum),
					Bytes.toBytes(DataTime));
		}
	}

	/**
	 * Get datatable name of a data set
	 * 
	 * @param row:row key of data set
	 * @return String[]
	 */
	public static String[] getDataTableName(String row) {
		String[] tableName = { row + "L", row + "H" };
		return tableName;
	}

	public static String getTimeVersion(String row, String Time) {
		Result result = HBaseHelper.getData(tableName, row, colFamily[1]);
		String timeVersion;
		for (Cell C : result.listCells()) {
			String str = Bytes.toString(CellUtil.cloneQualifier(C));
			if (!str.equals(quaTimeVersionCF[0]) && !str.equals(quaTimeVersionCF[1])
					&& Time.equals(Bytes.toString(CellUtil.cloneValue(C)))) {
				timeVersion = str;
				return timeVersion;
			}
		}
		return null;
	}

	public static Map<String, String> getTimeVersion(String row, String startTime, String endTime)
			throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date start = sdf.parse(startTime);
		Date end = sdf.parse(endTime);
		if (end.before(start)) {
			Date tmp = start;
			start = end;
			end = tmp;
		}
		Result result = HBaseHelper.getData(tableName, row, colFamily[1]);
		Map<String, String> versionList = new HashMap<String, String>();
		for (Cell C : result.listCells()) {
			String str = Bytes.toString(CellUtil.cloneQualifier(C));
			if (!str.equals(quaTimeVersionCF[0]) && !str.equals(quaTimeVersionCF[1])) {
				Date time = sdf.parse(Bytes.toString(CellUtil.cloneValue(C)));
				if (start.before(time) && time.before(end)) {
					versionList.put(str, Bytes.toString(CellUtil.cloneValue(C)));
				}
			}
		}
		if (versionList.isEmpty()) {
			System.out.println(
					"There is no corresponding version between [" + startTime + "," + endTime + "] of this data set");
			System.exit(0);
		}
		return versionList;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		boolean isConnected = HBaseHelper.connect();
		// createMetaDataTable();
		// insertMetaData("1", "WorldMap", "(-180,180),(-90,90)", "0", "8",
		// "2018-11-04", "2018-11-05");
		// updateTimeVersion("1", "2018-11-06");
		// getMetaData("1");
		String[] str = getScope("Thailand").split("\\(|,|\\)|\\s+");
		System.out.println(str[0].equals(""));
		for (String s : str) {
			System.out.println(s);
		}
		// for (String s : str) {
		// System.out.println("Version Date is " + s);
		// }
		HBaseHelper.close();
		// SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		// Date currTime = new Date();
		// String curTime = formatter.format(currTime);
		// System.out.println(curTime);
	}

}
