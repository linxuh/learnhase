import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.client.Table;
import org.dtools.ini.IniSection;

import iniFile.ReadUpdateIni;
import tileRowkeyutils.Rowkey;
import tileutils.FileUtils;
import tileutils.TileMetaData;
import tileutils.TileReadWrite;
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
		IniSection parameters = ReadUpdateIni.readIniFile(filename, "LoadData");
		explainParameter(parameters);
		long endTime = System.currentTimeMillis(); // 获取结束时间
		System.out.println("LoadTime：" + (endTime - startTime) + "ms"); // 输出程序运行时间
		HBaseHelper.close();
	}

	public static void explainParameter(IniSection parameters) throws IOException {
		String datasetName = parameters.getItem("DatasetName").getValue();
		File writeName = new File("result/LoadOutput.txt");
		FileWriter writer = new FileWriter(writeName, true);
		BufferedWriter out = new BufferedWriter(writer);
		Date date = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
		long startTime = System.currentTimeMillis();
		if (parameters.hasItem("Scope") && parameters.hasItem("MinLevel") && parameters.hasItem("MaxLevel")
				&& parameters.hasItem("DataTime")) {
			TileMetaData.createMetaDataTable();
			TileMetaData.insertMetaData(datasetName, parameters.getItem("Scope").getValue(),
					parameters.getItem("MaxLevel").getValue(), parameters.getItem("MinLevel").getValue(),
					dateFormat.format(date).toString(), parameters.getItem("DataTime").getValue());
			String[] tableName = TileReadWrite.createDataTable(datasetName);
			String currentVersion = TileMetaData.getCurrentVersion(datasetName);
			Table[] tables = HBaseHelper.getTableObject(tableName);
			out.write("==================================================================\r\nInsert " + datasetName
					+ " dataset\t" + dateFormat.format(date).toString() + "\r\n");
			out.write("Scope:" + parameters.getItem("Scope").getValue() + "\r\nLevel: "
					+ parameters.getItem("MinLevel").getValue() + " ==> " + parameters.getItem("MaxLevel").getValue()
					+ "\r\nDataTime:" + parameters.getItem("DataTime").getValue() + "\r\n");
			batchload(parameters.getItem("DataPath").getValue(), tables, currentVersion, out);
		}
		// 更新数据集
		else if (parameters.hasItem("DataTime")) {
			TileMetaData.updateTimeVersion(datasetName, parameters.getItem("DataTime").getValue());
			String[] tableName = TileMetaData.getDataTableName(datasetName);
			String currentVersion = TileMetaData.getCurrentVersion(datasetName);
			Table[] tables = HBaseHelper.getTableObject(tableName);
			out.write("==================================================================\r\nUpdate " + datasetName
					+ " dataset\t" + dateFormat.format(date).toString() + "\r\n");
			out.write("DataTime:" + parameters.getItem("DataTime").getValue() + "\r\n");
			batchload(parameters.getItem("DataPath").getValue(), tables, currentVersion, out);
		} else {
			System.out.println("Input parameters error!\n Please check example.ini file! ");
			System.exit(0);
		}
		long endTime = System.currentTimeMillis(); // 获取结束时间
		out.write("LoadTime:" + (endTime - startTime) + "ms\r\n");
		out.close();
	}

	public static void load(String path, Table[] tables, String currentVersion) {
		File[] layers = FileUtils.listDirectories(path);
		for (File layer : layers) {
			File[] cols = FileUtils.listDirectories(layer.getAbsolutePath());
			for (File col : cols) {
				File[] rows = FileUtils.listPng(col.getAbsolutePath());
				for (File row : rows) {
					String rowkey = Rowkey.getRowkey(layer.getName(), Rowkey.getRightRowNumber(row, layer.getName()),
							col.getName());/* 不带扩展名的文件后缀 */
					TileReadWrite.insertTile(row.getAbsolutePath(), tables, rowkey, currentVersion);
					System.out.println(
							String.format("Inserted %s,%s,%s...", layer.getName(), col.getName(), row.getName()));
				}
			}
		}
	}

	public static void batchload(String path, Table[] tables, String currentVersion, BufferedWriter out)
			throws IOException {
		int count = 0;
		File[] layers = FileUtils.listDirectories(path);
		for (File layer : layers) {
			File[] cols = FileUtils.listDirectories(layer.getAbsolutePath());
			for (File col : cols) {
				List<String> rowkeys = new ArrayList<String>();
				File[] rows = FileUtils.listPng(col.getAbsolutePath());
				for (File row : rows) {
					rowkeys.add(Rowkey.getRowkey(layer.getName(), Rowkey.getRightRowNumber(row, layer.getName()),
							col.getName()));/* 不带扩展名的文件后缀 */
				}
				TileReadWrite.batchInsertTile(rows, tables, rowkeys.toArray(new String[rowkeys.size()]),
						currentVersion);
				count += rows.length;
				System.out.println(String.format("Inserted level:%s, Column:%s...", layer.getName(), col.getName()));
			}
		}
		out.write(String.format("Insert %d tiles in LatestTable and HistoricalTable respectively\r\n", count));
		System.out.println(String.format("Insert %d tiles in LatestTable and HistoricalTable respectively", count));
	}

}
