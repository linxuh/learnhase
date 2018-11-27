package tileutils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import tileRowkeyutils.Rowkey;
import utils.HBaseHelper;

/**
 * Created by Liebing Yu (huleryo.ylb@cug.edu.cn) on 2018/9/27
 * 
 * Update by Linxu Han on 2018/11/05
 */
public class TileReadWrite {

	public static final String quaHTileCF = "TList";

	public static final String quaLTileCF = "T";

	public static final String[] colFamily = { "LCF", "HCF" };

	/**
	 * Read picture from disk to bytes.
	 * 
	 * @param path: picture path on disk.
	 * @return byte[]
	 */
	public static byte[] read(String path) throws IOException {
		FileInputStream fis = new FileInputStream(path);
		byte[] bytes = new byte[fis.available()];
		fis.read(bytes);
		fis.close();

		return bytes;
	}

	public static byte[] read() {
		return new byte[1];
	}

	/**
	 * Write picture bytes to disk.
	 * 
	 * @param path: path to be write.
	 * @param bytes: bytes to be write.
	 * @return void
	 */
	public static void write(String path, byte[] bytes) throws IOException {
		FileOutputStream fos = new FileOutputStream(path);
		fos.write(bytes);
		fos.close();
	}

	/**
	 * Write bytes value to a HBase table.
	 * 
	 * @param tbName: table name
	 * @param row: row key
	 * @param cf: column family
	 * @param qua: qualifier
	 * @param value: bytes value
	 * @return boolean
	 */
	public static boolean write(Table tbName, String row, String cf, String qua, byte[] value) {
		return HBaseHelper.insertRow(tbName, row, cf, qua, value);
	}

	/**
	 * Write tile in path to HBase.
	 * 
	 * @param path: tile path on disk
	 * @param tbName: HBase table name
	 * @param row: row key
	 * @param cf: column family
	 * @param qua: qualifier
	 * @return boolean
	 */
	public static boolean write(String path, Table tbName, String row, String cf, String qua) {
		byte[] bytes;
		// if cannot read path, return false.
		try {
			bytes = read(path);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return write(tbName, row, cf, qua, bytes);
	}

	/**
	 * Write bytes value in bulk to a HBase table.
	 * 
	 * @param tbName: table name
	 * @param row[]:several row key
	 * @param cf: column family
	 * @param qua: qualifier
	 * @param bytea: bytes value
	 * @return boolean
	 */
	public static boolean batchWrite(Table tb, String[] row, String cf, String qua, List<byte[]> bytes) {
		return HBaseHelper.batchInsertRow(tb, row, cf, qua, bytes);
	}

	/**
	 * Write tile in bulk to HBase.
	 * 
	 * @param fileName: tile path on disk
	 * @param tbName: HBase table name
	 * @param row[]: several row key
	 * @param cf: column family
	 * @param qua: qualifier
	 * @return boolean
	 */
	public static boolean batchWrite(File[] fileName, Table tb, String[] row, String cf, String qua) {
		List<byte[]> bytes = new ArrayList<byte[]>();
		// List<String> row = new ArrayList<String>();
		for (File path : fileName) {
			try {
				bytes.add(read(path.getAbsolutePath()));
				// row.add(Rowkey.getRowkey(level, Rowkey.getRightRowNumber(path, level), col));
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}
		}
		// byte[][] data = bytes.toArray(new byte[bytes.size()][]);
		// String [] rowkey = row.toArray(new String[row.size()]);
		return batchWrite(tb, row, cf, qua, bytes);
	}

	/**
	 * Create historicalTile data table and latestTile data table.
	 * 
	 * @param row: row key of metadata table
	 * @return String[2] table name
	 * 
	 */
	public static String[] createDataTable(String row) {
		Result dsname = HBaseHelper.getData(TileMetaData.tableName, row);
		String[] tableName = TileMetaData.getDataTableName(row);
		if (!dsname.isEmpty()) {
			for (int i = 0; i < tableName.length; i++) {
				HBaseHelper.createTable(tableName[i], new String[] { colFamily[i] });
			}
		} else {
			System.out.println(row + "dataset is not exsit!");
		}
		return tableName;
	}

	/**
	 * Insert tile to HBase table.
	 * 
	 * @param path: tile path on disk
	 * @param tablename: data table name
	 * @param row: row key of tile
	 * @param version:timeversion of tile
	 * @return void
	 */
	public static void insertTile(String path, Table[] tablename, String row, String version) {
		Result tList = HBaseHelper.getData(tablename[1], row, colFamily[1], quaHTileCF);
		if (tList.isEmpty()) {
			HBaseHelper.insertRow(tablename[1], row, colFamily[1], quaHTileCF, Bytes.toBytes(version));
		} else {
			String newList = tList.value() + "," + version;
			HBaseHelper.insertRow(tablename[1], row, colFamily[1], quaHTileCF, Bytes.toBytes(newList));
		}
		write(path, tablename[1], row, colFamily[1], version);
		write(path, tablename[0], row, colFamily[0], quaLTileCF);
	}

	/**
	 * Insert tile in bulk to HBase table.
	 * 
	 * @param fileName: tile path on disk
	 * @param tablename: data table name
	 * @param row: row key of several tile
	 * @param version:timeversion of tile
	 * @return void
	 */
	public static void batchInsertTile(File[] fileName, Table[] tablename, String[] row, String version) {

		Result[] tList = HBaseHelper.batchGetData(tablename[1], row, colFamily[1], quaHTileCF);
		for (int i = 0; i < row.length; i++) {
			if (tList[i].isEmpty()) {
				HBaseHelper.insertRow(tablename[1], row[i], colFamily[1], quaHTileCF, Bytes.toBytes(version));
			} else {
				String newList = tList[i].value() + "," + version;
				HBaseHelper.insertRow(tablename[1], row[i], colFamily[1], quaHTileCF, Bytes.toBytes(newList));
			}
		}
		batchWrite(fileName, tablename[1], row, colFamily[1], version);
		batchWrite(fileName, tablename[0], row, colFamily[0], quaLTileCF);
	}

	/**
	 * Get latest tile from a HBase table.
	 * 
	 * @param tbName: table name
	 * @param level: tile level
	 * @param row: tile row
	 * @param col: tile column
	 * @return byte[]
	 */
	public static byte[] getLTile(String tbName, String level, String row, String col) {
		String rowkey = Rowkey.getRowkey(level,
				String.valueOf(Rowkey.getRightRowNumber(Integer.parseInt(row), Integer.parseInt(level))), col);
		Result result = HBaseHelper.getData(tbName, rowkey);
		byte[] bs = result.value();
		// TileReadWrite.write("D:\\" + rowkey + ".png", bs);
		return bs;
	}

	/**
	 * Get historical tile from a HBase table..
	 * 
	 * @param tbName: HBase table name
	 * @param level: tile level
	 * @param col: tile column
	 * @param row: tile row
	 * @param cf: column family
	 * @param qua: qualifier
	 * @return boolean
	 */
	public static byte[] getHTile(String tbName, String version, String level, String row, String col) {
		String rowkey = Rowkey.getRowkey(level,
				String.valueOf(Rowkey.getRightRowNumber(Integer.parseInt(row), Integer.parseInt(level))), col);
		Result result = HBaseHelper.getData(tbName, rowkey);
		byte[] bs;
		String[] timeVersion = new String(result.getValue(Bytes.toBytes(colFamily[1]), Bytes.toBytes(quaHTileCF)))
				.split(",");
		List<String> tempList = Arrays.asList(timeVersion);
		if (tempList.contains(version)) {
			bs = result.getValue(Bytes.toBytes(colFamily[1]), Bytes.toBytes(version));
		} else {
			bs = getLTile(tbName, level, row, col);
		}
		return bs;
	}

	public static void main(String[] args) throws IOException {
		List<byte[]> bytes = new ArrayList<byte[]>();
		bytes.add(TileReadWrite.read("C:\\Users\\80784_000\\Desktop\\tif\\files\\0\\0\\0.png"));
		// System.out.println(bytes.toString());
		// TileReadWrite.write("C:\\Users\\80784_000\\Desktop\\1.jpg", bytes);
		if (HBaseHelper.connect()) {
			// boolean isWrite = TileReadWrite.write("MapTileTest", "row5", "cof1", "q4",
			// bytes.get(0));
			// System.out.println(isWrite);
		}
	}
}
