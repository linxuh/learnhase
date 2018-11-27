package utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by Liebing Yu (huleryo.ylb@cug.edu.cn) on 2018/9/28
 * 
 * Update by Linxu Han on 2018/11/7
 */
public class HBaseHelper {

	private static Configuration configuration;
	private static Connection connection;
	private static Admin admin;

	private HBaseHelper() {
	}

	/**
	 * Connect to HBase.
	 * 
	 * @return boolean
	 */
	public static boolean connect() {
		boolean isConnected = false;
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.rootdir", "hdfs://master:9000/hbase");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");
		// try to connect to HBase.
		try {
			connection = ConnectionFactory.createConnection(configuration);
			admin = connection.getAdmin();
			isConnected = true;
		} catch (IOException e) {
			e.printStackTrace();
			isConnected = false;
		}
		return isConnected;
	}

	/**
	 * Close the connection to HBase.
	 */
	public static void close() {
		try {
			if (admin != null) {
				admin.close();
			}
			if (connection != null) {
				connection.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static boolean tableExist(String name) throws IOException {
		TableName tn = TableName.valueOf(name);
		if (admin.tableExists(tn)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Create a new table in HBase.
	 * 
	 * @param tbName: table name.
	 * @param colFs: column families.
	 * @return boolean
	 */
	public static boolean createTable(String tbName, String[] colFs) {
		boolean isCreated;
		TableName tableName = TableName.valueOf(tbName);
		// if table is already exists, return false.`
		try {
			if (admin.tableExists(tableName))
				return false;
		} catch (IOException e) {
			e.printStackTrace();
		}
		// try to create table.
		HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
		for (String str : colFs) {
			HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
			hTableDescriptor.addFamily(hColumnDescriptor);
		}
		try {
			admin.createTable(hTableDescriptor);
			isCreated = true;
		} catch (IOException e) {
			e.printStackTrace();
			isCreated = false;
		}
		return isCreated;
	}

	/**
	 * Delete table exists in HBase.
	 * 
	 * @param tbName: name of table need to be delete.
	 * @return boolean
	 */
	public static boolean deleteTable(String tbName) {
		boolean isDeleted;
		TableName tableName = TableName.valueOf(tbName);
		// if table is not existed, return false.
		try {
			if (!admin.tableExists(tableName))
				return false;
		} catch (IOException e) {
			e.printStackTrace();
		}
		// try to delete table.
		try {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			isDeleted = true;
		} catch (IOException e) {
			e.printStackTrace();
			isDeleted = false;
		}
		return isDeleted;
	}

	/**
	 * Put an row to a specific table.
	 * 
	 * @param tbName: table name.
	 * @param row: row key
	 * @param cf: column family
	 * @param qua: qualifier
	 * @param bytes: bytes value
	 * @return boolean
	 */
	public static boolean insertRow(String tbName, String row, String cf, String qua, byte[] bytes) {
		boolean isPutted;
		Table table;
		// if cannot connect to table, return false.
		try {
			table = connection.getTable(TableName.valueOf(tbName));
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		Put put = new Put(Bytes.toBytes(row));
		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qua), bytes);
		try {
			table.put(put);
			table.close();
			isPutted = true;
		} catch (IOException e) {
			e.printStackTrace();
			isPutted = false;
		}
		return isPutted;
	}

	/**
	 * Put an row to a specific table.
	 * 
	 * @param table: table object.
	 * @param row: row key
	 * @param cf: column family
	 * @param qua: qualifier
	 * @param bytes: bytes value
	 * @return boolean
	 */
	public static boolean insertRow(Table table, String row, String cf, String qua, byte[] bytes) {
		boolean isPutted;
		Put put = new Put(Bytes.toBytes(row));
		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qua), bytes);
		try {
			table.put(put);
			table.close();
			isPutted = true;
		} catch (IOException e) {
			e.printStackTrace();
			isPutted = false;
		}
		return isPutted;
	}

	/**
	 * Put data in bulk to a specific table.
	 * 
	 * @param tbName: table name.
	 * @param row: several row key
	 * @param cf: column family
	 * @param qua: qualifier
	 * @param bytes:several bytes value
	 * @return boolean
	 */
	public static boolean batchInsertRow(String tbName, String[] row, String cf, String qua, byte[][] bytes) {
		boolean isPutted;
		Table table;
		List<Put> puts = new ArrayList<Put>();
		// if cannot connect to table, return false.
		try {
			table = connection.getTable(TableName.valueOf(tbName));
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		for (int i = 0; i < row.length; i++) {
			Put put = new Put(Bytes.toBytes(row[i]));
			put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qua), bytes[i]);
			puts.add(put);
		}
		try {
			table.put(puts);
			table.close();
			isPutted = true;
		} catch (IOException e) {
			e.printStackTrace();
			isPutted = false;
		}
		return isPutted;
	}

	/**
	 * Put data in bulk to a specific table.
	 *
	 * @param table: table object.
	 * @param row: several row key
	 * @param cf: column family
	 * @param qua: qualifier
	 * @param bytes:several bytes value
	 * @return boolean
	 */
	public static boolean batchInsertRow(Table table, String[] row, String cf, String qua, List<byte[]> bytes) {
		boolean isPutted;
		// System.out.println(bytes.size());
		// System.out.println(row.length);
		List<Put> puts = new ArrayList<Put>();
		for (int i = 0; i < row.length; i++) {
			Put put = new Put(Bytes.toBytes(row[i]));
			put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qua), bytes.get(i));
			puts.add(put);
		}
		try {
			table.put(puts);
			table.close();
			isPutted = true;
		} catch (IOException e) {
			e.printStackTrace();
			isPutted = false;
		}
		return isPutted;
	}

	/**
	 * Delete the whole row in table.
	 * 
	 * @param tbName: table name
	 * @param row: rew key
	 * @return boolean
	 */
	public static boolean deleteRow(String tbName, String row) {
		boolean isDeleted;
		Table table;
		// if cannot connect to table, return false.
		try {
			table = connection.getTable(TableName.valueOf(tbName));
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		Delete delete = new Delete(Bytes.toBytes(row));
		try {
			table.delete(delete);
			table.close();
			isDeleted = true;
		} catch (IOException e) {
			e.printStackTrace();
			isDeleted = false;
		}
		return isDeleted;
	}

	/**
	 * Delete the whole row in table.
	 *
	 * @param table object: table object
	 * @param       row: rew key
	 * @return boolean
	 */
	public static boolean deleteRow(Table table, String row) {
		boolean isDeleted;
		Delete delete = new Delete(Bytes.toBytes(row));
		try {
			table.delete(delete);
			table.close();
			isDeleted = true;
		} catch (IOException e) {
			e.printStackTrace();
			isDeleted = false;
		}
		return isDeleted;
	}

	/**
	 * Get data by row key.
	 * 
	 * @param tbName: table name
	 * @param row: row key
	 * @return org.apache.hadoop.hbase.client.Result
	 */
	public static Result getData(String tbName, String row) {
		Table table;
		// if cannot connect to table, return false.
		try {
			table = connection.getTable(TableName.valueOf(tbName));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		Get get = new Get(Bytes.toBytes(row));
		Result result = null;
		try {
			result = table.get(get);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * Get data by row key.
	 *
	 * @param table: tableobject
	 * @param row: row key
	 * @return org.apache.hadoop.hbase.client.Result
	 */
	public static Result getData(Table table, String row) {
		Get get = new Get(Bytes.toBytes(row));
		Result result = null;
		try {
			result = table.get(get);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * get data of cell
	 *
	 * @param     tbName; table name
	 * @param row ; row key
	 * @param cf  ; column family
	 * @param     qua; qualifier
	 * @return org.apache.hadoop.hbase.client.Result
	 */
	public static Result getData(String tbName, String row, String cf, String qua) {
		Table table;
		// if cannot connect to table, return false.
		try {
			table = connection.getTable(TableName.valueOf(tbName));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		Get get = new Get(Bytes.toBytes(row));
		get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qua));
		Result result = null;
		try {
			result = table.get(get);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * get data of cell
	 *
	 * @param     table; tableobject
	 * @param row ; row key
	 * @param cf  ; column family
	 * @param     qua; qualifier
	 * @return org.apache.hadoop.hbase.client.Result
	 */
	public static Result getData(Table table, String row, String cf, String qua) {
		Get get = new Get(Bytes.toBytes(row));
		get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qua));
		Result result = null;
		try {
			result = table.get(get);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * Get data of a cell in bulk.
	 *
	 * @param tbName: table name
	 * @param row: several row key
	 * @param cf: column family
	 * @param qua: qualifier
	 * @return Result[]
	 */
	public static Result[] batchGetData(String tbName, String[] row, String cf, String qua) {
		Table table;
		// if cannot connect to table, return false.
		List<Get> gets = new ArrayList<Get>();
		try {
			table = connection.getTable(TableName.valueOf(tbName));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		for (int i = 0; i < row.length; i++) {
			Get get = new Get(Bytes.toBytes(row[i]));
			get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qua));
			gets.add(get);
		}
		Result[] result = null;
		try {
			result = table.get(gets);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * Get data of a cell in bulk.
	 *
	 * @param row: several row key
	 * @param cf: column family
	 * @param qua: qualifier
	 * @return Result[]
	 */
	public static Result[] batchGetData(Table table, String[] row, String cf, String qua) {
		List<Get> gets = new ArrayList<Get>();
		for (int i = 0; i < row.length; i++) {
			Get get = new Get(Bytes.toBytes(row[i]));
			get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qua));
			gets.add(get);
		}
		Result[] result = null;
		try {
			result = table.get(gets);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * Get data of a cell in bulk.
	 *
	 * @param row: several row key
	 * @return Result[]
	 */
	public static Result[] batchGetData(Table table, String[] row) {
		List<Get> gets = new ArrayList<Get>();
		for (int i = 0; i < row.length; i++) {
			Get get = new Get(Bytes.toBytes(row[i]));
			gets.add(get);
		}
		Result[] result = null;
		try {
			result = table.get(gets);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * Get data of a column family.
	 * 
	 * @param tbName: table name
	 * @param row: row key
	 * @param cf: column family
	 * @return org.apache.hadoop.hbase.client.Result
	 */
	public static Result getData(String tbName, String row, String cf) {
		Table table;
		// if cannot connect to table, return false.
		try {
			table = connection.getTable(TableName.valueOf(tbName));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		Get get = new Get(Bytes.toBytes(row));
		get.addFamily(Bytes.toBytes(cf));
		Result result = null;
		try {
			result = table.get(get);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * Get data of a column family.
	 *
	 * @param table: tableobject
	 * @param row: row key
	 * @param cf: column family
	 * @return org.apache.hadoop.hbase.client.Result
	 */
	public static Result getData(Table table, String row, String cf) {
		Get get = new Get(Bytes.toBytes(row));
		get.addFamily(Bytes.toBytes(cf));
		Result result = null;
		try {
			result = table.get(get);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * Query all in the table.
	 * 
	 * @param tbName: table name
	 * @return org.apache.hadoop.hbase.client.ResultScanner
	 */
	public static ResultScanner queryAll(String tbName) {
		Scan scan = new Scan();
		ResultScanner results = null;
		try {
			Table table = connection.getTable(TableName.valueOf(tbName));
			results = table.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return results;
	}

	/**
	 * get tableobject
	 * 
	 * @param tbName: table anme
	 */
	public static Table getTableObject(String tbName) {
		Table table;
		// if cannot connect to table, return false.
		try {
			table = connection.getTable(TableName.valueOf(tbName));
			return table;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * get tableobject
	 * 
	 * @param tbName: table anme
	 */
	public static Table[] getTableObject(String[] tbName) {
		List<Table> tables = new ArrayList<Table>();
		for (int i = 0; i < tbName.length; i++) {
			// if cannot connect to table, return false.
			try {
				tables.add(connection.getTable(TableName.valueOf(tbName[i])));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return tables.toArray(new Table[tables.size()]);
	}

	public static void main(String[] args) throws IOException {
		boolean isConnected = HBaseHelper.connect();
//		if (isConnected) {
//            boolean isCreated = HBaseHelper.createTable("MapTileTest", new String[]{"cof1", "colf2"});
//            System.out.println(isCreated);
//            boolean isDeleted = HBaseHelper.deleteTable("MapTileTest");
//            System.out.println(isDeleted);
//            boolean isPutted = HBaseHelper.insertRow("MapTileTest", "irow", "cof1", "q3", Bytes.toBytes("abcd"));
//            System.out.println(isPutted);
//            boolean isDeleted = HBaseHelper.deleteRow("MapTileTest", "row5");
//            System.out.println(isDeleted);
//            Result result = getData("MapTileTest", "irow");
//            System.out.println(result.value());
		ResultScanner results = HBaseHelper.queryAll("TileTestL");
		for (Result r : results) {
			System.out.println(r);
		}
//		}

		/*
		 * for (int level = 0; level < 2; level++) { for (int row = 0; row < (int)
		 * Math.pow(2, level); row++) { for (int col = 0; col < (int) Math.pow(2,
		 * level); col++) { String rowkey = Rowkey.getRowkey(level, row, col); Result
		 * result = getData("MapTileTest1", rowkey); byte[] bs = result.value();
		 * TileReadWrite.write("D:\\" + rowkey + ".png", bs); } } }
		 */
		HBaseHelper.close();
	}
}
