/**
 * A simple HBase demo.
 */
package com.whu.demo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import java.io.IOException;

/**
 * @author lntussy
 * September 17, 2018
 */
public class HBaseDemo1 {
	public static Configuration configuration;
	public static Connection connection;
	public static Admin admin;
	
	public static void main(String[] args) throws IOException {
		// (1)创建一个表，表名为Score,列族为sname, course
		createTable("Score", new String[] {"sname", "course"});
		
		// (2)在Score表中插入一条数据，其行键为95001，sname为LiMing（因为sname列族下没有子列所以第4个参数为空）
		// 等价为：put 'Score', '95001', 'sname', 'LiMing'
		//insertRow("Score", "95001", "sname", "", "LiMing");
		// (3)在Score表中插入一条数据，其行键为95001，course:Math为88（course为列族，Math为course下的子列）
		// 等价为：put 'Score', '95001', 'course:Math', '88'
		//insertRow("Score", "95001", "course", "Math","88");
		// (4)在Score表中插入一条数据，其行键为95001，course:English为85（course为列族，Math为course下的子列）
		// 等价为：put 'Score', '95001', 'course:English', '88'
		//insertRow("Score", "95001", "course", "English", "85");
		
		// (5)删除Score表中指定列的数据,其行键为95001，列族为course，列为Math
		// 等价为：delete 'Score', '95001', 'course:Math'
		// 说明：执行该语句前需要将deleteRow方法中删除列的代码反注释，将删除列族的代码注释
		//deleteRow("Score", "95001", "course", "Math");
		// (6)删除Score表中指定列族的数据，其行键为95001，列族为course
		// 等价为：delete 'Score', '95001', 'score'
		// 说明：执行该语句前需要将deleteRow方法中删除列的代码注释，将删除列族的代码反注释
		//deleteRow("Score", "95001", "course", "");
		// (7)删除Score表中指定行的数据，其行键为95001
		// 等价位：deleteall 'Score', '95001'
		// 说明：执行该语句前需要将deleteRow方法中删除列和列族的代码注释
		//deleteRow("Score", "95001", "", "");
		
		// (8)查询Score表中，行键为95001，列族为course，列为Math的值
		// 等价为：get 'Score', '95001', 'course:Math'
		//getData("Score", "95001", "course", "Math");
		// (9)查询Score表中，行键为95001， 列族为sname的值
		// 等价为：get 'Score', '95001', 'sname'
		//getData("Score", "95001", "sname", "");
		
		// (10)删除Score表
		// 等价为：disable 'Score' drop 'Score'
		//deleteTable("Score");
	}
	
	//建立连接
	public static void init() {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.rootdir", "hdfs://master:9000/hbase");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");
		
		try {
			connection = ConnectionFactory.createConnection(configuration);
			admin = connection.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//关闭连接
	public static void close() {
		try {
			if(admin != null) {
				admin.close();
			}
			
			if(connection != null) {
				connection.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 创建表
	 * @param myTableName 表名
	 * @param colFamily 列族
	 * @throws IOException
	 */
	public static void createTable(String myTableName, String[] colFamily) throws IOException {
		init();
		TableName tableName = TableName.valueOf(myTableName);
		
		if(admin.tableExists(tableName)) {
			System.out.println("table is exists!");
		} else {
			HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
			
			for(String str: colFamily) {
				HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
				hTableDescriptor.addFamily(hColumnDescriptor);
			}
			admin.createTable(hTableDescriptor);
			System.out.println("create table sucessfully");
		}
	}
	
	/**
	 * 删除表
	 * @param tableName 表名
	 * @throws IOException
	 */
	public static void deleteTable(String tableName) throws IOException {
		init();
		TableName tn = TableName.valueOf(tableName);
		
		if (admin.tableExists(tn)) {
			admin.disableTable(tn);
			admin.deleteTable(tn);
		}
		
		close();
	}
	
	/**
	 * 查看已有HBase表
	 * @throws IOException
	 */
	public static void listTables() throws IOException {
		init();
		HTableDescriptor hTableDescriptors[] = admin.listTables();
		
		for (HTableDescriptor hTableDescriptor: hTableDescriptors) {
			System.out.println(hTableDescriptor.getNameAsString());
		}
	}
	
	/**
	 * 向某一行的某一列插入数据
	 * @param tableName 表名
	 * @param rowKey 行健
	 * @param colFamily 列族
	 * @param col 列名（如果列族下没有子列，可为空）
	 * @param val 值
	 * @throws IOException
	 */
	public static void insertRow (String tableName, String rowKey, String colFamily, String col, String val) throws IOException {
		init();
		Table table = connection.getTable(TableName.valueOf(tableName));
		Put put = new Put(rowKey.getBytes());
		put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
		table.put(put);
		table.close();
		close();
	}
	
	/**
	 * 删除数据
	 * @param tableName 表名
	 * @param rowKey 行健
	 * @param colFamily 列族
	 * @param col 列
	 * @throws IOException
	 */
	public static void deleteRow(String tableName, String rowKey, String colFamily, String col) throws IOException {
		init();
		Table table = connection.getTable(TableName.valueOf(tableName));
		Delete delete = new Delete(rowKey.getBytes());
		// 删除指定列族的所有数据
		//delete.addFamily(colFamily.getBytes());
		// 删除指定列的数据
		delete.addColumn(colFamily.getBytes(), col.getBytes());
		table.delete(delete);
		table.close();
		close();
	}
	
	/**
	 * 根据行健查找数据
	 * @param tableName 表名
	 * @param rowKey 行健
	 * @param colFamily 列族
	 * @param col 列
	 */
	public static void getData(String tableName, String rowKey, String colFamily, String col) throws IOException {
		init();
		Table table = connection.getTable(TableName.valueOf(tableName));
		Get get = new Get(rowKey.getBytes());
		get.addColumn(colFamily.getBytes(), col.getBytes());
		Result result = table.get(get);
		showCell(result);
		table.close();
		close();
	}

	/**
	 * 格式化输出
	 * @param result
	 */
	private static void showCell(Result result) {
		Cell[] cells = result.rawCells();
		for (Cell cell: cells) {
			System.out.println("Row Name: " + new String(CellUtil.cloneRow(cell)) + " ");
			System.out.println("Timestamp: " + cell.getTimestamp() + " ");
			System.out.println("Column Family: " + new String(CellUtil.cloneFamily(cell)) + " ");
			System.out.println("Column Name: " + new String(CellUtil.cloneQualifier(cell)) + " ");
			System.out.println("value: " + new String(CellUtil.cloneValue(cell)) + " ");
		}
		
	}
}
