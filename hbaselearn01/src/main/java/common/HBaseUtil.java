package common;

import java.io.IOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase连接类 参考：http://blog.csdn.net/bobshute/article/details/54141716
 * 
 * @author kwz
 *
 */
public class HBaseUtil {

	private static Connection conn;
	private static Admin admin;

	static {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "127.0.0.1");
		try {
			conn = ConnectionFactory.createConnection(config);
			admin = conn.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 关闭
	 */
	public static void closeConnection() {
		try {
			admin.close();
			conn.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * http://blog.csdn.net/qq_31570685/article/details/51757604
	 * 
	 * @param tableName
	 * @param family
	 */
	public static void createTable(String tableName, String family) {
		TableName tName = TableName.valueOf(tableName);
		try {
			if (admin.tableExists(tName)) {
				System.out.println("表已存在：" + tableName);
			} else {
				// HTableDescriptor desc = new HTableDescriptor();
				TableDescriptorBuilder build = TableDescriptorBuilder.newBuilder(tName);
				build.addFamily(new HColumnDescriptor(family));
				admin.createTable(build.build());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * http://blog.csdn.net/qq_31570685/article/details/51757604
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param column
	 * @param value
	 */
	public static void putData(String tableName, String rowKey, String family, String column, String value) {
		TableName tName = TableName.valueOf(tableName);
		Table table = null;
		try {
			table = conn.getTable(tName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
			table.put(put);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * http://blog.csdn.net/qq_31570685/article/details/51757604
	 * 
	 * @param tableName
	 */
	public static void getValueByTable(String tableName) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			ResultScanner rs = table.getScanner(new Scan());
			for (Result r : rs) {
				showResult(r);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * http://blog.csdn.net/qq_31570685/article/details/51757604
	 * 
	 * @param tableName
	 * @param rowKey
	 */
	public static void queryByRowKey(String tableName, String rowKey) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowKey));
			Result result = table.get(get);
			showResult(result);
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 展示一个result
	 * 
	 * @param result
	 */
	private static void showResult(Result result) {
		if (result != null) {
			System.out.println("获得到rowkey:" + new String(result.getRow()));
			for (Cell cell : result.listCells()) {
				System.out.print("row:\t");
				long timestamp = cell.getTimestamp();
				System.out.print("timestamp:" + timestamp + "\t");
				String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
				// family = Bytes.toString(cell.getFamilyArray());
				// family = new String(cell.getFamilyArray(), "ISO-8859-1");
				System.out.print("family:" + family + "\t");
				String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
						cell.getQualifierLength());
				System.out.print("qualifier:" + qualifier + "\t");
				String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
				System.out.println("value:" + value);
			}
		}
	}

	/**
	 * http://blog.csdn.net/qq_31570685/article/details/51757604
	 * 
	 * @param tableName
	 * @param family
	 * @param column
	 * @param value
	 */
	public static void query(String tableName, String family, String column, String value) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			Filter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(column), CompareOp.EQUAL,
					Bytes.toBytes(value));
			Scan scan = new Scan();
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				showResult(r);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss SSSSSS");
		System.out.println("begin:" + formatter.format(LocalTime.now()));
		final String tableName = "t_lesson1";
		final String family = "fm";
		final String rowKey = "rowkey1";
		final String column = "column1";
		HBaseUtil.createTable(tableName, family);
		HBaseUtil.putData(tableName, rowKey, family, column, "value1");
		System.out.println("全表：");
		HBaseUtil.getValueByTable(tableName);
		System.out.println("查询rowkey：" + rowKey);
		HBaseUtil.queryByRowKey(tableName, rowKey);
		System.out.println("条件查询：");
		HBaseUtil.query(tableName, family, column, "value1");
		System.out.println("end:" + formatter.format(LocalTime.now()));
		HBaseUtil.closeConnection();
	}

}
