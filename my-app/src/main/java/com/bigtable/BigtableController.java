package com.bigtable;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class BigtableController {
  String projectId, instanceId;
  private static final byte[] TABLE_NAME = Bytes.toBytes("User-Preference");

  private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("Items");

  public int interested(int itemId) {
    int ans = 0;
    try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
      Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
      Scan scan = new Scan();
      scan.addColumn(COLUMN_FAMILY_NAME, Bytes.toBytes(itemId));
      SingleColumnValueFilter filter = new SingleColumnValueFilter(COLUMN_FAMILY_NAME, Bytes.toBytes(itemId),
          CompareOp.NOT_EQUAL, Bytes.toBytes(0));
      scan.setFilter(filter);
      ResultScanner scanner = table.getScanner(scan);
      for (Result result = scanner.next(); result != null; result = scanner.next()) {
        ans += Bytes.toInt(result.getValue(COLUMN_FAMILY_NAME, Bytes.toBytes(itemId)));
      }
    } catch (IOException e) {
      System.err.println("Exception while running program: " + e.getMessage());
      e.printStackTrace();
    }
    return ans;
  }

  public int popular() {
    int ans = 0, maxView = -1;
    for (var i : this.columnId) {
      int curView = view_count(i);
      if (curView > maxView) {
        maxView = curView;
        ans = i;
      }
    }
    return ans;
  }

  public void top(int userID, int K) {
    int rowKey = userID;
    Result getResult = table.get(new Get(Bytes.toBytes(rowKey)).setMaxVersions(Integer.MAX_VALUE)
        .addColumn(COLUMN_FAMILY_NAME, "ItemID").addColumn(COLUMN_FAMILY_NAME, "Count"));
    System.out.println(Result);
  }

  public int view_count(int itemId) {
    int ans = 0;
    try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
      Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
      Scan scan = new Scan();
      scan.addColumn(COLUMN_FAMILY_NAME, Bytes.toBytes(itemId));
      ResultScanner scanner = table.getScanner(scan);
      for (Result result = scanner.next(); result != null; result = scanner.next()) {
        ans += Bytes.toInt(result.getValue(COLUMN_FAMILY_NAME, Bytes.toBytes(itemId)));

      }
    } catch (IOException e) {
      System.err.println("Exception while running program: " + e.getMessage());
      e.printStackTrace();
    }
    return ans;
  }

  public BigtableController(String path) throws IOException {
    var csv = new CSVHandler();
    csv.readCSV(path);
    // change later based on submission format
    this.projectId = "ds-hw-5";
    this.instanceId = "in1234";
    try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
      Admin admin = connection.getAdmin();
      try {
        // delete table if it already exists
        if (admin.tableExists(TableName.valueOf(TABLE_NAME))) {
          admin.disableTable(TableName.valueOf(TABLE_NAME));
          admin.deleteTable(TableName.valueOf(TABLE_NAME));
        }
        // [START bigtable_hw_create_table]
        // Create a table with a single column family
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY_NAME));

        admin.createTable(descriptor);
        // [END bigtable_hw_create_table]
        // [START bigtable_hw_write_rows]
        // Retrieve the table we just created so we can do some reads and writes
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        List<Put> putList = new ArrayList<Put>();
        for (var row : csv.recordList) {
          Put put = new Put(Bytes.toBytes(row.getUserID()));
          put.addColumn(COLUMN_FAMILY_NAME, Bytes.toBytes(row.getItemID()), Bytes.toBytes(row.getViewCount()));
          putList.add(put);
          if (putList.size() >= 1e6) {
            table.put(putList);
            putList.clear();
          }
        }
        if (putList.size() > 0) {
          table.put(putList);
        }
      } catch (IOException e) {
        if (admin.tableExists(TableName.valueOf(TABLE_NAME))) {
          admin.disableTable(TableName.valueOf(TABLE_NAME));
          admin.deleteTable(TableName.valueOf(TABLE_NAME));
        }
        throw e;
      }
    } catch (IOException e) {
      System.err.println("Exception while running program: " + e.getMessage());
      e.printStackTrace();
    }
  }
}
