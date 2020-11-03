package com.bigtable;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.Set;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class BigtableController {
  String projectId, instanceId;
  Set<Integer> columnId;
  private static final byte[] TABLE_NAME = Bytes.toBytes("User-Preference");

  private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("Items");

  class Pair {
    int v1, v2;

    public Pair(int v1, int v2) {

      this.v1 = v1;
      this.v2 = v2;
    }
  }

  public int[] top(int userID, int K) {
    int[] ans = new int[K];
    try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
      Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
      Result getResult =
          table.get(new Get(Bytes.toBytes(userID)).setMaxVersions().addFamily(COLUMN_FAMILY_NAME));
      Cell[] raw = getResult.rawCells();
      if (raw == null) {
        System.out.println(
            "No data was returned. If you recently ran the import job, try again in a minute.");
        return new int[0];
      }
      PriorityQueue<Pair> maxHeap =
          new PriorityQueue<Pair>(
              K,
              new Comparator<Pair>() {
                public int compare(Pair n1, Pair n2) {
                  return n1.v1 - n2.v1;
                }
              });
      int itemId, viewCount;
      for (Cell cell : raw) {
        itemId = Bytes.toInt(cell.getQualifierArray());
        viewCount = Bytes.toInt(cell.getValueArray());
        maxHeap.add(new Pair(viewCount, itemId));
        if (maxHeap.size() > K) {
          maxHeap.poll();
        }
      }
      int idx = 0;
      Pair pair;
      while ((pair = maxHeap.poll()) != null) {
        ans[idx++] = pair.v2;
      }
    } catch (IOException e) {
      System.err.println("Exception while running program: " + e.getMessage());
      e.printStackTrace();
    }
    return ans;
  }

  public int interested(int itemId) {
    int ans = 0;
    try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
      Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
      Scan scan = new Scan();
      scan.addColumn(COLUMN_FAMILY_NAME, Bytes.toBytes(itemId));
      SingleColumnValueFilter filter =
          new SingleColumnValueFilter(
              COLUMN_FAMILY_NAME, Bytes.toBytes(itemId), CompareOp.NOT_EQUAL, Bytes.toBytes(0));
      scan.setFilter(filter);
      ResultScanner scanner = table.getScanner(scan);
      for (Result result = scanner.next(); result != null; result = scanner.next()) {
        ans++;
      }
    } catch (IOException e) {
      System.err.println("Exception while running program: " + e.getMessage());
      e.printStackTrace();
    }
    return ans;
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

  public BigtableController() {
    // change later based on submission format
    this.projectId = "ds-hw-5";
    this.instanceId = "in1234";
  }

  public void readCSV(String filepath) throws IOException {
    try (Connection connection = BigtableConfiguration.connect(this.projectId, this.instanceId)) {
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
        this.columnId = new HashSet<Integer>();

        // csv read
        BufferedReader reader = new BufferedReader(new FileReader("data.csv"));
        String line = null;
        Scanner scanner = null;
        int index = 0;
        // read headers
        line = reader.readLine();
        while ((line = reader.readLine()) != null) {
          Record rec = new Record();
          scanner = new Scanner(line);
          scanner.useDelimiter(",");
          while (scanner.hasNext()) {
            String data = scanner.next();
            if (index == 0) rec.setUserID(Integer.parseInt(data));
            else if (index == 1) rec.setItemID(Integer.parseInt(data));
            else if (index == 2) rec.setViewCount(Integer.parseInt(data));
            else System.out.println("invalid data::" + data);
            index++;
          }
          index = 0;
          Put put = new Put(Bytes.toBytes(rec.getUserID()));
          put.addColumn(
              COLUMN_FAMILY_NAME,
              Bytes.toBytes(rec.getItemID()),
              Bytes.toBytes(rec.getViewCount()));
          this.columnId.add(rec.getItemID());
          putList.add(put);
          if (putList.size() >= 1e6) {
            table.put(putList);
            putList.clear();
          }
        }
        if (putList.size() > 0) {
          table.put(putList);
        }
        reader.close();

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
