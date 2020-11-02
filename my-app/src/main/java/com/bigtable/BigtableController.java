package com.bigtable;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class BigtableController {
  String projectId, instanceId;
  Set<Integer> columnId;
  private static final byte[] TABLE_NAME = Bytes.toBytes("User-Preference");

  private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("Items");

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
      Configuration config = HBaseConfiguration.create();
      AggregationClient aggregationClient = new AggregationClient(config);
      Scan scan = new Scan();
      scan.addColumn(COLUMN_FAMILY_NAME, Bytes.toBytes(itemId));
//      Scan scan = new Scan();
//      scan.addColumn(Bytes.toBytes("drs"), Bytes.toBytes("count"));

//      ColumnInterpreter<Long, Long> ci = new LongColumnInterpreter();

      Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
      Long sum = aggregationClient.sum(table, new LongColumnInterpreter() , scan);
      System.out.println(sum);
        ans = sum.intValue();
//      Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
//      Scan scan = new Scan();
//      scan.addColumn(COLUMN_FAMILY_NAME, Bytes.toBytes(itemId));
//      ResultScanner scanner = table.getScanner(scan);
//      for (Result result = scanner.next(); result != null; result = scanner.next()) {
//        ans += Bytes.toInt(result.getValue(COLUMN_FAMILY_NAME, Bytes.toBytes(itemId)));
//      }
    } catch (IOException e) {
      System.err.println("Exception while running program: " + e.getMessage());
      e.printStackTrace();
    } catch (Throwable throwable) {
      throwable.printStackTrace();
    }
    return ans;
  }
  public int popular(){
    int ans = 0, maxView = -1;
      for(var i :this.columnId){
        int curView = view_count(i);
        if(curView>maxView){
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


    // close reader
  }
}
