package com.bigtable;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import java.io.IOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class BigtableController {
  String projectId, instanceId;
  private static final byte[] TABLE_NAME = Bytes.toBytes("User-Preference");

  private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("Items");

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
//        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

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
      System.exit(1);
    }

    System.exit(0);
  }
}
