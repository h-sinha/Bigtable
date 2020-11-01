package com.bigtable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/** CSV Reader */
public class CSVHandler {
  List<Record> recordList = new ArrayList<>();

  public void readCSV(String filepath) throws IOException {
    // open file input stream
    BufferedReader reader = new BufferedReader(new FileReader("data.csv"));

    // read file line by line
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
      this.recordList.add(rec);
    }

    // close reader
    reader.close();
  }
}
