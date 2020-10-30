package com.bigtable;

import java.io.IOException;

/** Hello world! */
public class App {
  public static void main(String[] args) throws IOException {
    var csv = new CSVHandler();
    csv.readCSV("data.csv");
  }
}
