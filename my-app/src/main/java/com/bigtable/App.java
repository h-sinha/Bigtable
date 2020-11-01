package com.bigtable;

import java.io.IOException;

/** Hello world! */
public class App {
  public static void main(String[] args) throws IOException {
    var inst = new BigtableController("data.csv");
    System.out.println("Viewcount of item 1 = " + inst.view_count(1));
    System.out.println("Number of users interested in item 1 = " + inst.interested(1));
    System.exit(0);
  }
}
