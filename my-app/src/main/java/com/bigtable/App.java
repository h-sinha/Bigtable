package com.bigtable;

import java.io.IOException;

/** Hello world! */
public class App {
  public static void main(String[] args) throws IOException {
    var inst = new BigtableController("data.csv");
    System.out.println("Number of people interested in item 1 = " + inst.interested(1));
    return;
  }
}
