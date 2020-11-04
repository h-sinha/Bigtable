package com.bigtable;

import java.io.IOException;

/** Hello world! */
public class App {
  public static void main(String[] args) throws IOException {
    var inst = new BigtableController();
    inst.readCSV("data.csv");
    System.out.println("Viewcount of item 1 = " + inst.view_count(1));
    System.out.println("Number of users interested in item 1 = " + inst.interested(1));
    System.out.println("Most popular item in DB = " + inst.popular());
    System.out.print("Top K iterms for userId:1 - ");
    var res = inst.top(1, 2);
    for (var x : res)
      System.out.print(x + " ");
    System.out.println("");
    System.out.print("Top K interested items for itemId:2 - ");
    var res2 = inst.top_interested(3, 2);
    for (var x : res2)
      System.out.print(x + " ");
    System.out.println("");
    System.exit(0);
  }
}
