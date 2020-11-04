package com.bigtable;

import java.io.IOException;
import java.util.Scanner;

/** Hello world! */
public class App {
  public static void main(String[] args) throws IOException {
    Scanner sc = new Scanner(System.in); // Create a Scanner object
    System.out.println("Enter project ID for bigtable");
    String pid = sc.nextLine();
    System.out.println("Enter instance ID for bigtable");
    String iid = sc.nextLine();
    var inst = new BigtableController(pid, iid);
    int cmd, userId, itemId, K;
    String path;
    while (1) {
      System.out.print(
          "1.readCSV\n2.top\n3.interested\n4.top_interested\n5.view_count\n6.popular\n7.Quit\nEnter command number = ");
      cmd = sc.nextInt();
      switch (cmd) {
        case 1:
          System.out.print("Enter path to csv file = ");
          path = sc.nextLine();
          inst.readCSV(path);
          break;
        case 2:
          System.out.print("Enter user ID = ");
          userId = sc.nextInt();
          System.out.print("Enter K = ");
          K = sc.nextInt();
          var res = inst.top(userId, K);
          System.out.format("Top K items for userId:%d - \n", userId);
          for (var x : res) System.out.print(x + " ");
          System.out.println("");
          break;
        case 3:
          System.out.print("Enter item ID = ");
          itemId = sc.nextInt();
          System.out.format(
              "Number of users interested in item %d = %d\n", itemId, inst.interested(1));
          break;
        case 4:
          System.out.print("Enter item ID = ");
          itemId = sc.nextInt();
          System.out.print("Enter K = ");
          K = sc.nextInt();
          System.out.format("Top interested items for itemId:%d - \n", itemId);
          var res2 = inst.top_interested(itemId, K);
          for (var x : res2) System.out.print(x + " ");
          System.out.println("");
          break;
        case 5:
          System.out.print("Enter item ID = ");
          itemId = sc.nextInt();
          System.out.format("Viewcount of item %d = %d\n", itemId, inst.view_count(1));
          break;
        case 6:
          System.out.println("Most popular item in DB = " + inst.popular());
          break;
        case 7:
          break;
        default:
          System.out.println("Enter correct command number.");
      }
    }
    inst.readCSV("data.csv");
    System.out.println("Viewcount of item 1 = " + inst.view_count(1));
    System.out.println("Number of users interested in item 1 = " + inst.interested(1));
    System.out.println("Most popular item in DB = " + inst.popular());
    System.out.print("Top K iterms for userId:1 - ");
    var res = inst.top(1, 2);
    for (var x : res) System.out.print(x + " ");
    System.out.println("");
    System.out.print("Top K interested items for itemId:2 - ");
    var res2 = inst.top_interested(3, 2);
    for (var x : res2) System.out.print(x + " ");
    System.out.println("");
    System.exit(0);
  }
}
