package com.bigtable;

import java.io.IOException;
import java.util.Scanner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/** Hello world! */
public class App {
  public static void main(String[] args) throws IOException {
    Logger.getRootLogger().setLevel(Level.OFF);
    Scanner sc = new Scanner(System.in);
    System.out.print("Enter project ID for bigtable = ");
    String pid = sc.nextLine();
    System.out.print("Enter instance ID for bigtable = ");
    String iid = sc.nextLine();
    var inst = new BigtableController(pid, iid);
    int cmd, userId, itemId, K;
    String path;
    while (true) {
      System.out.print(
          "1.readCSV\n2.top\n3.interested\n4.top_interested\n5.view_count\n6.popular\n7.Quit\nEnter command number = ");
      Scanner cmds = new Scanner(System.in);
      cmd = cmds.nextInt();
      switch (cmd) {
        case 1:
          System.out.print("Enter path to csv file = ");
          Scanner sc1 = new Scanner(System.in);
          path = sc1.nextLine();
          inst.readCSV(path);
          break;
        case 2:
          System.out.print("Enter user ID = ");
          Scanner sc2 = new Scanner(System.in);
          userId = sc2.nextInt();
          System.out.print("Enter K = ");
          Scanner sc3 = new Scanner(System.in);
          K = sc3.nextInt();
          var res = inst.top(userId, K);
          System.out.format("Top K items for userId:%d - ", userId);
          for (var x : res) System.out.print(x + " ");
          System.out.println("");
          break;
        case 3:
          System.out.print("Enter item ID = ");
          Scanner sc4 = new Scanner(System.in);
          itemId = sc4.nextInt();
          System.out.format(
              "Number of users interested in item %d = %d\n", itemId, inst.interested(1));
          break;
        case 4:
          System.out.print("Enter item ID = ");
          Scanner sc5 = new Scanner(System.in);
          itemId = sc5.nextInt();
          System.out.print("Enter K = ");
          Scanner sc6 = new Scanner(System.in);
          K = sc6.nextInt();
          System.out.format("Top interested items for itemId:%d - ", itemId);
          var res2 = inst.top_interested(itemId, K);
          for (var x : res2) System.out.print(x + " ");
          System.out.println("");
          break;
        case 5:
          System.out.print("Enter item ID = ");
          Scanner sc7 = new Scanner(System.in);
          itemId = sc7.nextInt();
          System.out.format("Viewcount of item %d = %d\n", itemId, inst.view_count(1));
          break;
        case 6:
          System.out.println("Most popular item in DB = " + inst.popular());
          break;
        case 7:
          System.exit(0);
        default:
          System.out.println("Enter correct command number.");
          System.out.println(
              "-----------------------------------------------------------------------------");
      }
    }
  }
}
