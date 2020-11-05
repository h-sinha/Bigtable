# Bigtable
This project communicates with Cloud Bigtable through the Cloud Bigtable HBase client for Java, which is a customized version of the Apache HBase client. The Bigtable contains following data - 
* **userID** -> Unique ID for user
* **itemID** -> Unique ID for item 
* **viewCount** -> The total times a given itemID has been viewed by a given userID. 
## Methods
<ol>
   <li>
    <b>readCSV</b>
    <ul> 
      <li><b> Description </b> - Reads CSV file and stores data in Bigtable.</li>
      <li><b> Signature </b> - void readCSV(filepath) </li>
      <li><b> Parameters </b> - filepath:string</li>
    </ul>
  </li>
  <li>
    <b>top</b>
    <ul> 
      <li><b> Description </b> - Get the top K items for a given userID with the highest view count.</li>
      <li><b> Signature </b> - int[] top(userID, K) </li>
      <li><b> Parameters </b> - userID:int and K:int</li>
      <li><b> Return Type </b> - Integer array of size K containing top K items. </li>
    </ul>
  </li>
  <li>
    <b>interested</b>
    <ul> 
      <li><b> Description </b> - Get the number of users interested in a given itemID.  </li>
      <li><b> Signature </b> - int interested(itemID) </li>
      <li><b> Parameters </b> - itemID:int</li>
      <li><b> Return Type </b> - integer denoting the number of users interested in itemID.</li>
    </ul>
  </li>
  <li>
    <b>top_interested</b>
    <ul> 
      <li><b> Description </b> - For a given itemID, find the top K items that are of interest to those who viewed this item. </li>
      <li><b> Signature </b> - int[] top_interested(itemID,K) </li>
      <li><b> Parameters </b> - userID:int and K:int</li>
      <li><b> Return Type </b> - Integer array of size K containing top K items.</li>
    </ul>
  </li>
  <li>
    <b>view_count</b>
    <ul> 
      <li><b> Description </b> - Get the total view count for a given itemID.  </li>
      <li><b> Signature </b> - int view_count(itemID) </li>
      <li><b> Parameters </b> - itemID:int</li>
      <li><b> Return Type </b> - Integer denoting total view count for given itemID.</li>
    </ul>
  </li>
   <li>
    <b>popular</b>
    <ul> 
      <li><b> Description </b> - Get the itemID of the most popular item in the database.  </li>
      <li><b> Signature </b> - int popular() </li>
      <li><b> Return Type </b> - Integer denoting the itemID of the most popular item.</li>
    </ul>
  </li>
</ol>

## How to use?
* Clone/Download this repository
* Run following commands
``` 
cd Bigtable
mvn package
mvn exec:java
```
* Interaction -

   * You'll see this after running the commands mentioned above.
   ```
   Enter project ID for bigtable = ****
   Enter instance ID for bigtable = ****
   ```
   Enter project ID and instance ID corresponding to the instance where bigtable should be created.
   * After this the command prompt would display the list of functions supported
   ```
   1.readCSV
   2.top
   3.interested
   4.top_interested
   5.view_count
   6.popular
   7.Quit
   Enter command number = 
   ```
   Example - For readCSV enter 1. The program would then ask for the path to csv file(full path or path relative to Bigtable folder).
   ```
   Enter command number = 1
   Enter path to csv file = data.csv
   ```
   Similarly this can be used for all other functions.
