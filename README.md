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
