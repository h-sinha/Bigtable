package com.bigtable;

/**
 * Record Class
 *
 */

public class Record {

	private int userID;
    private int itemID;
    private int viewCount;

	public int getUserID() {
		return userID;
	}
	public void setUserID(int id) {
		this.userID = id;
	}
    public int getItemID() {
		return ItemID;
	}
	public void setItemID(int id) {
		this.itemID = id;
	}
    public int getViewCount() {
		return viewCount;
	}
	public void setViewCount(int cnt) {
		this.viewCount = cnt;
	}
	@Override
	public String toString(){
		return "\nUserID="+getUserID()+"::ItemID="+getItemID()+"::ViewCount="+getViewCount();
	}
}
