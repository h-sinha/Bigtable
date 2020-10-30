package com.bigtable;

/**
 * Record Class
 *
 */

public class Employee {

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
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getRole() {
		return role;
	}
	public void setRole(String role) {
		this.role = role;
	}
	public String getSalary() {
		return salary;
	}
	public void setSalary(String salary) {
		this.salary = salary;
	}
	
	@Override
	public String toString(){
		return "\nID="+getId()+"::Name"+getName()+"::Role="+getRole()+"::Salary="+getSalary();
	}
}
