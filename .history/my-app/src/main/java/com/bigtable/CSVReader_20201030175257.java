package com.bigtable;

/**
 * CSV Reader
 *
 */
 public class ReadCSVWithScanner {

	public static void main(String[] args) throws IOException {
		// open file input stream
		BufferedReader reader = new BufferedReader(new FileReader(
				"records.csv"));

		// read file line by line
		String line = null;
		Scanner scanner = null;
		int index = 0;
		List<Record> recordList = new ArrayList<>();

		while ((line = reader.readLine()) != null) {
			Employee rec = new Employee();
			scanner = new Scanner(line);
			scanner.useDelimiter(",");
			while (scanner.hasNext()) {
				String data = scanner.next();
				if (index == 0)
					rec.setUserID(Integer.parseInt(data));
				else if (index == 1)
					rec.setItemID(Integer.parseInt(data));
				else if (index == 2)
					rec.setViewCount(Integer.parseInt(data));
				else
					System.out.println("invalid data::" + data);
				index++;
			}
			index = 0;
			recordList.add(rec);
		}
		
		//close reader
		reader.close();
		
		System.out.println(empList);
		
	}

}