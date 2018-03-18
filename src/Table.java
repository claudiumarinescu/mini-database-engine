import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class Table {

	private String name;
	private ArrayList<String> columnNames;
	private ArrayList<String> columnTypes;
	private ArrayList<ArrayList<Object>> records;
	
	private ReadWriteLock lock = new ReentrantReadWriteLock();
	private ReadWriteLock transLock = new ReentrantReadWriteLock();
	
	
	public Table(String name, ArrayList<String> names, ArrayList<String> types) {
		this.setName(name);
		this.columnNames = names;
		this.columnTypes = types;

		this.records = new ArrayList<ArrayList<Object>>();
		for (int i = 0; i < names.size(); i++) {
			this.records.add(new ArrayList<Object>());
		}
	}
	
	public void insert(List<Object> values) {
		int idx = 0;
		for (Object o : values) {
			this.records.get(idx++).add(o);
		}
	}

	public void update(List<Object> values, String condition) {
		if (condition.length() == 0) {
			for (int i = 0; i < this.records.get(0).size(); i++) {
				for (int j = 0; j < columnNames.size(); j++) {
					this.records.get(j).set(i, values.get(j)); 
				}
			}
		} else {
			String[] cond = condition.split(" ");
			switch (cond[1]) {
			case "<":
				for (int i = 0; i < this.records.get(0).size(); i++) {
					if ((int) this.records.get(columnNames.indexOf(cond[0])).get(i) 
							< Integer.parseInt(cond[2])) {

						for (int j = 0; j < columnNames.size(); j++) {
							this.records.get(j).set(i, values.get(j)); 
						}
					}
				}
				break;

			case ">":
				for (int i = 0; i < this.records.get(0).size(); i++) {
					if ((int) this.records.get(columnNames.indexOf(cond[0])).get(i) 
							> Integer.parseInt(cond[2])) {

						for (int j = 0; j < columnNames.size(); j++) {
							this.records.get(j).set(i, values.get(j)); 
						}
					}
				}
				break;

			case "==":
				for (int i = 0; i < this.records.get(0).size(); i++) {
					switch (columnTypes.get(columnNames.indexOf(cond[0]))) {
					case "int":
						if (((Integer) this.records.get(columnNames.indexOf(cond[0])).get(i))
								.equals(Integer.parseInt(cond[2]))) {
							for (int j = 0; j < columnNames.size(); j++) {
								this.records.get(j).set(i, values.get(j)); 
							}
						}
						break;

					default:
						if (this.records.get(columnNames.indexOf(cond[0])).get(i).equals(cond[2])) {
							for (int j = 0; j < columnNames.size(); j++) {
								this.records.get(j).set(i, values.get(j)); 
							}
						}
						break;
					}
				}
				break;
			}
		}
	}

	public ArrayList<ArrayList<Object>> select(String[] operations, String condition) {
		ArrayList<ArrayList<Object>> satis = new ArrayList<ArrayList<Object>>();
		ArrayList<ArrayList<Object>> result = new ArrayList<ArrayList<Object>>();
		for (int i = 0; i < this.columnNames.size(); i++) {
			satis.add(new ArrayList<Object>());
		}
		for (int i = 0; i < operations.length; i++) {
			result.add(new ArrayList<Object>());
		}
		
		if (condition == "") {
			satis = records;
		} else {
			String[] cond = condition.split(" ");
			
			switch (cond[1]) {
			case "<":
				for (int i = 0; i < this.records.get(0).size(); i++) {
					if ((int) this.records.get(columnNames.indexOf(cond[0])).get(i) < Integer.parseInt(cond[2])) {
						for (int j = 0; j < columnNames.size(); j++) {
							satis.get(j).add(this.records.get(j).get(i)); 
						}
					}
				}
				break;
			case ">":
				for (int i = 0; i < this.records.get(0).size(); i++) {
					if ((int) this.records.get(columnNames.indexOf(cond[0])).get(i) > Integer.parseInt(cond[2])) {
						for (int j = 0; j < columnNames.size(); j++) {
							satis.get(j).add(this.records.get(j).get(i));
						}
					}
				}
				break;
			case "==":
				for (int i = 0; i < this.records.get(0).size(); i++) {
					switch (columnTypes.get(columnNames.indexOf(cond[0]))) {
					case "int":
						if (this.records.get(columnNames.indexOf(cond[0])).get(i).equals(Integer.parseInt(cond[2]))) {
							for (int j = 0; j < columnNames.size(); j++) {
								satis.get(j).add(this.records.get(j).get(i));
							}
						}
						break;
					default:
						if (this.records.get(columnNames.indexOf(cond[0])).get(i).equals(cond[2])) {
							for (int j = 0; j < columnNames.size(); j++) {
								satis.get(j).add(this.records.get(j).get(i));
							}
						}
						break;
					}
				}
				break;
			}
		}
		
		if (satis.get(0).isEmpty()) {
			return result;
		}
		
		int idx = 0;
		for (String s : operations) {
			if (s.startsWith("count")) {
				result.get(idx++).add(satis.get(0).size());
				continue;
			}
			
			if (s.startsWith("min")) {
				String col = s.substring(4, s.length() - 1);
				Integer min = Integer.MAX_VALUE;
				for (Object o : satis.get(columnNames.indexOf(col))) {
					if ((Integer) o < min) 
						min = (Integer) o;
				}
				result.get(idx++).add(min);
				continue;
			}
			
			if (s.startsWith("max")) {
				String col = s.substring(4, s.length() - 1);
				Integer max = Integer.MIN_VALUE;
				for (Object o : satis.get(columnNames.indexOf(col))) {
					if ((Integer) o > max) 
						max = (Integer) o;
				}
				result.get(idx++).add(max);
				continue;
			}
			
			if (s.startsWith("sum")) {
				String col = s.substring(4, s.length() - 1);
				Integer sum = 0;
				for (Object o : satis.get(columnNames.indexOf(col))) {
					sum += (Integer) o;
				}
				result.get(idx++).add(sum);
				continue;
			}
			
			if (s.startsWith("avg")) {
				String col = s.substring(4, s.length() - 1);
				Integer sum = 0;
				for (Object o : satis.get(columnNames.indexOf(col))) {
					sum += (Integer) o;
				}
				Integer avg = sum / satis.get(0).size();
				result.get(idx++).add(avg);
				continue;
			}
			
			for (Object o : satis.get(columnNames.indexOf(s))) {
				result.get(idx).add(o);
			}
			idx++;
		}
		
		return result;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public List<String> getColumnNames() {
		return columnNames;
	}

	public void setColumnNames(ArrayList<String> columnNames) {
		this.columnNames = columnNames;
	}

	public List<String> getColumnTypes() {
		return columnTypes;
	}

	public void setColumnTypes(ArrayList<String> columnTypes) {
		this.columnTypes = columnTypes;
	}
	
	public ArrayList<ArrayList<Object>> getRecords() {
		return this.records;
	}
	
	synchronized public void print() {
		for (String s : columnNames) {
			System.out.print(s + " | ");
		}
		System.out.println();
		
		
		for (int i = 0; i < records.get(0).size(); i++) {
			for (int j = 0; j < columnNames.size(); j++) {
				System.out.print(records.get(j).get(i) + " | ");
			}
			System.out.println();
		}
	}

	public ReadWriteLock getLock() {
		return lock;
	}

	public void setLock(ReadWriteLock lock) {
		this.lock = lock;
	}

	public ReadWriteLock getTransLock() {
		return transLock;
	}

	public void setTransLock(ReadWriteLock transLock) {
		this.transLock = transLock;
	}
	
}
