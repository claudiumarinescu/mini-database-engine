import java.util.ArrayList;

public interface MyDatabase {
	public void initDb(int numWorkerThreads);
	public void stopDb();
	public void createTable(String tableName, String[] columnNames, String[] columnTypes);
	public ArrayList<ArrayList<Object>> select(String tableName, String[] operations, String condition);
	public void update(String tableName, ArrayList<Object> values, String condition);
	public void insert(String tableName, ArrayList<Object> values);
	public void startTransaction(String tableName);
	public void endTransaction(String tableName);
}
