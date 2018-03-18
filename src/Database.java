import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Database implements MyDatabase {
	
	ArrayList<Table> tables;
	ExecutorService executor;
	static ConcurrentHashMap<String, Long> sema;
	
	public Database() {
		tables = new ArrayList<>();
		sema = new ConcurrentHashMap<>();
	}

	@Override
	public void initDb(int numWorkerThreads) {
		executor = Executors.newFixedThreadPool(numWorkerThreads);
	}

	@Override
	public void stopDb() {
		executor.shutdown();
	}

	@Override
	public void createTable(String tableName, String[] columnNames, String[] columnTypes) {
		Table table = new Table(tableName, new ArrayList<String>(Arrays.asList(columnNames)), 
								new ArrayList<String>(Arrays.asList(columnTypes)));
		this.tables.add(table);
	}

	@Override
	public ArrayList<ArrayList<Object>> select(String tableName, String[] operations, String condition) {
		for (Table t : tables) {
			if (t.getName().equals(tableName)) {
				Select select = new Select(t, operations, condition, Thread.currentThread().getId());
				Future<ArrayList<ArrayList<Object>>> result = executor.submit(select);
				try {
					if (result == null) {
						System.out.println("Result is null");
						return new ArrayList<ArrayList<Object>>();
					} else {
						return result.get();
					}
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}
		}

		return null;
	}

	@Override
	public void update(String tableName, ArrayList<Object> values, String condition) {
		for (Table table : tables) {
			if (table.getName().equals(tableName)) {
				Update update = new Update(table, values, condition, Thread.currentThread().getId());
				executor.submit(update);
				break;
			}
		}
	}

	@Override
	public void insert(String tableName, ArrayList<Object> values) {
		for (Table table : tables) {
			if (table.getName().equals(tableName)) {
				Insert insert = new Insert(table, values, Thread.currentThread().getId());
				executor.submit(insert);
				break;
			}
		}
	}

	@Override
	synchronized public void startTransaction(String tableName) {
		for (Table table : tables) {
			if (table.getName().equals(tableName)) {
				Long caller = Thread.currentThread().getId();
				table.getTransLock().writeLock().lock();
				Database.sema.put(tableName, caller);
			}
		}
	}

	@Override
	public void endTransaction(String tableName) {
		for (Table table : tables) {
			if (table.getName().equals(tableName)) {
				table.getTransLock().writeLock().unlock();
				Database.sema.remove(tableName);
			}
		}
	}
}
