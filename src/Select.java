import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Select implements Callable<ArrayList<ArrayList<Object>>> {

	private Table table;
	private String[] operations;
	private String condition;
	private Long caller;

	public Select(Table t, String[] operations, String condition, Long caller) {
		this.table = t;
		this.operations = operations;
		this.condition = condition;
		this.caller = caller;
	}

	@Override
	public ArrayList<ArrayList<Object>> call() throws Exception {
		ArrayList<ArrayList<Object>> result = new ArrayList<ArrayList<Object>>();
		
		if (((ReentrantReadWriteLock) table.getTransLock()).isWriteLocked()) {
			if (Database.sema.get(table.getName()) != this.caller) {
				table.getTransLock().readLock().lock();
				table.getLock().readLock().lock();
				
				try {
					result = table.select(operations, condition);
				} finally {
					table.getLock().readLock().unlock();
					table.getTransLock().readLock().unlock();
				}
			} else {
				table.getLock().readLock().lock();
				
				try {
					result = table.select(operations, condition);
				} finally {
					table.getLock().readLock().unlock();
				}
			}
		} else {
			table.getLock().readLock().lock();

			try {
				result = table.select(operations, condition);
			} finally {
				table.getLock().readLock().unlock();
			}
		}

		return result;
	}

}
