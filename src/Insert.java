import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Insert implements Runnable {

	private Table table;
	private List<Object> values;
	private Long caller;

	public Insert(Table t, List<Object> values, Long caller) {
		this.table = t;
		this.values = values;
		this.caller = caller;
	}

	@Override
	public void run() {
		if (((ReentrantReadWriteLock) table.getTransLock()).isWriteLocked()) {
			if (Database.sema.get(table.getName()) != this.caller) {
				table.getTransLock().readLock().lock();
				table.getLock().writeLock().lock();
				
				try {
					table.insert(values);
				} finally {
					table.getLock().writeLock().unlock();
					table.getTransLock().readLock().unlock();
				}
			} else {
				table.getLock().writeLock().lock();
				
				try {
					table.insert(values);
				} finally {
					table.getLock().writeLock().unlock();
				}
			}
		} else {
			table.getLock().writeLock().lock();
			
			try {
				table.insert(values);
			} finally {
				table.getLock().writeLock().unlock();
			}
		}
	}

}
