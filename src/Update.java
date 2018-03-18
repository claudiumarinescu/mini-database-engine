import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Update implements Runnable {

	private Table table;
	private List<Object> values;
	private String condition;
	private Long caller;
	
	public Update(Table t, List<Object> values, String condition, Long caller) {
		this.table = t;
		this.values = values;
		this.condition = condition;
		this.caller = caller;
	}

	@Override
	public void run() {
		if (((ReentrantReadWriteLock) table.getTransLock()).isWriteLocked()) {
			if (Database.sema.get(table.getName()) != this.caller) {
				table.getTransLock().readLock().lock();
				table.getLock().writeLock().lock();
				
				try {
					table.update(values, condition);
				} finally {
					table.getLock().writeLock().unlock();
					table.getTransLock().readLock().unlock();
				}
			} else {
				table.getLock().writeLock().lock();
				
				try {
					table.update(values, condition);
				} finally {
					table.getLock().writeLock().unlock();
				}
			}
		} else {
			table.getLock().writeLock().lock();

			try {
				table.update(values, condition);
			} finally {
				table.getLock().writeLock().unlock();
			}
		}
	}
}
