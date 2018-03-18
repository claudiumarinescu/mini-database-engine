import java.util.ArrayList;
import java.util.concurrent.CyclicBarrier;

public class Main {

	// - Test consistency 4 worker threads 4 client threads
	// -- multiple client threads insert in one table
	// --- select a sum make sure it's all right
	// -- multiple client threads update the same table
	// --- select a sum make sure it's all right
	// -- check transactions
	
	// - Test scalability 1,2,4 worker threads and 1,2,4 client threads
	// -- multiple clients insert in multiple tables
	// -- one client update in one table
	// -- one client thread selects one table
	
	
	public static void main(String[] args) throws Exception {
		// sanityCheck();
		// testConsistency();
		testScalability();
	}
	
	private static void sanityCheck() {
		MyDatabase db = databaseCreationScalability(1);
		for(int i = 0; i < 10; i++) {
			ArrayList<Object> values = new ArrayList<Object>();
			values.add("Ion"+i);
			values.add(i);
			values.add(i%2==1);
			db.insert("Students1", values);
		}
		
		ArrayList<Object> values = new ArrayList<Object>();
		values.add("Ion"+20);
		values.add(20);
		values.add(20%2==1);
		db.update("Students1", values, "grade == 3");
		String[] operations = {"grade", "studentName", "grade", "gender", "grade"};
		ArrayList<ArrayList<Object>> results = db.select("Students1", operations, "grade < 100");
		System.out.println(results);
		String[] operations1 = {"sum(grade)", "min(grade)", "max(grade)", "avg(grade)", "count(grade)", "min(grade)"};
		results = db.select("Students1", operations1, "grade < 100");
		System.out.println(results);
		db.stopDb();
	}
	
	private static void testScalability() throws Exception {
		int[] numsThreads = {1,2,4};
		for(int numThreads : numsThreads) {
			System.out.println("There are now " + numThreads +" Threads");
			MyDatabase db = databaseCreationScalability(numThreads);
			CyclicBarrier barrier = new CyclicBarrier(numThreads+1);
			ScalabilityTestThread[] threads = new ScalabilityTestThread[numThreads];
			for(int threadId=0; threadId<numThreads; threadId++) {
				threads[threadId] = new ScalabilityTestThread(db, threadId, numThreads, barrier);
				threads[threadId].start();
			}
			barrier.await();
			long startTime = System.currentTimeMillis();
			barrier.await();//Here we do the insert
			barrier.await();
			long afterInsertTime = System.currentTimeMillis();
			barrier.await();//Here we do the update
			barrier.await();
			long afterUpdateTime = System.currentTimeMillis();
			barrier.await();//Here we do the select 
			barrier.await();
			long afterSelectTime = System.currentTimeMillis();

			System.out.println("Insert time "+(afterInsertTime - startTime));
			System.out.println("Update time "+(afterUpdateTime - afterInsertTime));
			System.out.println("Select time "+(afterSelectTime - afterUpdateTime));

			for(int threadId=0; threadId<numThreads; threadId++) {
				threads[threadId].join();
			}
			db.stopDb();
		}
	}

	private static void testConsistency() throws Exception {
		int numThreads = 4;
		MyDatabase db = databaseCreationConsistency(numThreads);
		CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
		ConsistencyWriterThreads[] threads = new ConsistencyWriterThreads[numThreads];
		ConsistencyReaderThread thread = new ConsistencyReaderThread(db, barrier);
		thread.start();
		for(int threadId=1; threadId < numThreads; threadId++) {
			threads[threadId] = new ConsistencyWriterThreads(db, barrier, threadId);
			threads[threadId].start();
		}
		barrier.await();
		barrier.await();
		barrier.await();
		
		for(int threadId=1; threadId<numThreads; threadId++) {
			threads[threadId].join();
		}
		thread.join();
		db.stopDb();
	}
	
	static MyDatabase databaseCreationScalability(int numWorkerThreads) {
		MyDatabase db = new Database();
		String[] columnNames = {"studentName", "grade", "gender"};
		String[] columnTypes = {"string", "int", "bool"};
		for(int i=0; i < 4; i++)
			db.createTable("Students"+i, columnNames, columnTypes);

		db.initDb(numWorkerThreads);
		return db;
	}
	
	static MyDatabase databaseCreationConsistency(int numWorkerThreads) {
		MyDatabase db = new Database();
		String[] columnNames = {"studentName", "grade0", "grade1", "grade2","grade3"};
		String[] columnTypes = {"string", "int", "int", "int", "int"};
		db.createTable("Students0", columnNames, columnTypes);

		db.initDb(numWorkerThreads);
		return db;
	}
}
