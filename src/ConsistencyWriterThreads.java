import java.util.ArrayList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class ConsistencyWriterThreads extends Thread{

	private MyDatabase db;
	private CyclicBarrier barrier;
	private int threadId;

	public ConsistencyWriterThreads(MyDatabase db, CyclicBarrier barrier, int threadId) {
		this.db = db;
		this.barrier = barrier;
		this.threadId = threadId;
	}

	void barrierWrapper() {
		try {
			barrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}
	}
	
	public void run() {
		for(int i = 0; i < 10_000; i++) {
			ArrayList<Object> values = new ArrayList<Object>();
			values.add("Ion"+(i+(threadId*1_000_000)));
			values.add(i+(threadId*1_000_000));
			values.add(i+(threadId*1_000_000));
			values.add(i+(threadId*1_000_000));
			values.add(i+(threadId*1_000_000));
			db.insert("Students0", values);
		}
		barrierWrapper();
		for(int i = 0; i < 1_000; i++) {
			ArrayList<Object> values = new ArrayList<Object>();
			values.add("George"+i);
			values.add(-i*threadId);
			values.add(-i*threadId);
			values.add(-i*threadId);
			values.add(-i*threadId);
			db.update("Students0", values, "grade0 == "+i);
		}
		barrierWrapper();
		for(int i=0;i<10;i++) {
			db.startTransaction("Students0");

			String[] operations = {"studentName", "grade0", "grade1", "grade2", "grade3"};
			ArrayList<ArrayList<Object>> resultsA = db.select("Students0", operations, "studentName == Ion1000000");
			ArrayList<ArrayList<Object>> resultsB = db.select("Students0", operations, "studentName == Ion2000000");
			ArrayList<Object> valuesA = new ArrayList<Object>();
			ArrayList<Object> valuesB = new ArrayList<Object>();

			valuesA.add(resultsA.get(0).get(0));
			valuesB.add(resultsB.get(0).get(0));
			
			valuesA.add((int)resultsA.get(1).get(0)+1);
			valuesB.add((int)resultsB.get(1).get(0)-1);
			
			valuesA.add(resultsA.get(2).get(0));
			valuesB.add(resultsB.get(2).get(0));
			
			valuesA.add(resultsA.get(3).get(0));
			valuesB.add(resultsB.get(3).get(0));
			
			valuesA.add(resultsA.get(4).get(0));
			valuesB.add(resultsB.get(4).get(0));

			db.update("Students0", valuesA, "studentName == Ion1000000");
			db.update("Students0", valuesB, "studentName == Ion2000000");
			db.endTransaction("Students0");
		}
		barrierWrapper();
	}
}
