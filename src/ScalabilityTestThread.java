import java.util.ArrayList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class ScalabilityTestThread extends Thread{
	MyDatabase db;
	int threadId;
	int numThreads;
	CyclicBarrier barrier;

	ScalabilityTestThread(MyDatabase db, int threadId, int numThreads, CyclicBarrier barrier) {
		this.db = db;
		this.threadId = threadId;
		this.numThreads = numThreads;
		this.barrier = barrier;
	}

	void barrierWrapper() {
		try {
			barrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}
	}
	
	public void run() {
		int jobsPerThread = 4/numThreads; //each thread has a job
		barrierWrapper();
		barrierWrapper();
		// -- multiple clients insert in multiple tables
		for(int partialJobId=0; partialJobId<jobsPerThread; partialJobId++) {
			int jobId = (jobsPerThread * threadId) + partialJobId;
			ArrayList<Object> values = new ArrayList<Object>();
			for(int i = 0; i < 100_000; i++) {
				//values.clear();
				values = new ArrayList<Object>();
				values.add("Ion"+(i*(jobId+1)));
				values.add(i*(jobId+1));
				values.add(i%2==1);
				db.insert("Students" + jobId, values);
			}
		}
		barrierWrapper();
		barrierWrapper();
		//-- one client update in one table
		if(threadId==0) {
			ArrayList<Object> values = new ArrayList<Object>();
			values.add("Ioana");
			values.add(3);
			values.add(true);
			db.update("Students2", values, "gender == true");
		}
		barrierWrapper();
		barrierWrapper();
		// -- one client thread selects one table
		ArrayList<ArrayList<Object>> results = null;
		if(threadId==0) {
			String[] operations = {"sum(grade)"};
			results = db.select("Students0", operations, "grade < 10000000");
			results = db.select("Students1", operations, "grade < 10000000");
			results = db.select("Students2", operations, "grade == 3");
			results = db.select("Students3", operations, "grade > 10");
		}
		barrierWrapper();
		if(threadId==0) {
			System.out.println(results);
		}
	}
}
