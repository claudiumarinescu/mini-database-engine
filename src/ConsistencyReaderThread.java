import java.util.ArrayList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class ConsistencyReaderThread extends Thread{

	private MyDatabase db;
	private CyclicBarrier barrier;

	public ConsistencyReaderThread(MyDatabase db, CyclicBarrier barrier) {
		this.db = db;
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
		ArrayList<ArrayList<Object>> results = null;
		String[] operations = {"sum(grade0)", "sum(grade1)", "sum(grade2)", "sum(grade3)"};
		boolean passed = true;
		for(int i=0;i<10000;i++) {
			results = db.select("Students0", operations, "grade0 > -1");
			for(int index=1; index<results.size(); index++)
				if(!results.get(0).isEmpty() && !results.get(index).isEmpty() && (int)results.get(index).get(0)!=(int)results.get(0).get(0)) {
					System.out.println("Select/Insert Consistency FAIL"+results.get(index).get(0)+" "+results.get(0).get(0));
					passed = false;
				}
		}
		if(passed)
			System.out.println("Select/Insert Consistency PASS");
		passed = true;
		barrierWrapper();
		for(int i=0;i<20;i++) {
			results = db.select("Students0", operations, "grade0 > -1");
			for(int index=1; index<results.size(); index++)
				if(!results.get(0).isEmpty() && !results.get(index).isEmpty() && (int)results.get(index).get(0)!=(int)results.get(0).get(0)) {
					System.out.println("Select/Update Consistency FAIL"+results.get(index).get(0)+" "+results.get(0).get(0));
					passed = false;
				}
		}
		if(passed)
			System.out.println("Select/Update Consistency PASS");
		passed = true;
		barrierWrapper();
		
		/*if (!results.get(0).isEmpty()) {
			int correctSum = (int) results.get(0).get(0);
			for(int i=0;i<20;i++) {
				results = db.select("Students0", operations, "grade0 > -1");
				for(int index=0; index<results.size(); index++)
					if(!results.get(index).isEmpty() && (int)results.get(index).get(0)!=correctSum) {
						System.out.println("Transactions Consistency FAIL" + results.get(index).get(0)+" "+correctSum);
						passed = false;
					}
			}
		}
		if(passed)
			System.out.println("Transactions Consistency PASS");*/
		barrierWrapper();
	}

}
