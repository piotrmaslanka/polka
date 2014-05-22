package zero.repair;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedDeque;

import zero.store.NotFoundException;
import zero.store.SeriesController;
import zero.store.SeriesDB;

/**
 * Thread that 
 * 	1) Get repair requests
 *  2) 
 */
public class ReparatorySupervisorThread extends Thread {

	static private ReparatorySupervisorThread instance;
	private volatile boolean isTerminated = false;
	
	public void terminate() {
		this.isTerminated = true;
	}	
	
	public static ReparatorySupervisorThread getInstance() {
		if (instance == null) instance = new ReparatorySupervisorThread();
		return instance;
	}
	
	private ConcurrentLinkedDeque<RepairRequest> requests = new ConcurrentLinkedDeque<>();
	
	public void postRequest(RepairRequest rr) {
		this.requests.addFirst(rr);
	}
	
	public ReparatorySupervisorThread() {
		super();
	}

	public void run() {
		while (!isTerminated) {
			RepairRequest rr = this.requests.pollLast();
			if (rr == null)
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
				}
			else {
				
				// does it make sense?
				SeriesController sercon = null;
				try {
					sercon = SeriesDB.getInstance().getSeries(rr.sd.seriesName);
					if (!sercon.doesPartialExist(rr.to)) {
						continue;
					}
				} catch (IOException | NotFoundException e) {
					continue;
				} finally {
					try {
						sercon.close();
					} catch (IOException e) {
						continue;
					}
				}
				ReparatoryThread rt = new ReparatoryThread(sercon, rr.from, rr.to);
				rt.start();
				try {
					rt.join();
				} catch (InterruptedException e) {
					continue;
				}
				
				System.out.flush();
			}
		}
	}
	
}
