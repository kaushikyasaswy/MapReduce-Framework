package edu.upenn.cis455.mapreduce.worker;

import edu.upenn.cis455.mapreduce.helperclasses.*;

public class WorkerStatusUpdate implements Runnable {

	WorkerServlet ws;
	Client client;

	/**
	 * Constructor
	 * @param w
	 */
	public WorkerStatusUpdate(WorkerServlet w) {
		ws = w;
		client = new Client();
	}

	/**
	 * Run method of the thread
	 */
	public void run() {
		while (true) {
			client.setURL("http://"+ws.get_master()+"/master/workerstatus");
			client.clear_request_headers();
			client.set_get_param("port", ""+ws.get_port());
			client.set_get_param("status", ws.get_status());
			client.set_get_param("job", ws.get_job());
			client.set_get_param("keysRead", ""+ws.get_keys_read());
			client.set_get_param("keysWritten", ""+ws.get_keys_written());
			client.send_get_request();
			try {
				Thread.sleep(10000);
			} 
			catch (InterruptedException e) {
				continue;
			}
		}
	}

}