package edu.upenn.cis455.mapreduce.master;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.helperclasses.Context;
import edu.upenn.cis455.mapreduce.worker.WorkerServlet;

public class ReduceThread implements Runnable{

	private WorkerServlet ws;
	private Job job;
	private Context context;
	private BufferedReader br;
	private String key;
	private String next_key;
	private ArrayList<String> values;

	/**
	 * Constructor
	 * @param w is the object of the worker servlet
	 * @param c is the context for reducing phase
	 * @param f is the file to write the output to
	 */
	public ReduceThread(WorkerServlet w, Context c, File f) {
		ws = w;
		job = ws.get_job_class();
		context = c;
		key = null;
		next_key = null;
		values = new ArrayList<String>();
		try {
			br = new BufferedReader(new FileReader(f));
		} 
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Function to find sets of rows in the reduce.txt file
	 * @return true if there are more keysets to read and false otherwise
	 */
	private synchronized boolean read_keys_set() {
		try {
			String line;
			if (next_key == null) {
				if ((line = br.readLine()) == null)
					return false;
				if (line.trim().equals(""))
					return false;
				ws.increment_keys_read();
				key = line.split("\t")[0];
				values.add(line.split("\t")[1]);
			}
			else {
				key = next_key;
			}
			while(true) {
				line = br.readLine();
				if (line == null || line.trim().equals("")) {
					next_key = null;
					String[] values_array = new String[values.size()];
					int i = 0;
					for (String value : values) {
						values_array[i++] = value;
					}
					job.reduce(key, values_array, context);
					ws.increment_keys_written();
					return false;
				}
				ws.increment_keys_read();
				next_key = line.split("\t")[0];
				if (next_key.equals(key)) {
					values.add(line.split("\t")[1]);
				}
				else {
					String[] values_array = new String[values.size()];
					int i = 0;
					for (String value : values) {
						values_array[i++] = value;
					}
					job.reduce(key, values_array, context);
					ws.increment_keys_written();
					values.clear();
					values.add(line.split("\t")[1]);
					break;
				}
			}
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * The run method to perform reduce
	 */
	public void run() {
		boolean keep_reading = read_keys_set();
		while(keep_reading) {
			keep_reading = read_keys_set();
			// Keep running till reduce is done
		}
	}

}
