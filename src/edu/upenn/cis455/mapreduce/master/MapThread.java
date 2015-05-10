package edu.upenn.cis455.mapreduce.master;

import java.io.*;
import java.util.*;
import edu.upenn.cis455.mapreduce.helperclasses.Context;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.worker.WorkerServlet;

public class MapThread implements Runnable{

	private WorkerServlet ws;
	private Job job;
	private Context context;
	
	/**
	 * Constructor
	 * @param w
	 * @param c
	 */
	public MapThread(WorkerServlet w, Context c) {
		ws = w;
		job = ws.get_job_class();
		context = c;
	}
	
	/**
	 * Run method of the thread
	 */
	public void run() {
		String input_line;
		while (!ws.is_input_files_empty()) {
			File file = ws.get_input_file();
			try {
				BufferedReader br = new BufferedReader(new FileReader(file));
				String line;
				while((line = br.readLine())!=null) {
					ws.add_input_line(line);
					ws.increment_keys_read();
				}
				br.close();
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		while (!ws.is_input_lines_empty()) {
			input_line = ws.get_input_line();
			job.map(input_line.split("\t")[0], input_line.split("\t")[1], context);
			ws.increment_keys_written();
		}
	}
}