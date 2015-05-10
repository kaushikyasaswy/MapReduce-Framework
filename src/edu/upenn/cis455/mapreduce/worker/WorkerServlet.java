package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.util.*;

import javax.servlet.*;
import javax.servlet.http.*;

import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.helperclasses.*;
import edu.upenn.cis455.mapreduce.master.MapThread;
import edu.upenn.cis455.mapreduce.master.ReduceThread;

public class WorkerServlet extends HttpServlet {

	static final long serialVersionUID = 455555002;

	private String master;
	private String status;
	private int port;
	private String job;
	private int keys_read;
	private int keys_written;
	private String storagedir;
	private Job job_class;
	private int num_of_threads;
	private int num_of_active_workers;
	private HashMap<String, String> active_workers = new HashMap<String, String>();
	private String input_dir;
	private String output_dir;
	private Queue<File> input_files = new LinkedList<File>();
	private Queue<String> input_lines = new LinkedList<String>();
	private ArrayList<Thread> map_threads = new ArrayList<Thread>();
	private ArrayList<Thread> reduce_threads = new ArrayList<Thread>();
	private Context context;
	private String spoolout_dir;
	private String spoolin_dir;
	private int num_of_spoolin_files;
	private WorkerStatusUpdate workerstatusupdate_thread;
	private Thread statusthread;

	/**
	 * Function to return the master
	 */
	public String get_master() {
		return master;
	}

	/**
	 * Function to return the status
	 * @return
	 */
	public String get_status() {
		return status;
	}

	/**
	 * Function to return the port
	 * @return
	 */
	public int get_port() {
		return port;
	}

	/**
	 * Function to return the job
	 * @return
	 */
	public String get_job() {
		return job;
	}

	/**
	 * Function to return the number of keys read
	 * @return
	 */
	public int get_keys_read() {
		return keys_read;
	}

	/**
	 * Function to return the number of keys written
	 * @return
	 */
	public int get_keys_written() {
		return keys_written;
	}

	/**
	 * Function to initialize the servlet
	 */
	public void init(ServletConfig config) {
		master = config.getInitParameter("master");
		port = Integer.parseInt(config.getInitParameter("port"));
		storagedir = config.getInitParameter("storagedir");
		if (storagedir.endsWith("/")) 
			storagedir = storagedir.substring(0, storagedir.length()-1);
		if (!storagedir.startsWith("/"))
			storagedir = "/" + storagedir;
		spoolin_dir = storagedir+"/spool_in";
		spoolout_dir = storagedir+"/spool_out";
		clear_spool_directories();
		create_spoolout_files();
		File f = new File(spoolin_dir+"/");
		if (!f.exists()) {
			f.mkdir();
		}
		status = "idle";
		job = "";
		keys_read = 0;
		keys_written = 0;
		num_of_spoolin_files = 0;
		workerstatusupdate_thread = new WorkerStatusUpdate(this);
		statusthread = new Thread(workerstatusupdate_thread);
		statusthread.start();
	}

	/**
	 * doPost method of the servlet
	 */
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException
	{
		String req_uri = request.getRequestURI();
		if (req_uri.contains("/runmap")) {
			runmap_handler(request, response);
		}
		else if (req_uri.contains("/pushdata")) {
			pushdata_handler(request, response);
		}
		else if (req_uri.contains("/runreduce")) {
			runreduce_handler(request, response);
		}
	}

	/**
	 * Function to handle a runmap request
	 * @param request
	 * @param response
	 */
	private void runmap_handler(HttpServletRequest request, HttpServletResponse response) {
		status = "mapping";
		keys_read = 0;
		keys_written = 0;
		job = request.getParameter("job");
		try {
			job_class = (Job) Class.forName(job).newInstance();
		} 
		catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			e.printStackTrace();
		}
		num_of_threads = Integer.parseInt(request.getParameter("numThreads"));
		num_of_active_workers = Integer.parseInt(request.getParameter("numWorkers"));
		for (int i=1; i<=num_of_active_workers; i++) {
			active_workers.put("worker"+i, request.getParameter("worker"+i));
		}
		String input = request.getParameter("input");
		if (input.startsWith("/"))
			input = input.substring(1);
		input_dir = storagedir + "/" + input + "/";
		File[] files = new File(input_dir).listFiles();
		for (int i=0; i<files.length; i++)
			input_files.add(files[i]);
		context = new Context(status, spoolout_dir, spoolin_dir, num_of_active_workers);
		MapThread map_handler = new MapThread(this, context);
		for (int i=0; i<num_of_threads; i++) {
			Thread t = new Thread(map_handler);
			map_threads.add(t);
			t.start();
		}
		while (!check_mapping_done()) {
			// Wait till mapping is done
		}
		pushdata();
		status = "waiting";
		statusthread.interrupt();
	}

	/**
	 * Function to handle pushdata request
	 * @param request
	 * @param response
	 */
	private void pushdata_handler(HttpServletRequest request, HttpServletResponse response) {
		num_of_spoolin_files++;
		StringBuilder buffer = new StringBuilder();
		BufferedReader reader;
		try {
			reader = request.getReader();
			String line;
			while ((line = reader.readLine()) != null) {
				buffer.append(line + "\n");
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		String data = buffer.toString();
		File file = new File(spoolin_dir+"/file"+num_of_spoolin_files+".txt");
		try {
			PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
			out.println(data);
			out.close();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Function to handle runreduce request
	 * @param request
	 * @param response
	 */
	private void runreduce_handler(HttpServletRequest request, HttpServletResponse response) {
		status = "reducing";
		keys_read = 0;
		keys_written = 0;
		job = request.getParameter("job");
		try {
			job_class = (Job) Class.forName(job).newInstance();
		} 
		catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			e.printStackTrace();
		}
		num_of_threads = Integer.parseInt(request.getParameter("numThreads"));
		String output = request.getParameter("output");
		if (output.startsWith("/"))
			output = output.substring(1);
		output_dir = storagedir + "/" + output;
		File f = new File(output_dir);
		if (!f.exists()) {
			f.mkdir();
		}
		else {
			delete_dir(f);
			f.mkdir();
		}
		combine_all_files(spoolin_dir);
		sort_file(spoolin_dir+"/reduce_me.txt");
		context = new Context(status, output_dir);
		ReduceThread reduce_handler = new ReduceThread(this, context, new File(spoolin_dir+"/reduce.txt"));
		for (int i=0; i<num_of_threads; i++) {
			Thread t = new Thread(reduce_handler);
			reduce_threads.add(t);
			t.start();
		}
		while(!check_reducing_done()) {
			//
		}
		status = "idle";
		statusthread.interrupt();
	}

	/**
	 * Function to check if the threads have completed reducing
	 * @return
	 */
	private boolean check_reducing_done() {
		boolean flag = true;
		for (Thread t : reduce_threads) {
			if (t.isAlive()) {
				flag = false;
			}
		}
		return flag;
	}

	/**
	 * Function to check if the threads have completed mapping
	 * @return
	 */
	private boolean check_mapping_done() {
		boolean flag = true;
		for (Thread t : map_threads) {
			if (t.isAlive()) {
				flag = false;
			}
		}
		return flag;
	}

	/**
	 * Function to increment the  number of keys read
	 */
	public synchronized void increment_keys_read() {
		keys_read++;
	}

	/**
	 * Function to increment the number of keys written
	 */
	public synchronized void increment_keys_written() {
		keys_written++;
	}

	/**
	 * Function to get an input file
	 * @return
	 */
	public synchronized File get_input_file() {
		if (!input_files.isEmpty())
			return input_files.remove();
		else
			return null;
	}

	/**
	 * Function to check if any input files are left
	 * @return
	 */
	public synchronized boolean is_input_files_empty() {
		if (input_files.isEmpty())
			return true;
		else
			return false;
	}

	/**
	 * Function to add an input line to the queue
	 * @param line
	 */
	public synchronized void add_input_line(String line) {
		input_lines.add(line);
	}

	/**
	 * Function to get an input line from the queue
	 * @return
	 */
	public synchronized String get_input_line() {
		if (!is_input_lines_empty())
			return input_lines.remove();
		else
			return null;
	}

	/**
	 * Function to check if any input lines are left
	 * @return
	 */
	public synchronized boolean is_input_lines_empty() {
		if (input_lines.isEmpty())
			return true;
		else
			return false;
	}

	/**
	 * Function to get the job
	 * @return
	 */
	public Job get_job_class() {
		return job_class;
	}

	/**
	 * Function to create spoolout files
	 */
	private void create_spoolout_files() {
		File spoolout = new File(spoolout_dir);
		spoolout.mkdir();
		for(int i=1; i<=num_of_active_workers; i++) {
			File f = new File(spoolout_dir + "/worker" + i + ".txt");
		}
	}

	/**
	 * Function to clear the spool in and out directories if they already exist
	 */
	private void clear_spool_directories() {
		File spoolin = new File(spoolin_dir);
		if (spoolin.exists()) {
			delete_dir(spoolin);
		}
		File spoolout = new File(spoolout_dir);
		if (spoolout.exists()) {
			delete_dir(spoolout);
		}
	}

	/**
	 * Function to delete the contents of a directory
	 * @param dir
	 */
	private void delete_dir(File dir) {
		if (dir.listFiles().length == 0)
			return;
		for(File f: dir.listFiles()) {
			if(f.isDirectory())
				delete_dir(f);
			else 
				f.delete();
		}
		dir.delete();
	}

	/**
	 * Function to push the data from the spool out directory to the respective workers
	 */
	private void pushdata() {
		try {
			Client client  = new Client();

			for (Map.Entry<String, String> worker : active_workers.entrySet()) {
				String url = "http://" + worker.getValue() + "/worker/pushdata";
				client.setURL(url);
				client.clear_post_params();
				client.clear_request_headers();
				client.set_post_body(get_file_data(spoolout_dir + "/" + worker.getKey() +".txt"));
				client.send_post_request();
			}
		}
		catch(Exception e) {}
	}

	/**
	 * Function to get the data present in a file
	 * @param file
	 * @return
	 */
	private String get_file_data(String file) {
		String data = "";
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line;
			while((line = br.readLine())!=null) {
				if(line.equals("")||line.equals("\n"))
					continue;
				data += line+"\n";
			}
			br.close();
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		return data;
	}

	/**
	 * Function to combine all the files data
	 * @param dir
	 */
	private void combine_all_files(String dir) {
		File directory = new File(dir);
		File file = new File(dir+"/reduce_me.txt");
		PrintWriter out;
		try {
			out = new PrintWriter(new BufferedWriter(new FileWriter(file)));
			String data = "";
			for (File f : directory.listFiles()) {
				if (f.getName().startsWith("file")) {
					data += get_file_data(f.getAbsolutePath());
				}
			}
			out.println(data);
			out.close();
		} 
		catch (IOException e) {
			//e.printStackTrace();
		}
	}

	/**
	 * Function to sort the contents of a file
	 * @param filename
	 */
	private void sort_file(String filename) {
		try {
			Process p = Runtime.getRuntime().exec("sort " + filename);
			BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
			File f = new File(spoolin_dir + "/reduce.txt");
			FileWriter fw = new FileWriter(f.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			String line;
			while((line=br.readLine())!=null) {
				if (line.trim().equals(""))
					continue;
				bw.write(line+"\n");
			}
			bw.close();
			br.close();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}