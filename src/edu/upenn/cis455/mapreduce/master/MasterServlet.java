package edu.upenn.cis455.mapreduce.master;

import edu.upenn.cis455.mapreduce.helperclasses.*;

import java.io.*;
import java.util.*;
import javax.servlet.*;
import javax.servlet.http.*;

public class MasterServlet extends HttpServlet {

	static final long serialVersionUID = 455555001;

	private HashMap<String, WorkerStatus> workers_map = new HashMap<String, WorkerStatus>();
	private String ip;
	private int port;
	private String status;
	private String job;
	private int keys_read;
	private int keys_written;
	private long last_received;
	private boolean job_running = false;
	private String input_dir;
	private String output_dir;
	private int number_of_map_threads;
	private int number_of_reduce_threads;
	private int number_of_workers_active;
	private ArrayList<String> workers_active = new ArrayList<>();

	/**
	 * doGet method of the servlet
	 */
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException
	{
		PrintWriter out = response.getWriter();
		String req_uri = request.getRequestURI();
		if (req_uri.contains("/workerstatus")) {
			WorkerStatus ws = new WorkerStatus();
			last_received = new Date().getTime();
			ip = request.getHeader("X-FORWARDED-FOR"); // If the client is sitting behind a proxy, i.e the request received is a redirected request, we need to the get the client's IP
			if (ip == null) {  
				ip = request.getRemoteAddr();  
			}
			try {
				port = Integer.parseInt(request.getParameter("port"));
				if (port < 0 || port > 65535) {
					out.println("<html><body><h1>ERROR</h1>Port number invalid</body></html>");
					return;
				}
				status = request.getParameter("status");
				if (!status.equals("mapping") && !status.equals("waiting") && !status.equals("reducing") && !status.equals("idle")) {
					out.println("<html><body><h1>ERROR</h1>Status invalid</body></html>");
					return;
				}
				job = request.getParameter("job");
				keys_read = Integer.parseInt(request.getParameter("keysRead"));
				keys_written = Integer.parseInt(request.getParameter("keysWritten"));
			}
			catch(Exception e) {
				out.println("<html><body><h1>ERROR</h1>Error with some parameter(s)</body></html>");
				return;
			}
			ws.ip = this.ip;
			ws.port = this.port;
			ws.status = this.status;
			ws.job = this.job;
			ws.keys_written = this.keys_written;
			ws.keys_read = this.keys_read;
			ws.last_received = this.last_received;
			workers_map.put(ip+":"+port, ws);
			if (job_running) {
				if (check_if_waiting()) {
					send_post_runreduce();
				}
				else if (check_if_idle()) {
					job_running = false;
				}
			}
		}
		else if (req_uri.contains("/status")) {
			long current_time = new Date().getTime();
			out.println("<html><body><table border=\"1\" style=\"width:100%\"><tr><th>IP:Port</th><th>Status</th><th>Job</th><th>KeysRead</th><th>KeysWritten</th></tr>");
			for (Map.Entry<String, WorkerStatus> worker : workers_map.entrySet()) {
				if ((current_time - worker.getValue().last_received) < 30000) {
					out.println("<tr><td>"+worker.getKey()+"</td><td>"+worker.getValue().status+"</td><td>"+worker.getValue().job+"</td><td>"+worker.getValue().keys_read+"</td><td>"+worker.getValue().keys_written+"</td></tr>");
				}
			}
			if (job_running) {
				out.println("</table><br><hr><br><p>A Job is currently running. Please wait for it to finish</p></body></html>");
			}
			else {
				out.println("</table><br><hr><br>");
				out.println("<form action=\"master\" method=\"post\">");
				out.println("<label>Job:   </label> <input name=\"job\" id=\"job\"/><br>");
				out.println("<label>Input Directory:   </label> <input name=\"input\" id=\"input\"/><br>");
				out.println("<label>Output Directory:   </label> <input name=\"output\" id=\"output\"/><br>");
				out.println("<label>Number of Map Threads:   </label> <input name=\"mapthreads\" id=\"mapthreads\"/><br>");
				out.println("<label>Number of Reduce Threads:   </label> <input name=\"reducethreads\" id=\"reducethreads\"/><br>");		
				out.println("<br><button type=\"submit\">Submit</button></body></html>");
			}
		}
	}

	/**
	 * doPost method of the servlet
	 */
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException
	{
		try {
			job = request.getParameter("job");
			input_dir = request.getParameter("input");
			output_dir = request.getParameter("output");
			number_of_map_threads = Integer.parseInt(request.getParameter("mapthreads"));
			number_of_reduce_threads = Integer.parseInt(request.getParameter("reducethreads"));
		}
		catch(Exception e) {
			PrintWriter out = response.getWriter();
			out.println("<html><body><h1>ERROR</h1>Problem with parameters entered</body></html>");
			return;
		}
		long current_time = new Date().getTime();
		number_of_workers_active = 0;
		workers_active.clear();
		for (Map.Entry<String, WorkerStatus> worker : workers_map.entrySet()) {
			if (current_time - worker.getValue().last_received < 30000) {
				number_of_workers_active++;
				workers_active.add(worker.getKey());
			}
		}
		job_running = true;
		send_post_runmap();
	}

	/**
	 * Function to send a /runmap post request to each of the active workers
	 */
	public void send_post_runmap() {
		Client client  = new Client();
		long current_time = new Date().getTime();
		for (Map.Entry<String, WorkerStatus> worker : workers_map.entrySet()) {
			if (current_time - worker.getValue().last_received < 30000) {
				client.setURL("http://"+worker.getKey()+"/worker/runmap");
				client.clear_post_params();
				client.clear_request_headers();
				client.set_post_param("job",job);
				client.set_post_param("input",input_dir);
				client.set_post_param("numThreads", ""+number_of_map_threads);
				client.set_post_param("numWorkers", ""+number_of_workers_active);
				int i = 0;
				for (String active_worker : workers_active) {
					i++;
					client.set_post_param("worker"+i, active_worker);
				}
				client.send_post_request();
			}
		}
	}

	/**
	 * Function to send a /runreduce post request to each of the active workers
	 */
	public void send_post_runreduce() {
		Client client  = new Client();
		long current_time = new Date().getTime();
		for (Map.Entry<String, WorkerStatus> worker : workers_map.entrySet()) {
			if (current_time - worker.getValue().last_received < 30000) {
				client.setURL("http://"+worker.getKey()+"/worker/runreduce");
				client.clear_post_params();
				client.clear_request_headers();
				client.set_post_param("job",job);
				client.set_post_param("output",output_dir);
				client.set_post_param("numThreads", ""+number_of_reduce_threads);
				client.send_post_request();
			}
		}
	}

	/**
	 * Function to check if all the active workers are done mapping and in a waiting state
	 * @return true if all the active workers are in a waiting state and false otherwise
	 */
	public boolean check_if_waiting() {
		boolean flag = true;
		long current_time = new Date().getTime();
		for (Map.Entry<String, WorkerStatus> worker : workers_map.entrySet()) {
			if (current_time - worker.getValue().last_received < 30000) {
				if (!worker.getValue().status.equals("waiting"))
					flag = false;
			}
		}
		return flag;
	}

	/**
	 * Function to check if all the active workers are done reducing and in an idle state
	 * @return true if all the active workers are in an idle state and false otherwise
	 */
	public boolean check_if_idle() {
		boolean flag = true;
		long current_time = new Date().getTime();
		for (Map.Entry<String, WorkerStatus> worker : workers_map.entrySet()) {
			if (current_time - worker.getValue().last_received < 30000) {
				if (!worker.getValue().status.equals("idle"))
					flag = false;
			}
		}
		return flag;
	}
}