package edu.upenn.cis455.mapreduce.helperclasses;

import java.io.*;
import java.net.*;
import java.util.*;

public class Client {

	private URL url;
	private String url_str;
	private String response_body;
	private Socket clientSocket;
	private HashMap<String, String> post_params;
	private HashMap<String, String> get_params;
	private HashMap<String, String> request_headers;
	private HashMap<String, String> response_headers;
	private String post_body;

	/**
	 * Constructor
	 */
	public Client() {
		this.post_params = new HashMap<String, String>();
		this.get_params = new HashMap<String, String>();
		this.request_headers = new HashMap<String, String>();
		this.response_headers = new HashMap<String, String>();
		post_body = "";
	}

	/**
	 * Set a post request parameter
	 * @param key
	 * @param value
	 */
	public void set_post_param(String key, String value) {
		try {
			post_params.put(URLEncoder.encode(key, "UTF-8"), URLEncoder.encode(value, "UTF-8"));
		} 
		catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Set the post request body
	 * @param body
	 */
	public void set_post_body(String body) {
		post_body = body;
	}

	/**
	 * Set the get request parameters
	 * @param key
	 * @param value
	 */
	public void set_get_param(String key, String value) {
		try {
			get_params.put(URLEncoder.encode(key, "UTF-8"), URLEncoder.encode(value, "UTF-8"));
		} 
		catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Set a request header
	 * @param key
	 * @param value
	 */
	public void set_request_header(String key, String value) {
		try {
			request_headers.put(URLEncoder.encode(key, "UTF-8"), URLEncoder.encode(value, "UTF-8"));
		} 
		catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Clear all the post params
	 */
	public void clear_post_params() {
		post_params.clear();
	}

	/**
	 * Clear all the request headers
	 */
	public void clear_request_headers() {
		request_headers.clear();
	}

	/**
	 * Set the request URL
	 * @param u
	 */
	public void setURL(String u) {
		url_str = u;
	}

	/**
	 * Send a post request
	 */
	public void send_post_request() {
		/*if (url.getProtocol().equalsIgnoreCase("https"))
		{
			send_post_https_request();
			return;
		}*/
		try {
			url = new URL(url_str);
			InetAddress address;
			address = InetAddress.getByName(url.getHost());
			clientSocket = new Socket(address.getHostAddress(), url.getPort());
			clientSocket.setSoTimeout(10000);
		} 
		catch (Exception e1) {
			e1.printStackTrace();
		}
		BufferedWriter bw;
		BufferedReader br;
		response_body = "";
		String line;
		try {
			bw = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream(), "UTF8"));
			bw.write("POST " + url_str + " HTTP/1.0\r\n");
			String parameters_string = "";
			if (!post_body.equals("")) {
				if (!request_headers.containsKey("Content-Type")) {
					request_headers.put("Content-Type", "text/plain");
				}
				parameters_string += post_body;
			}
			else {
				if (!request_headers.containsKey("Content-Type")) {
					request_headers.put("Content-Type", "application/x-www-form-urlencoded");
				}
				int i = 1;
				for (Map.Entry<String, String> param : post_params.entrySet()) {
					if (i==post_params.size()) {
						parameters_string += param.getKey() + "=" + param.getValue();
					}
					else {
						parameters_string += param.getKey() + "=" + param.getValue() + "&";
					}
					i++;
				}
			}
			request_headers.put("Content-Length", ""+parameters_string.getBytes().length);
			for (Map.Entry<String, String> header : request_headers.entrySet()) {
				bw.write(header.getKey() + ": " + header.getValue() + "\r\n");
			}
			bw.write("\r\n");
			bw.write(parameters_string);
			bw.write("\r\n\r\n");
			bw.flush();
			br = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			while ((line = br.readLine()) != null) {
				response_body += line;
			} 
			bw.close();
			br.close();
		}
		catch (SocketTimeoutException e) {
			e.printStackTrace();
			return;
		}
		catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}

	/**
	 * Not needed
	 */
	public void send_post_https_request() {

	}

	/**
	 * Send a get request
	 */
	public void send_get_request() {
		/*if (url.getProtocol().equalsIgnoreCase("https"))
		{
			send_post_https_request();
			return;
		}*/
		try {
			boolean trying_to_connect = true;
			url = new URL(url_str);
			InetAddress address;
			address = InetAddress.getByName(url.getHost());
			while (trying_to_connect) {
				try {
					clientSocket = new Socket(address.getHostAddress(), url.getPort());
					clientSocket = new Socket(url.getHost(), url.getPort());
					trying_to_connect = false;
				}
				catch (ConnectException e) {
					System.out.println("Server down. Trying to connect again");
					try {
						Thread.sleep(2000);
					} 
					catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				}
			}
			clientSocket.setSoTimeout(10000);
		}
		catch (Exception e1) {
			e1.printStackTrace();
		}
		BufferedWriter bw;
		BufferedReader br;
		response_body = "";
		String line;
		try {
			bw = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream(), "UTF8"));
			String first_line;
			first_line = "GET " + url_str + "?";
			for (Map.Entry<String, String> param : get_params.entrySet()) {
				first_line += param.getKey() + "=" + param.getValue() + "&";
			}
			first_line = first_line.substring(0, first_line.length()-1);
			first_line += " HTTP/1.0\r\n";
			bw.write(first_line);
			for (Map.Entry<String, String> header : request_headers.entrySet()) {
				bw.write(header.getKey() + "=" + header.getValue() + "\r\n");
			}
			bw.write("\r\n");
			bw.flush();
			br = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			while ((line = br.readLine()) != null) {
				response_body += line;
			}
			bw.close();
			br.close();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Function to send a https request to the url
	 *//*
	private void send_https_request()
	{
		HttpsURLConnection con = null;
		int status=0;
		try{
			con = (HttpsURLConnection) url.openConnection();
			con.setRequestMethod(method);
			con.setRequestProperty("User-Agent", "cis455crawler");
			con.setInstanceFollowRedirects(false);
			for (Map.Entry<String, String> entry : request_headers.entrySet())
				con.setRequestProperty(entry.getKey(), entry.getValue());
			status = con.getResponseCode();
		}
		catch(Exception e) {
			System.out.println(e);
		}
		response_map.put("status", ""+status);
		if (status!=200 && status!=301 && status!=304 && status!=307)
		{
			input = null;
			return;
		}
		response_map.put("content-length", ""+con.getContentLength());
		response_map.put("content-type", con.getContentType());
		if (request_headers.containsKey("if-modified-since"))
			response_map.put("last-modified", getDate(con.getLastModified()));
		if(con.getHeaderField("location") != null)
			response_map.put("location", con.getHeaderField("location"));
		try {
			input = new BufferedReader(new InputStreamReader((con.getInputStream())));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		StringBuilder sb = new StringBuilder();
		String output;
		boolean first = true;
		try {
			while ((output = input.readLine()) != null) {
				if (first) {
					sb.append(output);
					first = false;
					continue;
				}
				sb.append("\n"+output);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		response_body = sb.toString();
	}*/

	/**
	 * Function to get the response_body of a response
	 * @return null in case it is a HEAD request and the response_body in case it is a GET request
	 */
	public String getBody() {
		return response_body;
	}

}