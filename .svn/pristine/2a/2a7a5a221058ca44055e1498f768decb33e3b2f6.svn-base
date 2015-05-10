package edu.upenn.cis455.mapreduce.helperclasses;

import java.io.*;
import java.math.BigInteger;
import java.security.*;
import java.util.HashMap;

public class Context implements edu.upenn.cis455.mapreduce.Context{

	private File file;
	private String status;
	private String spoolout_dir;
	private String spoolin_dir;
	private String output_dir;
	private int num_of_active_workers;
	private HashMap<Integer, BigInteger> divisions;

	/**
	 * Constructor for map phase
	 * @param s
	 * @param spoolout
	 * @param spoolin
	 * @param num
	 */
	public Context(String s, String spoolout, String spoolin, int num) {
		status = s;
		spoolin_dir = spoolin;
		spoolout_dir = spoolout;
		num_of_active_workers = num;
		generate_divisions(num_of_active_workers);
	}

	/**
	 * Constructor for reduce phase
	 * @param s
	 * @param output
	 */
	public Context(String s, String output) {
		System.out.println("Status is "+s);
		status = s;
		output_dir = output;
	}

	/**
	 * Function used by map and reduce to write the output
	 */
	public void write(String key, String value) {
		if (key == null || value == null || key.equals("") || value.equals(""))
			return;
		if (status.equals("mapping")) {
			file = new File(spoolout_dir+"/worker"+get_worker(key)+".txt");
		}
		else {
			file = new File(output_dir+"/output.txt");
		}
		try {
			PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
			out.println(key+"\t"+value);
			out.close();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Get the worker to which we have to assign the key to
	 * @param key
	 * @return
	 */
	private int get_worker(String key) {
		String hash_hex = get_hash(key);
		BigInteger hash = new BigInteger(hash_hex, 16);
		for(int i = 0; i<num_of_active_workers; i++) {
			if((hash.compareTo(divisions.get(i))) < 0) {
				return i+1;
			}
		}
		return 0;
	}

	/**
	 * Generate the hash divisions based on number of active workers
	 * @param n
	 */
	private void generate_divisions(int n) {
		String min_hex = "0000000000000000000000000000000000000000";
		String max_hex = "ffffffffffffffffffffffffffffffffffffffff";
		BigInteger min = new BigInteger(min_hex, 16);
		BigInteger max = new BigInteger(max_hex, 16);
		BigInteger num_of_divisions = new BigInteger(Integer.toString(num_of_active_workers));
		BigInteger division_size = max.divide(num_of_divisions);
		divisions = new HashMap<Integer, BigInteger>();
		for(int i = 0; i<num_of_active_workers; i++) {
			divisions.put(i, min.add(division_size));
			min = min.add(division_size);
		}
	}

	/**
	 * Function to compute the hash of a key
	 * @param key
	 * @return
	 */
	private String get_hash(String key) {
		MessageDigest md;
		StringBuffer sb = new StringBuffer();
		try {
			md = MessageDigest.getInstance("SHA-1");
			md.update(key.getBytes());
			byte digest[] = md.digest();
			for (int i = 0; i < digest.length; i++) {
				sb.append(Integer.toString((digest[i] & 0xff) + 0x100, 16).substring(1));
			}
		} 
		catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return sb.toString();
	}

}
