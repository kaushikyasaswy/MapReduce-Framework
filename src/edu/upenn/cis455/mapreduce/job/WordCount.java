package edu.upenn.cis455.mapreduce.job;

import java.util.StringTokenizer;
import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class WordCount implements Job{

	/**
	 * Function for wordcount map
	 */
	public void map(String key, String value, Context context)
	{
		StringTokenizer st = new StringTokenizer(value);
		while(st.hasMoreTokens()) {
			String key_new = st.nextToken();
			String value_new = "1";
			context.write(key_new, value_new);
		}
	}

	/**
	 * Function for wordcount reduce
	 */
	public void reduce(String key, String[] values, Context context)
	{
		try{
			if(key.equals("")||values.equals(null))
				return;
			int count = 0;
			for(String value : values){
				count += Integer.parseInt(value);
			}
			context.write(key,Integer.toString(count));
		}
		catch(NumberFormatException e) {
			e.printStackTrace();
		}
	}

}
