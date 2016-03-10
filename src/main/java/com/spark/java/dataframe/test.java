package com.spark.java.dataframe;

public class test {

	public static void main(String[] args) {
		String test="billing_period_id,  acct_output_octets , acct_session_id ,  acct_terminate_cause ,  acct_input_octets ,  nas_ip_address  ,  acct_stop_time      ,  mdn            ,  acct_start_time     ";
		System.out.println(test.trim());
		
		String[] te =test.split("\\s*,\\s*");
		//String[] te =test.trim().split(",");
		for(String temp : te)
		System.out.println(temp);

	}

}
