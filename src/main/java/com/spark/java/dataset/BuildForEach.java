package com.spark.java.dataset;

import org.apache.spark.api.java.function.ForeachFunction;

public class BuildForEach implements ForeachFunction<String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void call(String arg) throws Exception {
		System.out.println(arg);
		
	}

}
