package com.spark.java.dataset;

import org.apache.spark.api.java.function.MapFunction;

import com.spark.java.common.JavaPerson;

public class BuildString implements MapFunction<JavaPerson, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String call(JavaPerson person) throws Exception {
		return person.getFirst() + " has " + (100 - person.getAge()) + " years to live.";
	}

}
