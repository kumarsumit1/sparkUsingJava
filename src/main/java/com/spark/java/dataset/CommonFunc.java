package com.spark.java.dataset;

import org.apache.spark.api.java.function.MapFunction;

import com.spark.java.common.JavaPerson;

public class CommonFunc {

	
  public static class BuildMapString implements MapFunction<JavaPerson, String> {
	    public String call(JavaPerson u) throws Exception {
	        return u.getFirst() + " is " + (100 - u.getAge()) + " years to live.";
	    }
	}
}
