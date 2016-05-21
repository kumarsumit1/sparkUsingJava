package com.spark.java.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class DataProcessor {


	public static void main(String[] args) {
		
		//String filePath="E://tested//sample.xml";
		String filePath="input/people.json";
		//String filePath="input/sales";
		//String filePath="E://tested//test.tilde";
		String format="json";
		String formatClass=null;
		
		System.setProperty("hadoop.home.dir", "C:\\Users\\sumit.kumar\\Docker\\winutil\\");
		SparkConf conf = new SparkConf().setMaster("local").setAppName("appName");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(javaSparkContext);
		DataFrame asset = null;
		if(format.trim().toLowerCase().equalsIgnoreCase("xml")){
			formatClass="com.databricks.spark.xml";
			asset = sqlContext.read().format(formatClass).option("rowTag", "ASSET").load(filePath);
		}else if(format.trim().toLowerCase().equalsIgnoreCase("json")){
			asset = sqlContext.read().json(filePath);
		}else if(format.trim().toLowerCase().equalsIgnoreCase("csv")){
			formatClass="com.databricks.spark.csv";
			asset = sqlContext.read().format(formatClass) .option("header", "true") // Use first line of all files as header
				    .option("inferSchema", "true") // Automatically infer data types
				    .load(filePath);
		}else if(format.trim().toLowerCase().equalsIgnoreCase("customseperator")){
			formatClass="com.databricks.spark.csv";
			asset = sqlContext.read().format(formatClass) .option("header", "true") // Use first line of all files as header
				    .option("inferSchema", "true") // Automatically infer data types
				    .option("delimiter", "~")
				    .load(filePath);
		}
		

      asset.printSchema();
  	asset.registerTempTable("asset_table");
  	sqlContext.cacheTable("asset_table");

	}
}
