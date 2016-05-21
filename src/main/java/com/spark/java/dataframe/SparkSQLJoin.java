package com.spark.java.dataframe;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class SparkSQLJoin {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);
	    Logger.getLogger("akka").setLevel(Level.OFF);
	    
		boolean runLocal=true;
		SparkConf sparkConfig=new SparkConf();
		 if (runLocal) {
			  sparkConfig.set("spark.broadcast.compress", "false");
		      sparkConfig.set("spark.shuffle.compress", "false");
		      sparkConfig.set("spark.shuffle.spill.compress", "false").setMaster("local").setAppName("SparkSQLJoin");
		    //  sc = new SparkContext("local", "TableStatsSinglePathMain", sparkConfig);
		    //  conf = new SparkConf().setMaster("local").setAppName("SparkSQLJoin");
		    } else {
		    	sparkConfig.setAppName("SparkSQLJoin");
		      
		    }
		
		JavaSparkContext sc = new JavaSparkContext(sparkConfig);
		
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
		JavaRDD<String> aaaRecords = sc.textFile("input/sales");
		List<String> aaaSchema=aaaRecords.top(1);
		
		List<StructField> aaaFields = new ArrayList<StructField>();
		
		for(String temp:aaaSchema){
			String[] schemaString =temp.trim().split("\\s*,\\s*");
			for(String str:schemaString){
				aaaFields.add(DataTypes.createStructField(str.trim(), DataTypes.StringType, true));
			}			
		}
		
		StructType schema = DataTypes.createStructType(aaaFields);
		
		
		JavaRDD<Row> rowRDD = aaaRecords.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(String v1) throws Exception {
				// TODO Auto-generated method stub
				return !(v1.toLowerCase().contains("customerId"));
			}
		}).map(new Function<String, Row>() {
		    public Row call(String record) throws Exception {
		      String[] fields = record.trim().split("\\s*,\\s*");
		      RowFactory rf=new RowFactory();
     	      return rf.create(fields);
		    }
		  });


		DataFrame babyDataFrame = sqlContext.createDataFrame(rowRDD, schema);


		babyDataFrame.registerTempTable("sales");
		
		JavaRDD<String> deviceRecords = sc.textFile("input/customers");
		List<String> deviceSchema=deviceRecords.top(1);
		
		List<StructField> deviceFields = new ArrayList<StructField>();
		
		for(String temp:deviceSchema){
			String[] schemaString =temp.trim().split("\\s*,\\s*");
			for(String str:schemaString){
				deviceFields.add(DataTypes.createStructField(str.trim(), DataTypes.StringType, true));
			}			
		}
		
		StructType deviceSchemaStruct = DataTypes.createStructType(deviceFields);
		

		JavaRDD<Row> deviceRDD = deviceRecords.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Boolean call(String v1) throws Exception {
				// TODO Auto-generated method stub
				return !(v1.toLowerCase().contains("custId"));
			}
		}).map(new Function<String, Row>() {
		    public Row call(String record) throws Exception {
		      String[] fields = record.trim().split("\\s*,\\s*");
		      RowFactory rf=new RowFactory();
     	      return rf.create(fields);
		    }
		  });


		DataFrame deviceDataFrame = sqlContext.createDataFrame(deviceRDD, deviceSchemaStruct);


		deviceDataFrame.registerTempTable("customers");

		 DataFrame billableIP = sqlContext.sql("SELECT * FROM sales left join customers on custId=customerId");
		 
		 billableIP.show();
		    
		// billableIP.write().format("parquet").save("output/billable_ip_data.parquet"); 
		
         billableIP.write().format("parquet").mode("overwrite").save("output/salesData.parquet"); 
         
        // billableIP.write().format("json").mode("overwrite").save("output/billable_ip_data"); 
	}

}
