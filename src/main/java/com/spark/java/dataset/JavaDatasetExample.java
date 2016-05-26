package com.spark.java.dataset;


import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.scheduler.InputFormatInfo;
import org.apache.spark.scheduler.SplitInfo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;

import com.spark.java.common.Functions;
import com.spark.java.common.JavaData;
import com.spark.java.common.JavaPerson;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author sumit.kumar
 *
 */
public class JavaDatasetExample implements Serializable{

	private static final long serialVersionUID = 1L;
	transient static Logger rootLogger = LogManager.getLogger("myLogger");
	
    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf()
                .setAppName("Example")
                .setMaster("local[*]");
        //preferred node  location data
      //  Map<String, Set<SplitInfo>>  splitInfo= InputFormatInfo.computePreferredLocations(arg0);
      //  JavaSparkContext sc = new JavaSparkContext(sparkConf,splitInfo); 
       
        
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(sc);

        List<JavaPerson> data = JavaData.sampleData();

        //NOTE: this does not actually work ... throws error "no encoder found for example_java.common.JavaPerson"
        Dataset<JavaPerson> dataset = sqlContext.createDataset(data, Encoders.bean(JavaPerson.class));

        Dataset<JavaPerson> below21 = dataset.filter((FilterFunction<JavaPerson>) person -> (person.getAge() < 21));

        below21.foreach((ForeachFunction<JavaPerson>) person -> System.out.println(person));
        
        //Custom Map function 
        Dataset<String> strings = dataset.map(Functions.BuildString, Encoders.STRING());
        strings.foreach(new BuildForEach());

    }
}