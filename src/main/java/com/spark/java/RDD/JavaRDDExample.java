package com.spark.java.RDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.spark.java.common.JavaData;
import com.spark.java.common.JavaPerson;

public class JavaRDDExample {

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf()
                .setAppName("Example")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<JavaPerson> rdd = sc.parallelize(JavaData.sampleData());

        rdd.filter(p -> p.getAge() < 21)
                .collect()
                .forEach(System.out::println);

    }
}