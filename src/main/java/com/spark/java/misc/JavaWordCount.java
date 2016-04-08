package com.spark.java.misc;

import java.util.Arrays;

import java.util.regex.Pattern;

import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public final class JavaWordCount {

  private static final Pattern SPACE = Pattern.compile(" ");

  public static final void main(final String... args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: JavaWordCount <file>");
      System.exit(1);
    }

    final SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");

    /* Create new spark context */
    final JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    /* Create new Java RDD */
    final JavaRDD<String> lines = ctx.textFile(args[0], 1);

    @SuppressWarnings("serial")
    final JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

      @Override
      public final Iterable<String> call(final String s) throws Exception {
        return Arrays.asList(SPACE.split(s));
      }
    });

    @SuppressWarnings("serial")
    final JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {

      @Override
      public Tuple2<String, Integer> call(final String s) {
        return new Tuple2<String, Integer>(s, 1);
      }
    });

    @SuppressWarnings("serial")
    final JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {

      @Override
      public Integer call(final Integer i1, final Integer i2) {
        return i1 + i2;
      }
    });

    /* Save this output at hdfs */
    counts.saveAsHadoopFile("/user/output", String.class, String.class, TextOutputFormat.class);

    if (ctx != null) {
      ctx.stop();
    }

    /* Close spark context */
    ctx.close();
  }
}
