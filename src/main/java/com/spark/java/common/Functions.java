package com.spark.java.common;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;

import scala.Tuple2;

public class Functions {

	public static MapFunction<JavaPerson, String> BuildString = new MapFunction<JavaPerson, String>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public String call(JavaPerson person) throws Exception {
			return person.getFirst() + " has " + (100 - person.getAge())
					+ " years to live.";
		}
	};

	public static class ValueComparator<K, V> implements
			Comparator<Tuple2<K, V>>, Serializable {
		private Comparator<V> comparator;

		public ValueComparator(Comparator<V> comparator) {
			this.comparator = comparator;
		}

		@Override
		public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
			return comparator.compare(o1._2(), o2._2());
		}
	}

	public static class LongComparator implements Comparator<Long>,
			Serializable {

		@Override
		public int compare(Long a, Long b) {
			if (a > b)
				return 1;
			if (a.equals(b))
				return 0;
			return -1;
		}
	}

	public static Comparator<Long> LONG_NATURAL_ORDER_COMPARATOR = new LongComparator();

	public static Function<Tuple2<String, Long>, String> GET_TUPLE_FIRST = new Function<Tuple2<String, Long>, String>() {
		/**
	 * 
	 */
		private static final long serialVersionUID = 1L;

		@Override
		public String call(Tuple2<String, Long> tuple) throws Exception {
			return tuple._1();
		}
	};

}
