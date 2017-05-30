package common.spark.extensions

import org.apache.spark.api.java.JavaPairRDD

fun <K, V> JavaPairRDD<K, V>.collectAsImmutableMap() = this.collectAsMap().toMap()