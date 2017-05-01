import common.spark.extensions.to
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext


fun main(args: Array<String>) {
	with(SparkConf()) {
		setMaster("local")
		setAppName("Word Count Spark")

		JavaSparkContext(this).use {
			val counts = it.textFile(args[0])
					.flatMap { it.split(" ").map { it.toLowerCase() }.iterator() }
					.mapToPair { it to 1 }
					.reduceByKey { a, b -> a + b }

			counts.saveAsTextFile(args[1])
		}
	}
}