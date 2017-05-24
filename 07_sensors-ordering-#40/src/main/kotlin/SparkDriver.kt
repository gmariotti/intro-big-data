import common.spark.extensions.tuple
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

fun main(args: Array<String>) {
	if (args.size != 2) {
		error("Expected arguments: <input> <output>")
	}

	val inputPath = args[0]
	val outputPath = args[1]

	with(SparkConf()) {
		setMaster("local")
		setAppName("Sensors Ordering - Exercise 40")

		JavaSparkContext(this).use {
			it.textFile(inputPath)
					.filter { line -> line.split(",")[2].toFloat() >= 50 }
					.mapToPair { line ->
						val values = line.split(",")
						values[0] tuple 1
					}.reduceByKey { value1, value2 -> value1 + value2 }
					.mapToPair { tuple -> tuple._2() tuple tuple._1 }
					.sortByKey(false)
					.saveAsTextFile(outputPath)
		}
	}
}