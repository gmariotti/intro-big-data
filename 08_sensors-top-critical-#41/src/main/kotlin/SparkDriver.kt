import common.spark.extensions.tuple
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

fun main(args: Array<String>) {
	if (args.size != 3) {
		error("Expected arguments: <input> <output> <top>")
	}

	val inputPath = args[0]
	val outputPath = args[1]
	val top = args[2].toInt()

	val conf = SparkConf()
			.setMaster("local")
			.setAppName("Sensors Top Critical - Exercise 41")

	JavaSparkContext(conf).use {
		val criticalDays = it.textFile(inputPath)
				.filter { line -> line.split(",")[2].toFloat() >= 50 }
				.mapToPair { line ->
					val values = line.split(",")
					values[0] tuple 1
				}.reduceByKey { val1, val2 -> val1 + val2 }
				.mapToPair { it._2() tuple it._1() }
				.sortByKey(false)
				.take(top)

		it.parallelize(criticalDays).saveAsTextFile(outputPath)
	}
}