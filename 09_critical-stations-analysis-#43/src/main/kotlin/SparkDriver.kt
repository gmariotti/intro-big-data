import common.spark.extensions.collectAsImmutableMap
import common.spark.extensions.tuple
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

operator fun <T> Array<T>.get(indices: IntRange) = this.sliceArray(indices)

fun main(args: Array<String>) {

	if (args.size != 6) {
		error("Expected <iStations> <iNeighbors> <threshold> <output1> <output2> <output3>")
	}

	val (inputStations, inputNeighbors) = args[0..1]
	val threshold = args[2].toDouble()
	val (outputPart1, outputPart2, outputPart3) = args[3..5]

	with(SparkConf()) {
		setMaster("local")
		setAppName("Critical Stations Analysis - Exercise 43")

		JavaSparkContext(this).use {
			val cachedInputStations = it.textFile(inputStations).cache()

			// Part 1
			cachedInputStations.mapToPair {
				val values = it.split(",")
				values[0] tuple (1.0 to if (values.last().toDouble() <= threshold) 1.0 else 0.0)
			}.reduceByKey { pair1, pair2 ->
				(pair1.first + pair2.first) to (pair1.second + pair2.second)
			}.mapValues { (total, critical) -> critical / total }
					.filter { it._2 >= 0.8 }
					.mapToPair { it._2 tuple it._1 }
					.sortByKey(false)
					.saveAsTextFile(outputPart1)

			// Part 2
			cachedInputStations.mapToPair {
				val values = it.split(",")
				val hour = values[2].toInt()
				val timeslot = (hour - hour % 4) to (hour + 3 - hour % 4)
				val value1 = (timeslot to values[0])
				value1 tuple (1.0 to if (values.last().toDouble() <= threshold) 1.0 else 0.0)
			}.reduceByKey { pair1, pair2 ->
				(pair1.first + pair2.first) to (pair1.second + pair2.second)
			}.mapToPair {
				val (total, critical) = it._2
				(critical / total) tuple it._1
			}.sortByKey(false)
					.saveAsTextFile(outputPart2)

			// Part 3
			val neighborsMap = it.textFile(inputNeighbors)
					.mapToPair {
						val values = it.split(",")
						values[0] tuple values[1].split(" ")
					}.collectAsImmutableMap()

			val totalValidLines = it.sc().longAccumulator()

			cachedInputStations.filter { it.split(",").last().toInt() == 0 }
					.mapToPair {
						val values = it.split(",")
						"${values[1]}:${values[2]}:${values[3]}" tuple it
					}.groupByKey()
					.flatMap {
						val readings = it._2
						readings.filter { read ->
							val sID = read.split(",").first()
							val neighbors = neighborsMap.getOrDefault(sID, listOf())
							val validReads = readings.filter {
								neighbors.contains(it.split(",").first())
							}
							if (validReads.size == neighbors.size) {
								totalValidLines.add(neighbors.size.toLong())
								true
							} else false
						}.iterator()
					}.saveAsTextFile(outputPart3)

			println("Number of valid lines is ${totalValidLines.count()}")
		}
	}
}
