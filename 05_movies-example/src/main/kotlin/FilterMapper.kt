import common.hadoop.extensions.split
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.util.stream.Collectors.toConcurrentMap

class FilterMapper : Mapper<LongWritable, Text, NullWritable, Text>() {
	lateinit var multipleOutputs: MultipleOutputs<NullWritable, Text>
	var threshold: Int = 0
	var categoriesByID = hashMapOf<Int, String>()

	override fun setup(context: Context) {
		multipleOutputs = MultipleOutputs(context)

		val cachedFiles = context.cacheFiles
		val file = BufferedReader(FileReader(File(cachedFiles[0])))
		categoriesByID = file.use {
			it.lines()
					.map { it.split("\t") }
					.map { it[0].toInt() to it[1] }
					.collect(toConcurrentMap(
							{ pair: Pair<Int, String> -> pair.first }
					) { (_, second) -> second })
					.toMap(categoriesByID)
		}

		threshold = context.configuration.getInt(THRESHOLD, 0)
	}

	override fun map(key: LongWritable, value: Text, context: Context) {
		val values = value.split(",")
		val film = values[0]
		val duration = values[1].toInt()
		val categories = values.subList(2, values.size)
				.map { categoriesByID[it.toInt()] }
				.toList()
				.joinToString(",")

		if (duration <= threshold) {
			multipleOutputs.write(LOW_DURATION, NullWritable.get(), "$film,$categories")
		} else {
			multipleOutputs.write(HIGH_DURATION, NullWritable.get(), "$film,$categories")
		}
	}

	override fun cleanup(context: Context?) {
		multipleOutputs.close()
	}
}