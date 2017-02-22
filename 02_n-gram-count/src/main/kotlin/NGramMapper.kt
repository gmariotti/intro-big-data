import common.hadoop.extensions.split
import common.hadoop.extensions.toIntWritable
import common.hadoop.extensions.toText
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import java.util.regex.Pattern

class NGramMapper : Mapper<LongWritable, Text, Text, IntWritable>() {

	override fun map(key: LongWritable, value: Text, context: Context) {
		val words = value.split(Pattern.compile("\\W+"))
		val numWords = context.configuration
				.get("numWords")
				.toInt()

		(0 until words.size).map { Pair(it, StringBuilder().append(words[it])) }
				.onEach {
					val (index, builder) = it
					((index + 1) until words.size).take(numWords - 1)
							.forEach { builder.append(" ").append(words[it]) }
					Pair(index, builder)
				}.map { it.second.toString() }
				.forEach { context.write(it.toText(), 1.toIntWritable()) }
	}
}
