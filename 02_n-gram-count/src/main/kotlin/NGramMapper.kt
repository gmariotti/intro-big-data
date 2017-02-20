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

		// FIXME - there should be a better solution
		(0..words.size).forEach { i ->
			val word = words[i]
			if (i + 1 == words.size) return
			val remaining = words.subList(i + 1, words.size)
			val builder = StringBuilder()
					.append(word)
			(0..remaining.size)
					.takeWhile { it != numWords }
					.forEach { builder.append(" ").append(remaining[it]) }
			context.write(builder.toString().toText(), 1.toIntWritable())
		}
	}
}
