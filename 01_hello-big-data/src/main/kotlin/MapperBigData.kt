import extensions.split
import extensions.toIntWritable
import extensions.toText
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import java.util.regex.Pattern

/**
 * Represents the Mapper class used for this example.
 * The generic types correspond to <InputKey, InputValue, OutputKey, OutputValue>
 */
class MapperBigData : Mapper<LongWritable, Text, Text, IntWritable>() {

	override fun map(key: LongWritable, value: Text, context: Context) {
		// Splits each sentence in a list of words.
//		val words = value.toString().split(Pattern.compile("\\s+"))
		val words = value.split(Pattern.compile("\\W+"))
		words.map(String::toLowerCase)
				.forEach { context.write(it.toText(), 1.toIntWritable()) }
	}
}