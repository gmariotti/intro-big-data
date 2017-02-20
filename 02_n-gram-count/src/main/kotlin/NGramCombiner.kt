import NGramDriver.NGramCounters
import common.hadoop.extensions.toIntWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

/**
 * Combiner for the problem. It should run at least once.
 * Input and Output are the same of the Reducer for the previous reason.
 */
class NGramCombiner : Reducer<Text, IntWritable, Text, IntWritable>() {

	override fun reduce(key: Text, values: Iterable<IntWritable>, context: Context) {
		val keyword = context.configuration
				.get("keyword")

		// if key starts with the keyword, than the counter is increase
		// based on the number of times it appears in the text, otherwise
		// writes the key and the sum of occurrences
		if (key.toString().startsWith(keyword)) {
			context.getCounter(NGramCounters.REMOVED_BY_KEYWORD)
					.increment(values.sumBy(IntWritable::get).toLong())
		} else {
			context.write(key, values.sumBy(IntWritable::get).toIntWritable())
		}
	}
}