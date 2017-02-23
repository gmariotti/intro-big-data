import NGramDriver.NGramCounters.REMOVED_BY_KEYWORD
import common.hadoop.extensions.plusAssign
import common.hadoop.extensions.toIntWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

class NGramReducer : Reducer<Text, IntWritable, Text, IntWritable>() {

	override fun reduce(key: Text, values: Iterable<IntWritable>, context: Context) {
		val keyword = context.configuration
				.get("keyword")

		// if key starts with the keyword, than the counter is increase
		// based on the number of times it appears in the text, otherwise
		// writes the key and the sum of occurrences
		if (key.toString().startsWith(keyword)) {
			context.getCounter(REMOVED_BY_KEYWORD) += values.sumBy(IntWritable::get).toLong()
		} else {
			context.write(key, values.sumBy(IntWritable::get).toIntWritable())
		}
	}
}