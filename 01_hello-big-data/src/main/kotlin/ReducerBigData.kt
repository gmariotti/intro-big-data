import common.hadoop.extensions.toIntWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

/**
 * Represents the Reducer class used for this example.
 * The generic types correspond to <InputKey, InputValue, OutputKey, OutputValue>
 */
class ReducerBigData : Reducer<Text, IntWritable, Text, IntWritable>() {

	override fun reduce(key: Text, values: Iterable<IntWritable>, context: Context) {
		// For each word returns the number of times it appeared in the input text.
		context.write(key, values.sumBy(IntWritable::get).toIntWritable())
	}
}