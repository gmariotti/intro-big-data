import common.hadoop.extensions.toIntWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

class NGramReducer: Reducer<Text, IntWritable, Text, IntWritable>() {

	override fun reduce(key: Text, values: Iterable<IntWritable>, context: Context) {
		context.write(key, values.sumBy(IntWritable::get).toIntWritable())
	}
}