import common.hadoop.extensions.split
import common.hadoop.extensions.toText
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

class NaturalJoinReducer : Reducer<Text, Text, NullWritable, Text>() {

	public override fun reduce(key: Text, values: Iterable<Text>, context: Context) {
		with(values.toList().sortedDescending()) {
			if (this.size == 2) {
				val result = this.map { it.split(":") }
						.filter { it.contains("N") || it.contains("I") }
						.map { it[1] }
						.joinToString(separator = ",")
				context.write(NullWritable.get(), result.toText())
			}
		}
	}
}