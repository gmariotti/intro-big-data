import common.hadoop.extensions.split
import common.hadoop.extensions.toText
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer


class ReducerFriends : Reducer<Text, Text, Text, Text>() {

	public override fun reduce(key: Text, values: Iterable<Text>, context: Context) {
		val friends = values.flatMap { it.split(",") }.distinct()
		context.write(key, friends.joinToString(separator = ",").toText())
	}
}