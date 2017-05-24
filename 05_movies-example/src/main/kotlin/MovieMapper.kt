import common.hadoop.extensions.toText
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class MovieMapper : Mapper<Text, Text, Text, Text>() {

	override fun map(key: Text, value: Text, context: Context) {
		context.write(key, ("N:$value").toText())
	}
}