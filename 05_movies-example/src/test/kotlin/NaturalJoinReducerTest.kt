import common.hadoop.extensions.toIntWritable
import common.hadoop.extensions.toText
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Reducer.Context
import org.junit.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify

class NaturalJoinReducerTest {

	@Suppress("UNCHECKED_CAST")
	@Test
	fun reduceJoin() {
		val reducer = NaturalJoinReducer()
		val context = mock(Context::class.java)

		reducer.reduce("10".toText(), listOf(
				"N:Guardians of the Galaxy Vol.2".toText(), "I:136,1,2,3".toText()
		), context as Reducer<Text, Text, NullWritable, Text>.Context)
		reducer.reduce("11".toText(), listOf("N:The Circle".toText()), context)
		reducer.reduce("12".toText(), listOf(
				"N:Split".toText(), "I:117,4,5".toText()
		), context)

		verify(context).write(NullWritable.get(), "Guardians of the Galaxy Vol.2,136,1,2,3".toText())
		verify(context).write(NullWritable.get(), "Split,117,4,5".toText())
	}
}