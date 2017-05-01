import common.hadoop.extensions.toText
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.junit.Test
import org.mockito.Mockito

@Suppress("UNCHECKED_CAST")
class PotentialFriendsReducerTest {

	@Test
	fun cleanupTest() {
		val reducer = PotentialFriendsReducer()
		val context = Mockito.mock(Reducer.Context::class.java)

		reducer.reduce("user1".toText(), listOf("user2".toText()),
				context as Reducer<Text, Text, Text, Text>.Context)
		reducer.reduce("user2".toText(), listOf("user1,user3".toText()), context)
		reducer.reduce("user3".toText(), listOf("user2".toText()), context)
		reducer.reduce("user4".toText(), listOf("user2".toText()), context)

		reducer.cleanup(context)
		Mockito.verify(context).write("user1".toText(), "user3,user4".toText())
		Mockito.verify(context).write("user2".toText(), "".toText())
		Mockito.verify(context).write("user3".toText(), "user1,user4".toText())
		Mockito.verify(context).write("user4".toText(), "user1,user3".toText())
	}
}