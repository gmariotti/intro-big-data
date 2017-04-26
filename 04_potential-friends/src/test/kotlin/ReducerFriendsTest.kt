import common.hadoop.extensions.toText
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.junit.Test
import org.mockito.Mockito

@Suppress("UNCHECKED_CAST")
class ReducerFriendsTest {

	@Test
	fun reduceTest() {
		val reducer = ReducerFriends()
		val context = Mockito.mock(Reducer.Context::class.java)

		reducer.reduce("user1".toText(), listOf("user2,user3".toText(), "user3,user4".toText()),
				context as Reducer<Text, Text, Text, Text>.Context)
		reducer.reduce("user2".toText(), listOf("user1".toText(), "user3,user4".toText()), context)

		Mockito.verify(context).write("user1".toText(), "user2,user3,user4".toText())
		Mockito.verify(context).write("user2".toText(), "user1,user3,user4".toText())
	}
}