import common.hadoop.extensions.toText
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.junit.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify

@Suppress("UNCHECKED_CAST")
class FriendsMapperTest {

	@Test
	fun testMap() {
		val mapper = FriendsMapper()
		val context = mock(Mapper.Context::class.java)

		mapper.map(LongWritable(10), "user1,user2".toText(),
				context as Mapper<LongWritable, Text, Text, Text>.Context)
		mapper.map(LongWritable(10), "user2,user3".toText(), context)
		mapper.cleanup(context)

		verify(context).write("user1".toText(), "user2".toText())
		verify(context).write("user2".toText(), "user1,user3".toText())
		verify(context).write("user3".toText(), "user2".toText())
	}
}