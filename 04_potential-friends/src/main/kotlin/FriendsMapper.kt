import common.hadoop.extensions.split
import common.hadoop.extensions.toText
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class FriendsMapper : Mapper<LongWritable, Text, Text, Text>() {
	private val friendsDictionary = mutableMapOf<String, List<String>>().withDefault { listOf() }

	public override fun map(key: LongWritable, value: Text, context: Context) {
		// In theory, it can accept also multiple friends on the same line
		val friends = value.split(",")
		friends.forEach { friend ->
			val friendsToAdd = friends.filter { friend != it }
					.filter { !(friendsDictionary[friend]?.contains(it) ?: true) }
					.toList()
			friendsDictionary[friend] = listOf(
					*friendsDictionary.getValue(friend).toTypedArray(), *friendsToAdd.toTypedArray()
			)
		}
	}

	public override fun cleanup(context: Context) {
		friendsDictionary.entries.forEach {
			(friend, listOfFriends) ->
			context.write(friend.toText(), listOfFriends.joinToString(separator = ",").toText())
		}
	}
}