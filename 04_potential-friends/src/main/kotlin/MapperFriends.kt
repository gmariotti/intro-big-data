import common.hadoop.extensions.split
import common.hadoop.extensions.toText
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class MapperFriends : Mapper<LongWritable, Text, Text, Text>() {
	private val friendsDictionary = mutableMapOf<String, List<String>>()

	public override fun map(key: LongWritable, value: Text, context: Context) {
		val friends = value.split(",")
		friends.forEach({ friend ->
			val listOfFriends = friendsDictionary.getOrDefault(friend, mutableListOf())
			val friendsToAdd = friends.filter { friend != it }
					.filter { !listOfFriends.contains(it) }
			friendsDictionary[friend] = listOf(
					*listOfFriends.toTypedArray(), *friendsToAdd.toTypedArray()
			)
		})
	}

	public override fun cleanup(context: Context) {
		friendsDictionary.entries.forEach({
			(friend, listOfFriends) ->
			context.write(friend.toText(), listOfFriends.joinToString(separator = ",").toText())
		})
	}
}