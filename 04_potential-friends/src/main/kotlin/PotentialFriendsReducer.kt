import common.hadoop.extensions.split
import common.hadoop.extensions.toText
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer


class PotentialFriendsReducer : Reducer<Text, Text, Text, Text>() {
	private val friends = mutableMapOf<String, List<String>>()

	public override fun reduce(key: Text, values: Iterable<Text>, context: Context) {
		this.friends[key.toString()] = values.flatMap { it.split(",") }.distinct()
	}

	public override fun cleanup(context: Context) {
		friends.forEach { entry ->
			val potentialFriends = friends.filter { it != entry }
					.asSequence()
					.filter { !entry.value.contains(it.key) }
					.filter { it.value.any { user -> entry.value.contains(user) } }
					.map { it.key }
					.toList()
			context.write(entry.key.toText(), potentialFriends.joinToString(separator = ",").toText())
		}
	}
}