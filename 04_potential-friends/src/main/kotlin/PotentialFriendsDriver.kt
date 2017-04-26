import common.hadoop.extensions.*
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.util.Tool

class PotentialFriendsDriver : Configured(), Tool {

	override fun run(args: Array<out String>): Int {
		if (args.size != 3) {
			error("Expected <input_path> <temp_dir> <output_path>, but received ${args.size} elements.")
		}

		val input = Path(args[0])
		val tempDir = Path(args[1])
		val output = Path(args[2])

		with(Job.getInstance(this.conf)) {
			jobName = "Big Data - Potential Friends"
			addPaths(input, tempDir)
			setJarByClass<PotentialFriendsDriver>()
			setInputFormatClass<TextInputFormat>()
			setOutputFormatClass<TextOutputFormat<Text, Text>>()

			setMapperClass<MapperFriends>()
			setReducerClass<ReducerFriends>(1)
			reducerOutput<Text, Text>()

			return if (waitForCompletion(true)) 0 else 1
		}
	}
}