import common.hadoop.extensions.*
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.util.Tool

val HIGH_DURATION = "highduration"
val LOW_DURATION = "lowduration"
val THRESHOLD = "threshold"

class MoviesDriver : Configured(), Tool {

	override fun run(args: Array<out String>): Int {

		if (args.size != 5) {
			error("Expected <movies> <info> <temp-path> <output-path> <categories>")
		}

		val moviesPath = Path(args[0])
		val infoPath = Path(args[1])
		val tempDir = Path(args[2])
		val outputDir = Path(args[3])
		val cachedFile = Path(args[4])

		with(Job.getInstance(Configuration())) {
			jobName = "Movies Example"
			setJarByClass<MoviesDriver>()

			addMultipleInputPath<KeyValueTextInputFormat, MovieMapper>(moviesPath)
			addMultipleInputPath<KeyValueTextInputFormat, InfoMapper>(infoPath)
			mapOutput<Text, Text>()

			addOutputPath(tempDir)
			setReducer<NaturalJoinReducer, NullWritable, Text>(1)
			setOutputFormatClass<TextOutputFormat<NullWritable, Text>>()

			if (this.waitForCompletion(true)) {
				val config = Configuration()

				// threshold of 2 hours
				config.set(THRESHOLD, "120")

				with(Job.getInstance(config)) {
					jobName = "Movies Example - Type of Movie"
					setJarByClass<MoviesDriver>()

					numReduceTasks = 0
					addInputPath(tempDir)
					setInputFormatClass<TextInputFormat>()
					setMapper<FilterMapper, NullWritable, Text>()

					addOutputPath(outputDir)
					addMultipleNamedOutput<TextOutputFormat<NullWritable, Text>, NullWritable, Text>(HIGH_DURATION)
					addMultipleNamedOutput<TextOutputFormat<NullWritable, Text>, NullWritable, Text>(LOW_DURATION)
					addCacheFile(cachedFile.toUri())

					return if (this.waitForCompletion(true)) 1 else 0
				}
			} else {
				return 0
			}
		}
	}
}