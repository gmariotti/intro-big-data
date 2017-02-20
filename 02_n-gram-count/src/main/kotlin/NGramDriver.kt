import common.hadoop.extensions.*
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.util.Tool

class NGramDriver : Configured(), Tool {

	enum class NGramCounters {
		REMOVED_BY_KEYWORD
	}

	override fun run(args: Array<out String>): Int {
		if (args.size != 5) {
			error("Expected <#reducers> <input_path> <output_path> <n> <keyword>, but received" +
					"${args.size} elements.")
		}
		val numReducers = args[0].toInt()
		val inputPath = Path(args[1])
		val outputPath = Path(args[2])
		val n = args[3].toInt()
		val keyword = args[4]

		val config = this.conf
		// set properties for the configuration
		config.set("numWords", n.toString())
		config.set("keyword", keyword)
		with(Job.getInstance(config)) {
			jobName = "Big Data - N-Gram Count"

			addPaths(inputPath, outputPath)
			setJarByClass<NGramDriver>()

			setInputFormatClass<TextInputFormat>()
			setOutputFormatClass<TextOutputFormat<Text, IntWritable>>()

			setMapper<NGramMapper, Text, IntWritable>()

			// Combiner input is the same of the Reducer while the output is the same
			// of the Mapper.
			setCombinerClass<NGramCombiner>()
			setReducer<NGramReducer, Text, IntWritable>(numReducers)

			return if (waitForCompletion(true)) 0 else 1
		}
	}
}