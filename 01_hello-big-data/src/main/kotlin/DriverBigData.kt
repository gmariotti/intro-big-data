import common.hadoop.extensions.*
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.util.Tool

/**
 * Hello World for Big Data with Hadoop
 */

class DriverBigData : Configured(), Tool {
	override fun run(args: Array<out String>): Int {
		// parsing of the input parameters
		if (args.size != 3) {
			error("Expected <#reducers> <input_path> <output_path>, but received ${args.size} elements.")
		}
		val numReducers = args[0].toInt()
		val inputPath = Path(args[1])
		val outputPath = Path(args[2])

		// Defines a new job
		with(Job.getInstance(this.conf)) {
			jobName = "Big Data - Hello World"

			// Sets the path for input file/folder and the output folder
			addPaths(inputPath, outputPath)

			// Specifies the class of the Driver for this job
			setJarByClass<DriverBigData>()

			// Specifies the job's format for input and output
			setInputFormatClass<TextInputFormat>()
			setOutputFormatClass<TextOutputFormat<Text, IntWritable>>()

			// Specifies the mapper class and its key:value output
			setMapperClass<MapperBigData>()
			mapOutput<Text, IntWritable>()

			// Specifies the reducer class and its key:value output
			setReducerClass<ReducerBigData>(numReducers)
			reducerOutput<Text, IntWritable>()

			return if (waitForCompletion(true)) 0 else 1
		}
	}
}