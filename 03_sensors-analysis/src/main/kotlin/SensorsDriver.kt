import common.hadoop.extensions.addMultipleInputPath
import common.hadoop.extensions.addMultipleNamedOutput
import common.hadoop.extensions.addOutputPath
import common.hadoop.extensions.setJarByClass
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.util.Tool

val HIGH_TEMP = "hightemp"
val LOW_TEMP = "lowtemp"
val THRESHOLD = "threshold"

class SensorsDriver : Configured(), Tool {
	override fun run(args: Array<out String>): Int {
		if (args.size != 4) {
			error("Expected <input_path1> <input_path2> <output_path> <temp_threshold>, but received " +
					"${args.size} elements.")
		}

		val firstInput = Path(args[0])
		val secInput = Path(args[1])
		val outputPath = Path(args[2])
		val highTemp = args[3]

		val config = this.conf.apply {
			set(THRESHOLD, highTemp)
		}

		with(Job.getInstance(config)) {
			jobName = "Big Data - Sensors Analysis"
			setJarByClass<SensorsDriver>()

			addMultipleInputPath<KeyValueTextInputFormat, SensorsMapperType1>(firstInput)
			addMultipleInputPath<TextInputFormat, SensorsMapperType2>(secInput)

			addOutputPath(outputPath)
			addMultipleNamedOutput<TextOutputFormat<Text, SensorData>, Text, SensorData>(HIGH_TEMP)
			addMultipleNamedOutput<TextOutputFormat<Text, SensorData>, Text, SensorData>(LOW_TEMP)

			numReduceTasks = 0

			return if (waitForCompletion(true)) 0 else 1
		}
	}
}
