import common.hadoop.extensions.addMultipleInputPath
import common.hadoop.extensions.addMultipleOutputPath
import common.hadoop.extensions.setJarByClass
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.util.Tool
import javax.xml.soap.Text

class SensorsDriver : Configured(), Tool {
	override fun run(args: Array<out String>): Int {
		if (args.size != 3) {
			error("Expected <input_path1> <input_path2> <temp_threshold>, but received " +
					"${args.size} elements.")
		}

		val firstInput = Path(args[0])
		val secInput = Path(args[1])
		val highTemp = args[2].toInt()

		val config = this.conf.apply {
			set("highTemp", highTemp.toString())
		}

		with(Job.getInstance(config)) {
			jobName = "Big Data - Sensors Analysis"
			setJarByClass<SensorsDriver>()

			addMultipleInputPath<KeyValueTextInputFormat, SensorsMapperType1>(firstInput)
			addMultipleInputPath<KeyValueTextInputFormat, SensorsMapperType2>(secInput)

			addMultipleOutputPath<TextOutputFormat, Text, SensorData>("high-temp")
			addMultipleOutputPath<TextOutputFormat, Text, SensorData>("low-temp")
			numReduceTasks = 0

			return if (waitForCompletion(true)) 0 else 1
		}
	}
}
