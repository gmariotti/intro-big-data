import common.hadoop.extensions.split
import common.hadoop.extensions.toText
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs

/*
Input format is of type:
<date>,<temp>,<s#id>
 */
class SensorsMapperType2 : Mapper<LongWritable, Text, Text, SensorData>() {
	lateinit var multipleOutputs: MultipleOutputs<Text, SensorData>

	override fun setup(context: Context) {
		multipleOutputs = MultipleOutputs(context)
	}

	override fun map(key: LongWritable, value: Text, context: Context) {
		val values = value.split(",")
		val sensorID = values[2]
		val date = values[0]
		val temp = values[1].toFloat()
		val sensorData = SensorData(date, temp)

		val threshold = context.configuration.get(THRESHOLD).toFloat()
		if (temp >= threshold) {
			multipleOutputs.write(HIGH_TEMP, sensorID.toText(), sensorData)
		} else {
			multipleOutputs.write(LOW_TEMP, sensorID.toText(), sensorData)
		}
	}

	override fun cleanup(context: Context) {
		multipleOutputs.close()
	}
}