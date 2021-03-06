import common.hadoop.extensions.split
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs

/*
Input format is of type:
<s#id>\t<date>,<temp>
 */
class SensorsMapperType1: Mapper<Text, Text, Text, SensorData>() {
	lateinit var multipleOutputs: MultipleOutputs<Text, SensorData>

	override fun setup(context: Context) {
		multipleOutputs = MultipleOutputs(context)
	}

	override fun map(key: Text, value: Text, context: Context) {
		val values = value.split(",")
		val date = values[0]
		val temp = values[1].toFloat()
		val sensorData = SensorData(date, temp)

		val threshold = context.configuration.get(THRESHOLD).toFloat()
		if (temp >= threshold) {
			multipleOutputs.write(HIGH_TEMP, key, sensorData)
		} else {
			multipleOutputs.write(LOW_TEMP, key, sensorData)
		}
	}

	override fun cleanup(context: Context?) {
		multipleOutputs.close()
	}
}