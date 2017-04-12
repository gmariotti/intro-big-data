import org.apache.hadoop.io.Writable
import java.io.DataInput
import java.io.DataOutput

class SensorData(var sensorID: String, var date: String, var temp: Float): Writable {
	override fun readFields(dataIn: DataInput) {
		sensorID = dataIn.readUTF()
		date = dataIn.readUTF()
		temp = dataIn.readFloat()
	}

	override fun write(dataOut: DataOutput) {
		dataOut.writeUTF(sensorID)
		dataOut.writeUTF(date)
		dataOut.writeFloat(temp)
	}
}