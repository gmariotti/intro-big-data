import org.apache.hadoop.io.Writable
import java.io.DataInput
import java.io.DataOutput

data class SensorData(var date: String, var temp: Float) : Writable {
	override fun readFields(dataIn: DataInput) {
		date = dataIn.readUTF()
		temp = dataIn.readFloat()
	}

	override fun write(dataOut: DataOutput) {
		dataOut.writeUTF(date)
		dataOut.writeFloat(temp)
	}
}