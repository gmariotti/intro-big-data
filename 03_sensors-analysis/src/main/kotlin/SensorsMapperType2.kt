import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class SensorsMapperType2 : Mapper<Text, Text, Text, SensorData>() {
}