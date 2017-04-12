import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class SensorsMapperType1: Mapper<Text, Text, Text, SensorData>() {


}