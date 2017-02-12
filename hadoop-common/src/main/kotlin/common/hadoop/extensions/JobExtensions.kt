package common.hadoop.extensions

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.*
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

/**
 * Extension method for applying FileInputFormat.addInputPath(..) and
 * FileOutputFormat.setOutputPath(..).
 *
 * @param inputPath The path containing the input file or folder to use.
 * @param outputPath The path containing the output folder where to put the results.
 */
fun Job.addPaths(inputPath: Path, outputPath: Path) {
	FileInputFormat.addInputPath(this, inputPath)
	FileOutputFormat.setOutputPath(this, outputPath)
}

/**
 * Extension method to call Job.setJarByClass(..) using a reified type instead of an input parameter.
 *
 * @param T The class type for the jar class.
 */
inline fun <reified T : Any> Job.setJarByClass() {
	this.setJarByClass(T::class.java)
}

/**
 * Extension method to call Job.setInputFormatClass(..) using a reified type instead of an input
 * parameter.
 *
 * @param T The class type for the setInputFormatClass(..)
 */
inline fun <reified T : InputFormat<*, *>> Job.setInputFormatClass() = {
	this.inputFormatClass = T::class.java
}

/**
 * Extension method to call Job.setOutputFormatClass(..) using a reified type instead of an input parameter.
 *
 * @param T The class type for the setOutputFormatClass(..)
 */
inline fun <reified T : OutputFormat<*, *>> Job.setOutputFormatClass() {
	this.outputFormatClass = T::class.java
}

/**
 * Extension method to call Job.setMapperClass(..) using a reified type instead of an input parameter.
 *
 * @param T The class type for the setMapperClass(..)
 */
inline fun <reified T : Mapper<*, *, *, *>> Job.setMapperClass() {
	this.mapperClass = T::class.java
}

/**
 * Extension method to call Job.setReducerClass(..) using a reified type instead of an input parameter
 * and to set the number of reducer tasks with Job.setNumReduceTasks.
 *
 * @param T The class type for the setReducerClass(..)
 * @param numReducers The number of reducers to use.
 */
inline fun <reified T : Reducer<*, *, *, *>> Job.setReducerClass(numReducers: Int) {
	this.reducerClass = T::class.java
	this.numReduceTasks = numReducers
}

/**
 * Extension method to call Job.setMapOutputKeyClass(..) using reified type KeyClass and to call
 * Job.setMapOutputValueClass(..) using reified type ValueClass.
 *
 * @param KeyClass The class type for the setMapOutputKeyClass(..)
 * @param ValueClass The class type for the setMapOutputValueClass(..)
 */
inline fun <reified KeyClass : Any, reified ValueClass : Any> Job.mapOutput() {
	this.mapOutputKeyClass = KeyClass::class.java
	this.mapOutputValueClass = ValueClass::class.java
}

/**
 * Extension method to call Job.setOutputKeyClass(..) using reified type KeyClass and to call
 * Job.setOutputValueClass(..) using reified type ValueClass.
 *
 * @param KeyClass The class type for the setOutputKeyClass(..)
 * @param ValueClass The class type for the setOutputValueClass(..)
 */
inline fun <reified KeyClass : Any, reified ValueClass : Any> Job.reducerOutput() {
	this.outputKeyClass = KeyClass::class.java
	this.outputValueClass = ValueClass::class.java
}