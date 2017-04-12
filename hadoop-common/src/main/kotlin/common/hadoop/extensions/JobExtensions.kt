package common.hadoop.extensions

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.*
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs

/**
 * Extension method for applying FileInputFormat.addInputPath(..) and
 * FileOutputFormat.setOutputPath(..).
 *
 * @param inputPath The path containing the input file or folder to use.
 * @param outputPath The path containing the output folder where to put the results.
 */
fun Job.addPaths(inputPath: Path, outputPath: Path) {
	this.addInputPath(inputPath)
	this.addOutputPath(outputPath)
}

fun Job.addInputPath(inputPath: Path) {
	FileInputFormat.addInputPath(this, inputPath)
}

fun Job.addOutputPath(outputPath: Path) {
	FileOutputFormat.setOutputPath(this, outputPath)
}

/**
 * TODO
 */
inline fun <reified T : InputFormat<*, *>, reified K : Mapper<*, *, *, *>> Job.addMultipleInputPath(path: Path) {
	MultipleInputs.addInputPath(this, path, T::class.java, K::class.java)
}

/**
 * TODO
 */
inline fun <reified T : OutputFormat<*, *>, reified Key : Any, reified Value : Any> Job.addMultipleNamedOutput(namedOutput: String) {
	MultipleOutputs.addNamedOutput(this, namedOutput, T::class.java, Key::class.java, Value::class.java)
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
 * Extension method for setting the Mapper of a Job with its output Key and Value.
 *
 * @param T The class type of the Mapper.
 * @param KeyClass The class type of the Mapper's Output Key
 * @param ValueClass The class type of the Mapper's Output Value
 */
inline fun <reified T : Mapper<*, *, KeyClass, ValueClass>, reified KeyClass : Any, reified ValueClass : Any> Job.setMapper() {
	this.setMapperClass<T>()
	this.mapOutput<KeyClass, ValueClass>()
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

/**
 * Extension method for setting the Reducer of a Job with its output Key and Value.
 *
 * @param T The class type of the Reducer.
 * @param KeyClass The class type of the Reducer's Output Key
 * @param ValueClass The class type of the Reducer's Output Value
 */
inline fun <reified T : Reducer<*, *, KeyClass, ValueClass>, reified KeyClass : Any, reified ValueClass : Any> Job.setReducer(numReducers: Int) {
	this.setReducerClass<T>(numReducers)
	this.reducerOutput<KeyClass, ValueClass>()
}

/**
 * Extension method to call Job.setCombinerClass(..) using a reified type instead of an input parameter.
 *
 * @param T The class type for the setCombinerClass(..)
 */
inline fun <reified T : Reducer<*, *, *, *>> Job.setCombinerClass() {
	this.combinerClass = T::class.java
}
