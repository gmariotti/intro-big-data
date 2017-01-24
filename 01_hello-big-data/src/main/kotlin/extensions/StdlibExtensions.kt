package extensions

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

/**
 * Extension method for conversion from String to hadoop.io.Text*
 */
fun String.toText(): Text = Text(this)

/**
 * Extension method for conversion from Int to hadoop.io.IntWritable
 */
fun Int.toIntWritable(): IntWritable = IntWritable(this)