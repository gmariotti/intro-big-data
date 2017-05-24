package common.hadoop.extensions

import org.apache.hadoop.io.Text
import java.util.regex.Pattern
import kotlin.text.split

fun Text.toIntWritable() = this.toString().toInt().toIntWritable()

/**
 * Extension method for Text class simulating the split(..) method of String class.
 *
 * @param limit Non-negative value specifying the maximum number of substrings to return.
 * Zero by default means no limit is set.
 */
fun Text.split(regex: Pattern, limit: Int = 0): List<String> =
		this.toString().split(regex, limit = limit)

fun Text.split(vararg delimiters: String, ignoreCase: Boolean = false, limit: Int = 0): List<String> =
		this.toString().split(*delimiters, ignoreCase = ignoreCase, limit = limit)