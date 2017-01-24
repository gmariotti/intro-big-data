package extensions

import org.apache.hadoop.io.Text
import java.util.regex.Pattern

/**
 * Extension method for Text class simulating the split(..) method of String class.
 *
 * @param limit Non-negative value specifying the maximum number of substrings to return.
 * Zero by default means no limit is set.
 */
fun Text.split(regex: Pattern, limit: Int = 0): List<String> {
	return this.toString().split(regex, limit)
}