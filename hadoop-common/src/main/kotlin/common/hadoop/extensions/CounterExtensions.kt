package common.hadoop.extensions

import org.apache.hadoop.mapreduce.Counter

operator fun Counter.inc(): Counter {
	this.increment(1)
	return this
}

operator fun Counter.plusAssign(value: Long) {
	this.increment(value)
}