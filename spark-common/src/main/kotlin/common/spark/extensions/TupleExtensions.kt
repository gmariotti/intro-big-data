package common.spark.extensions

import scala.Tuple2

infix fun <K, V> K.to(value: V) = Tuple2(this, value)