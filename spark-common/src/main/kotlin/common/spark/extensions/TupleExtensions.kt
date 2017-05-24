package common.spark.extensions

import scala.Tuple1
import scala.Tuple2

fun <K> K.tuple() = Tuple1(this)

infix fun <K, V> K.tuple(value: V) = Tuple2(this, value)
