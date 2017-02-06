package org.minyodev.examples

/**
  * Created by aesguerra on 2/6/17.
  */
object UnsignedLong extends App {

  val x = Long.MaxValue // declare my "unsigned" long
  // do some math with it ...
  val y = x + 10

  // Convert it to the equivalent Scala BigInt
  def asUnsigned(unsignedLong: Long) =
  (BigInt(unsignedLong >>> 1) << 1) + (unsignedLong & 1)

  x
  // Long = 9223372036854775807
  asUnsigned(y)
  // res1: scala.math.BigInt = 9223372036854775817

}
