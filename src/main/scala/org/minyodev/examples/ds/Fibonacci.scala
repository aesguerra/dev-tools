package org.minyodev.examples.ds

/**
  * Created by aesguerra on 5/29/17.
  *
  * 1 1 2 3 5 8 13 21 34 55 89, ...
  */
object Fibonacci extends App {

  println(getFibonacci(7))

  def getFibonacci(i: Int): Int = {
    if(i == 1 || i == 2) 1
    else
      getFibonacci(i - 2) + getFibonacci(i - 1)
  }
}
