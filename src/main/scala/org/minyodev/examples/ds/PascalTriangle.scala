package org.minyodev.examples.ds

/**
  * Created by aesguerra on 5/25/17.
  * 1
  * 1 1
  * 1 2 1
  * 1 3 3 1
  * 1 4 6 4 1
  * 1 5 10 10 5 1
  * 1 6 15 20 15 6 1
  */
object PascalTriangle extends App {

  println(getPt(7, 4))

  def getPt(r: Int, c: Int): Int = {
    if(c > r) 0
    else if(r == 1 || c == 1) 1
    else if(c == 2) r - 1
    else
      getPt(r - 1, c - 1) + getPt(r - 1, c)
  }
}