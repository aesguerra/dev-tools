package org.minyodev.examples.ds

/**
  * Created by aesguerra on 6/2/17.
  *
  * f(9) = 3, 3, 1
  * f(15) = 5, 3, 1
  * f(28) = 7, 2, 2, 1
  */
object PrimeFactorization extends App {
  getPF(165)

  def getPF(i: Int): Unit = {
    var fromTop = i / 2 // start from half
    var fromBottom = 2
    if(isPrime(i))
      print(i + " ")
    else {
      var tuloy = true
      while(tuloy) {
        if(fromTop * fromBottom == i) {
          // println("Found: " + fromTop + " * " + fromBottom + " = " + i)
          tuloy = false
          if(isPrime(fromBottom))
            print(fromBottom + " ")
          else
            getPF(fromBottom)
          if(isPrime(fromTop))
            print(fromTop + " ")
          else
            getPF(fromTop)
        }
        else if(fromTop == fromBottom) {
          fromBottom = fromBottom + 1
          fromTop = i / 2
        }
        else if(fromTop < fromBottom)  // umulit na
          fromBottom = fromBottom + 1
        else
          fromTop = fromTop - 1

        // println("fromTop: " + fromTop + ", fromBottom: " + fromBottom)
      }
    }
  }

  def isPrime(i: Int): Boolean = {
    if(i < 1)
      false
    else if(i == 2)
      true
    else
      !(2 until i).exists(x => (i % x == 0))
  }
}
