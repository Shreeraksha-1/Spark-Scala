package org.example

import org.apache.spark.sql.SparkSession

import scala.util.Try

object Reverse_Integer {

  def ReverseInteger(num:BigInt) :BigInt={
    val sign = if(num < 0) -1 else 1
    val reversedInteger=BigInt(num.abs.toString.reverse)
    reversedInteger*sign
  }

  def main(args: Array[String]): Unit = {
    println("Enter a 32-bit signed integer:")
    val userInput = readInt().toLong // Reading as Long to handle 32-bit signed integer range
    val num=BigInt(userInput)

    val ReversedNumber=ReverseInteger(num)
    if(ReversedNumber.isValidInt){
      println(s"Reversed Number is:${ReversedNumber.toInt}")
    }
    else {
      println("Result exceeds 32-bit integer range.")
    }
  }
}
