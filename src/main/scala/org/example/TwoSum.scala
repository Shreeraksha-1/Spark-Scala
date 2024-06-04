package org.example

import org.apache.spark.sql.SparkSession

import scala.util.Try

object TwoSum {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TwoSumSpark")
      .master("local[1]")
      .getOrCreate()

    //Take array of integers as input from the user
    println("Enter Space-Separated integers")
    //scala.io.StdIn, which contains the readLine method, and scala.util.Try, which will be used for parsing integers safely.
    /*
    val input = readLine()
    val Array1 = input.split(" ").map(_.toInt)

    when I'm trying with above code was getting error like cannot resolve symbol split and _.toInt
    also can't use readLine() because of new update in scala
     */
    val input = scala.io.StdIn.readLine()
    val Array1 = input.split(" ").flatMap(str => Try(str.toInt).toOption)

    // Take target from the user
    println("Enter the target sum:")
    val target = scala.io.StdIn.readInt()

    // Parallelize the array into an RDD
    val numsRDD = spark.sparkContext.parallelize(Array1.zipWithIndex)

    // Find the indices of the two numbers that sum up to the target
    // Collect the RDD into memory and perform operations
    val nums = numsRDD.collect()

    // Find the indices of the two numbers that sum up to the target
    val result = nums.flatMap { case (num, idx) =>
      val complement = target - num
      nums.filter { case (otherNum, otherIdx) =>
        otherNum == complement && otherIdx != idx
      }.map { case (_, otherIdx) =>
        (idx, otherIdx)
      }
    }

    // Print the indices
    println("Indices of the two numbers that sum up to the target:")
    result.foreach(println)

    spark.stop()
  }
}
