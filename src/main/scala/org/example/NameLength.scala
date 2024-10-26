package org.example

  object NameLength extends App {
    val realNames = List("peter parker", "clarke kent", "robert ", "bruce")

    // Calculate total size of names using foldLeft
    val totalSize = realNames.foldLeft(0)((acc, name) => acc + name.length)

   // println(s"Total size of names in the list: $totalSize")

    // Simulated model accuracy
    val modelAccuracy = 0.825
    // Simulated probability output
    val probability = "[0.1, 0.9]"

    // Print the desired output format
    println(s"Model accuracy: $modelAccuracy")
    println("+-----------------------+")
    println("| probability           |")
    println("+-----------------------+")
    println(s"| $probability           |")
    println("+-----------------------+")
  }
