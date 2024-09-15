package org.example

import scala.io.StdIn.readLine

object BirdNestBuilder {
  def main(args: Array[String]): Unit = {
    val winutilPath = "C:\\softwares\\winutils" // Set path to winutils.exe for Windows environment

    // Detect if running on Windows and set Hadoop-related system properties
    if (System.getProperty("os.name").toLowerCase.contains("win")) {
      System.out.println("Detected Windows OS")
      System.setProperty("hadoop.home.dir", winutilPath)
      System.setProperty("HADOOP_HOME", winutilPath)
    }

    // Prompt the user to input the forest array (comma-separated values)
    println("Enter the forest (comma-separated integers, where 0 represents empty spots):")
    val forestInput = readLine()

    // Convert the input string into an Array[Int]
    val forest = forestInput.split(",").map(_.trim.toInt)

    // Prompt the user to input the bird's starting position
    println("Enter the bird's initial position (index of the forest, which should be a zero):")
    val bird = readLine().toInt

    // Safeguard to check if the bird's initial position is valid
    if (forest(bird) != 0) {
      println("Invalid bird position! The bird must start at an empty spot (value 0).")
      return
    }

    var totalLength = 0        // Total length of sticks collected
    var sticksFound = List[Int]() // List of indices of sticks collected
    var right = true           // Start by flying right
    var leftIdx = bird - 1     // Left index to start looking to the left
    var rightIdx = bird + 1    // Right index to start looking to the right

    // Loop until total stick length is at least 100
    while (totalLength < 100 && (leftIdx >= 0 || rightIdx < forest.length)) {
      if (right) {
        // Bird flies to the right
        while (rightIdx < forest.length && forest(rightIdx) == 0) {
          rightIdx += 1
        }
        if (rightIdx < forest.length && forest(rightIdx) > 0) {
          totalLength += forest(rightIdx)      // Add stick length to nest
          sticksFound = sticksFound :+ rightIdx // Record the stick's index
          rightIdx += 1                        // Move the right pointer further
        }
      } else {
        // Bird flies to the left
        while (leftIdx >= 0 && forest(leftIdx) == 0) {
          leftIdx -= 1
        }
        if (leftIdx >= 0 && forest(leftIdx) > 0) {
          totalLength += forest(leftIdx)       // Add stick length to nest
          sticksFound = sticksFound :+ leftIdx // Record the stick's index
          leftIdx -= 1                         // Move the left pointer further
        }
      }
      right = !right // Alternate direction after each collection
    }

    // Print the collected sticks' indices
    println("The bird collected sticks from the following positions:")
    println(sticksFound.mkString(", "))
  }
}
