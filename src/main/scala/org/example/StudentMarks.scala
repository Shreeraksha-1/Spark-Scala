package org.example

object StudentMarks extends App {
  val studentMarks = Map(
    "student1" -> List(70, 75, 80, 90),
    "student2" -> List(90, 95, 80, 70),
    "student3" -> List(80, 90, 75, 100)
  )

  // Calculate sum of total marks by all students
  val totalSum = studentMarks.values.flatten.sum

  // Calculate average of all students
  val totalNumMarks = studentMarks.values.flatten.toSeq.length
  val totalAverage = totalSum.toDouble / totalNumMarks

  // Print sum of total marks by all students and average of all students
  println(s"Sum of total marks by all students: $totalSum")
  println(s"Average of all students: $totalAverage")

  // Create a new map with each student and their average marks
  val averageMap = studentMarks.map { case (student, marks) =>
    val average = marks.sum.toDouble / marks.length
    (student, average)
  }

  // Print the new map with student -> average pairs
  println("Average marks for each student:")
  averageMap.foreach { case (student, average) =>
    println(s"$student -> $average")
  }
}