import scala.io.StdIn

object SumOfSquaresOfOddNumbers {

  def main(args: Array[String]): Unit = {
    // Prompting user for input
    println("Enter a list of numbers separated by spaces:")
    val input = StdIn.readLine()

    // Parsing input to a list of integers
    val numbers = input.split("\\s+").map(_.toInt).toList

    // Computing sum of squares of odd numbers
    val sumOfSquaresOfOdds = computeSumOfSquaresOfOddNumbers(numbers)

    // Printing the result
    println(s"Sum of squares of odd numbers: $sumOfSquaresOfOdds")
  }

  def computeSumOfSquaresOfOddNumbers(numbers: List[Int]): Int = {
    val oddNumbers = numbers.filter(num => num % 2 != 0)
    val squares = oddNumbers.map(num => num * num)
    squares.sum
  }

}
