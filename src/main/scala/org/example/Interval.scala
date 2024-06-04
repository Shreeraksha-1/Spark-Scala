import org.apache.spark.sql.SparkSession

case class Interval(start: Int, end: Int)

object MergeIntervals {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("MergeIntervals")
      .getOrCreate()

    import spark.implicits._

    // Sample intervals (you can replace this with your own data)
    val sampleIntervals = Seq(
      Interval(1, 3),
      Interval(2, 6),
      Interval(8, 10),
      Interval(15, 18),
      Interval(15, 18),
      Interval(15, 34)
    )

    // Create Dataset from sample intervals
    val intervalsDS = sampleIntervals.toDS()

    // Sort intervals by their start times
    val sortedIntervals = intervalsDS.orderBy("start")

    // Merge overlapping intervals
    val mergedIntervals = sortedIntervals.mapPartitions { iter =>
      var result = List.empty[Interval]
      var prevInterval: Interval = null

      iter.foreach { interval =>
        if (prevInterval == null || interval.start > prevInterval.end) {
          // If there's no overlap, add the interval to the result
          result = interval :: result
          prevInterval = interval
        } else {
          // If there's an overlap, merge the intervals
          prevInterval = Interval(prevInterval.start, interval.end max prevInterval.end)
          result = result.tail
          result = prevInterval :: result
        }
      }

      result.iterator
    }

    // Output the merged intervals
    mergedIntervals.toDF().show()

    spark.stop()
  }
}
