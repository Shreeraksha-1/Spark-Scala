package  org.example

import org.apache.spark.sql.SparkSession

object sprakDemo{
  def main(args: Array[String]): Unit = {

    val spark= SparkSession.builder()
      .master("local[1]")
      .appName("Sprak Scala First project")
      .getOrCreate()
    println("Printing the spark session variable")
    println("app name"+ spark.sparkContext.appName)
    println("Deployment Mode" + spark.sparkContext.deployMode)
    println("Master" + spark.sparkContext.master)

    spark.stop()
  }
}