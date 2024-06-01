package  org.example

import org.apache.spark.sql.SparkSession


object sprakDemo{
  case class person(Name: String, age:Int)
  def main(args: Array[String]): Unit = {
    val spark= SparkSession.builder()
      .master("local[1]")
      .config("spark.hadoop.validateOutputSpecs", "false") // Disable output specification validation
      .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") // Use the local file system
      .config("spark.hadoop.mapreduce.framework.name", "local") // Use local execution framework
      // Use local execution framework
      // Disable output specification validation
      .getOrCreate()

    println("Printing the spark session variable")
    println("app name"+ spark.sparkContext.appName)
    println("Deployment Mode" + spark.sparkContext.deployMode)
    println("Master" + spark.sparkContext.master)

    //creating rdd from Parallelized Collections
    val rdd1= spark.sparkContext.parallelize(Seq(1,2,3,4,5,6,7))

    val data = Seq(person("Harry",26),
      person("Rama",27),
      person("Tejaswi",31))

    val rdd2= spark.sparkContext.parallelize(data)

    val RddToJson=rdd2.map(person=>s"""{"name":"$person.Name}",{"age":$person.age}""")

    RddToJson.saveAsTextFile("C:\\Rdd_Json1")

    import spark.implicits._
    //Save as File
    rdd2.toDF().show()


    spark.stop()
  }
}