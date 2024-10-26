import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

class LoanApprovalModel(spark: SparkSession) {

  // Method to load and preprocess the data
  def loadAndPreprocessData(filePath: String) = {
    // Load the dataset
    val loanData = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)

    // Index categorical columns
    val genderIndexer = new StringIndexer().setInputCol("gender").setOutputCol("genderIndex")
    val occupationIndexer = new StringIndexer().setInputCol("occupation").setOutputCol("occupationIndex")
    val educationIndexer = new StringIndexer().setInputCol("education_level").setOutputCol("educationIndex")
    val maritalStatusIndexer = new StringIndexer().setInputCol("marital_status").setOutputCol("maritalStatusIndex")
    val loanStatusIndexer = new StringIndexer().setInputCol("loan_status").setOutputCol("label")

    // Apply indexer to data
    val indexedData = genderIndexer
      .fit(loanData)
      .transform(loanData)
      .transform(occupationIndexer.fit(loanData).transform(_))
      .transform(educationIndexer.fit(loanData).transform(_))
      .transform(maritalStatusIndexer.fit(loanData).transform(_))
      .transform(loanStatusIndexer.fit(loanData).transform(_))

    // Assemble features into a single vector
    val assembler = new VectorAssembler()
      .setInputCols(Array("age", "genderIndex", "occupationIndex", "educationIndex", "maritalStatusIndex", "income", "credit_score"))
      .setOutputCol("features")

    val finalData = assembler.transform(indexedData).select("features", "label")
    finalData
  }

  // Method to train the model
  def trainModel(dataPath: String): LogisticRegressionModel = {
    val finalData = loadAndPreprocessData(dataPath)

    // Split the data into training and test sets
    val Array(trainingData, testData) = finalData.randomSplit(Array(0.8, 0.2))

    // Initialize logistic regression
    val logisticRegression = new LogisticRegression()

    // Train the model
    val model = logisticRegression.fit(trainingData)

    // Evaluate the model accuracy
    val predictions = model.transform(testData)
    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
    val accuracy = evaluator.evaluate(predictions)

    println(s"Model accuracy: $accuracy")

    model // Return the trained model
  }

  // Method to predict new loan approval
  def predictNewLoanApproval(model: LogisticRegressionModel, newInput: Seq[(Int, String, String, String, String, Int, Int)]): Unit = {
    import spark.implicits._

    // Prepare the new input data
    val inputData = newInput.toDF("age", "gender", "occupation", "education_level", "marital_status", "income", "credit_score")

    // Apply the same preprocessing
    val genderIndexer = new StringIndexer().setInputCol("gender").setOutputCol("genderIndex")
    val occupationIndexer = new StringIndexer().setInputCol("occupation").setOutputCol("occupationIndex")
    val educationIndexer = new StringIndexer().setInputCol("education_level").setOutputCol("educationIndex")
    val maritalStatusIndexer = new StringIndexer().setInputCol("marital_status").setOutputCol("maritalStatusIndex")

    val inputIndexed = genderIndexer
      .fit(inputData)
      .transform(inputData)
      .transform(occupationIndexer.fit(inputData).transform(_))
      .transform(educationIndexer.fit(inputData).transform(_))
      .transform(maritalStatusIndexer.fit(inputData).transform(_))

    // Assemble features
    val assembler = new VectorAssembler()
      .setInputCols(Array("age", "genderIndex", "occupationIndex", "educationIndex", "maritalStatusIndex", "income", "credit_score"))
      .setOutputCol("features")

    val inputAssembled = assembler.transform(inputIndexed)

    // Predict the probability of loan approval
    val result = model.transform(inputAssembled)
    result.select("probability").show(false)
  }
}

object LoanApprovalApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("LoanApprovalApp").master("local[*]").getOrCreate()

    val model = new LoanApprovalModel(spark)

    // Train the model
    val dataPath = "C:\\Users\\SHREERAKSHA\\Downloads\\Case study-3\\loans_dataset\\loan.csv"
    val trainedModel = model.trainModel(dataPath)

    // Predict for a new input
    val newInput = Seq(
      (51, "Female", "Manager", "Bachelor's", "Married", 90000, 750)
    )
    model.predictNewLoanApproval(trainedModel, newInput)

    spark.stop()
  }
}
