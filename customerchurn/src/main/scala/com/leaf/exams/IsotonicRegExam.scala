package com.leaf.exams

import com.leaf.oldcustomerchurn.Utils
import org.apache.spark.mllib.regression.IsotonicRegression
//保序回归例子
object IsotonicRegExam {
  def main(args: Array[String]) {
    val sc = Utils.initial("naivebayes test 3:1",true)
    val data = sc.textFile("E:/items/items/sample_400_1_3")
    val parsedData = data.map { line =>
      val parts = line.split(',').map(_.toDouble)
      (parts(0), parts(1), 1.0)
    }

    // Split data into training (60%) and test (40%) sets.
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)

    // Create isotonic regression model from training data.
    // Isotonic parameter defaults to true so it is only shown for demonstration
    val model = new IsotonicRegression().setIsotonic(true).run(training)

    // Create tuples of predicted and real labels.
    val predictionAndLabel = test.map { point =>
      val predictedLabel = model.predict(point._2)
      (predictedLabel, point._1)
    }

    // Calculate mean squared error between predicted and real labels.
    val meanSquaredError = predictionAndLabel.map{case(p, l) => math.pow((p - l), 2)}.mean()
    println("Mean Squared Error = " + meanSquaredError)
  }
}
