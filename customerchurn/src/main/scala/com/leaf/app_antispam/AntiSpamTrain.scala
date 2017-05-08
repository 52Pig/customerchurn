package com.leaf.app_antispam

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}


object AntiSpamTrain {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("app_antispam_train")
    val sc = new SparkContext(conf)
    val data = sc.textFile(args(0))
    val dataArray = data.map(_.split("\t"))
    val userLabelAndFeatures = AntiSpamFeatureEngineering.featureEngineeringProccess(dataArray)
    val labelAndFeatures = userLabelAndFeatures.map{case (user,x, label ,features)=> LabeledPoint(label, features)}
    val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(labelAndFeatures)
    val testData = sc.textFile(args(1))
    val testDataArray = testData.map(_.split("\t"))
    val testLabelAndFea = AntiSpamFeatureEngineering.featureEngineeringProccess(testDataArray)
    val result = testLabelAndFea.map{ case (user, x, label, features) =>
      val prediction = model.predict(features)
      x + "\t" + prediction
    }
    result.saveAsTextFile(args(2))
  }
}
