package com.leaf.exams

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object BayesExam {
  def main(args: Array[String]) {

  val conf = new SparkConf().setAppName("UUU").setMaster("local")
  val sc = new SparkContext(conf)

//  val data = sc.textFile("E:\\spark\\AboutSpark\\spark-1.1.1\\spark\\data\\mllib\\sample_naive_bayes_data.txt")
  val data = sc.textFile("E:/items/items/result100")
  val parsedData = data.map { line =>
    val parts = line.split('\t').map{ word=>
      if(word.equals("NaN")||word.equals("NULL")||word.equals("a")||word.toDouble<0)
        0
      else if(word.equals("b"))
        1
      else
        word.toDouble
    }
    LabeledPoint(parts(0), Vectors.dense(parts.tail))
  }
  // Split data into training (60%) and test (40%).
  val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
  val training = splits(0)
  val test = splits(1)

  val model = NaiveBayes.train(training, lambda = 1.0)

  val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
  val accuracy = 1.0*predictionAndLabel.filter(x => x._1 == x._2).count() / parsedData.count()

  // Save and load model
//  model.save(sc,"E:\\abc.txt")
  System.out.println(" "+ accuracy)  // 0.400861
//  model.save(sc, "myModelPath")
//  val sameModel = NaiveBayesModel.load(sc, "E:\\abc.txt")

  }
}
