package com.leaf.future

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix

object LinearRegSelf {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("linearreg-self")
    val sc = new SparkContext(conf)
    val input_data = "F:\\java\\spark\\spark\\data\\mllib\\ridge-data\\lpsa.data"
    val data = sc.textFile(input_data)
    val examples = data.map{ line =>
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    }.cache()

    val addIntercept : Boolean = true
    val alpha = 0.001
    val numIterations = 5000
    var loss = 10.0
    val sData = if(addIntercept){
      examples.map{x=>(x.label, 1.0 +: x.features.toArray)}
    } else {
      examples.map(x=>(x.label, x.features.toArray))
    }

    val numFeatures = examples.first().features.toArray.length
    val initialWeights = new Array[Double](numFeatures)
    val initialWeightsWithIntercept = if(addIntercept){
      0.0 +: initialWeights
    } else {
      initialWeights
    }
    val numExamples = examples.count().toInt // 样本点个数
    var weights = new DoubleMatrix(initialWeightsWithIntercept.length ,1, initialWeightsWithIntercept:_*)
    println("initial weights: " + weights)

    val label = sData.map(x=>x._1).collect()
    val features = sData.map(x=>x._2).collect()
    var hypothesis = 0.0
    var midError = 0.0

    for( k<- 0 until numIterations
         if(loss > 1.0)){
      val i = (new util.Random).nextInt(numFeatures)
//blog.csdn.net/springlustre/article/details/48828507
      val variable = new DoubleMatrix(features(i).length, 1, features(i):_*)
      hypothesis = variable.dot(weights)
      midError = label(i) - hypothesis
      weights = weights.add(variable.mul(alpha * midError))
      println("The current weights: " + weights)

      var cacheLoss = 0.0
      for(j <- 0 to (numExamples - 1)){
        var multiplier = new DoubleMatrix(features(j).length, 1, features(j):_*)
        cacheLoss += (label(j) - weights.dot(multiplier)) * (label(j) - weights.dot(multiplier))
      }
      loss = 0.5 * cacheLoss / numExamples
      println("The current loss: " + loss)
    }

//    for(i<- 0 to 10 ){
//      println(i)
//    }

    sc.stop()


  }

}
