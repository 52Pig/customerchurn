package com.leaf.exams

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.{SparkConf, SparkContext}

object DecisionTreeClaExam {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Decision Tree").setMaster("local")
    val sc = new SparkContext(conf)
//    val data = MLUtils.loadLibSVMFile(sc,"E:\\spark\\AboutSpark\\spark-1.1.1\\spark\\data\\mllib\\sample_libsvm_data.txt").cache()
//    val data = MLUtils.loadLibSVMFile(sc,"E:/items/items/result100")
//    val splits = data.randomSplit(Array(0.7,0.3))
    //    val (trainingData,testData) = (splits(0).map(word=>if(word.equals("a")) 0 else if(word.equals("b")) 1 else word),
    //                                    splits(1).map(word=>if(word.equals("NULL")) 0 else word))
    val data = sc.textFile("E:/items/items/sample1")

    val parsedData = data.map{line=>
      val parts = line.split("\t").map(word=>
        if(word.equals("a")||word.equals("NULL")||word.equals("NaN")||word.toDouble<0)
          0
        else if(word.equals("c"))
          1
        else if(word.equals("b"))
          2
        else word.toDouble
      )
      LabeledPoint(parts(0),Vectors.dense(parts.tail))
    }
    val splits = parsedData.randomSplit(Array(0.7,0.3),seed = 11l)
    val (trainingData,testData) = (splits(0),splits(1))
    //Train a DecisionTree model
    //Empty categoricalFeaturesInfo indicates all features are continuous
    val numClasses = 3
    val categoricalFeaturesInfo = Map[Int,Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 100
    
    val model = DecisionTree.trainClassifier(trainingData,numClasses,categoricalFeaturesInfo,impurity,maxDepth,maxBins)
    //Evaluate model on training instances and compute training error
    val labelAndPreds = testData.map{point=>
      val prediction = model.predict(point.features)
      (point.label,prediction)
    }

    val trainErr = 1.0 * labelAndPreds.filter( r=> r._1 != r._2).count.toDouble/parsedData.count
    println("Training Error = " + trainErr)     //
    println(testData.count.toDouble)         //300477.0
    println("Learned classification tree model : \n" + model.toString)
  }
}
