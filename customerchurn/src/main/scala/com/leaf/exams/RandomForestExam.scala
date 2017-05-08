package com.leaf.exams

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object RandomForestExam {
  def main(args: Array[String]) {
    val path = "E:/items/items/sample"
    val reader = scala.io.Source.fromFile(path).getLines()
    val arrs = reader.toArray.foreach(word=>
      if(word.equals("a"))
        0
      else if(word.equals("b"))
        1
      else if(word.equals("c"))
        2
    )

    println(arrs)
    val conf = new SparkConf().setAppName("RandomForest").setMaster("local")
    val sc = new SparkContext(conf)

    val data = MLUtils.loadLibSVMFile(sc,"E:/items/items/sample")
//    val data = sc.textFile("E:/items/items/sample")
    val parsedData = data.map{
       word=>
        if(word.equals("a"))
          0
        else if(word.equals("b"))
          1
        else if(word.equals("c"))
          2
        else word
     println(word)
    }

    val splits = parsedData.randomSplit(Array(0.7,0.3))
    val (trainingData,testData) = (splits(0),splits(1))

    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 3
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 8 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 10
    val maxBins = 32

//    RandomForest.supportedFeatureSubsetStrategies.foreach(println)
//    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
//      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins,4)


//    val model = RandomForest.trainClassifier(trainingData,numClasses,categoricalFeaturesInfo,numTrees,featureSubsetStrategy,
//                  impurity,maxDepth,maxBins)

    // Evaluate model on test instances and compute test error
//    val labelAndPreds = testData.map { point =>
//      val prediction = model.predict(point.features)
//      (point.label, prediction)
//    }
//    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / data.count()
//    println("Test Error = " + testErr)
//    println("Learned classification forest model:\n" + model.toDebugString)

    // Save and load model
//    model.save(sc, "myModelPath")
//    val sameModel = RandomForestModel.load(sc, "myModelPath")

  }

  def rftrain(): Unit ={
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    //    sc.parallelize()
    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "F:/java/spark/spark/data/mllib/sample_libsvm_data.txt")
    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    //    val model = new LogisticRegressionWithLBFGS()
    //      .setNumClasses(2)
    //      .run(training)
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 3 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32

    val model = RandomForest.trainClassifier(training, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    //    model.clearThreshold()
    //    println(model.weights)
    println(model)
    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (features,prediction, label)
    }
    //    predictionAndLabels.foreach(println)

    // Get evaluation metrics.
    //    val metrics = new MulticlassMetrics(predictionAndLabels)
    //    val precision = metrics.precision
    //    println("Precision = " + precision)

    // Save and load model
    //    model.save(sc, "myModelPath")
    //    val sameModel = LogisticRegressionModel.load(sc, "myModelPath")

  }

}
