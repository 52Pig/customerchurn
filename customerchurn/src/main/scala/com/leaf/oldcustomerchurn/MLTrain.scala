package com.leaf.oldcustomerchurn

import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.tree.{DecisionTree, GradientBoostedTrees, RandomForest}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MLTrain {
  val sparkConf = new SparkConf().setAppName("DecisionTree").setMaster("local")
  val sc = new SparkContext(sparkConf)

  def main(args: Array[String]) {
    // Load and parse the data file.
    //    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
//    val data = sc.textFile("D:/decision_tree_classification_test_5.txt")
    val data = sc.textFile("E:\\items\\items\\new\\result_train_model")
//    data.take(100).foreach(println)
    //    val data = sc.textFile("D:/youku_guid_new_rate.txt")
    classification(data)
    sc.stop()
  }

  def classification(data: RDD[String]): Unit = {
    val splits = data.randomSplit(Array(0.6, 0.4))

    val rddA = splits(0).filter(a => a.split("\t")(1) == "a").sample(false, 0.3, 123L)
    val rddB = splits(0).filter(a => a.split("\t")(1) == "b")
    val rddC = splits(0).filter(a => a.split("\t")(1) == "c")

    //    val temp1 = parseData(rddA).map(a => a.features(3).toDouble)
    //    val temp2 = parseData(rddB ++ rddC).map(a => a.features(3).toDouble)
    //    println(temp1.sum / temp1.count)
    //    println(temp2.sum / temp2.count)

    val trainingData = parseData(rddA ++ rddC ++ rddB ++ rddC)

    /*ResultWriter.println("训练集中，ABC类分别有：")
    ResultWriter.println(parseData(rddA).count())
    ResultWriter.println(parseData(rddB).count())
    ResultWriter.println(parseData(rddC).count()*2)*/

    val testData = parseData(splits(1))

    //    val dd = sc.textFile("D:/active_train_attr_detail.txt")
    //    val originalData = dd.map(a => {
    //      val s = a.split("\t")
    //      ((s(0), s.dropRight(13)), s(s.length - 1))
    //    })
    //      .groupByKey()
    //      .map(a => a._1._2 :+ AttrDetail.parseDatecol(a._2))
    //
    //    val data = originalData.map(line => {
    //      val t = line.tail.map { a =>
    //        if (a.equals("a")) 1
    //        else if (a.equals("b")) 0
    //        else if (a.equals("c")) 0
    //        else {
    //          val d = a.toDouble
    //          if (d <= 10000000 && d >= -10000000) d.toDouble
    //          else 0
    //        }
    //      }
    //      LabeledPoint(t(0), Vectors.dense(t.tail))
    //    }
    //    )


    //    val rddA = trainingData.filter {
    //      line =>
    //        line.label > 0.9
    //    }
    //    val rddB = trainingData.filter {
    //      line =>
    //        line.label < 0.1
    //    }
    //        val rddC = trainingData.filter {
    //          line =>
    //            (line.split('\t').tail)(0).equals("c")
    //        }


    //    val testData = parseData(splits(1))

    //      val (trainingData, testData) = (splits(0), splits(1))
    trainingData.cache()

    val model = randomForestTrain(trainingData)
    println(model.toDebugString)


    val params = test(testData, model)
    val TP = params(0)
    val FP = params(1)
    val TN = params(2)
    val FN = params(3)
    val P = TP + FN
    val N = TN + FP

    val accuracy = (TP.toDouble + TN) / (P + N)
    val recall = TP.toDouble / P
    val precision = TP.toDouble / (TP + FP)
    val f1 = (2 * precision * recall) / (precision + recall)

    /*ResultWriter.println("TP\\FP\\TN\\FN分别为:  " + TP + "    " + FP + "       " + TN + "     " + FN)
    ResultWriter.println("准确率为：" + accuracy)
    ResultWriter.println("recall：" + recall)
    ResultWriter.println("precision:" + precision)
    ResultWriter.println("f值:" + f1)
    ResultWriter.println("\n\n\n")*/

    println("TP\\FP\\TN\\FN分别为:  " + TP + "    " + FP + "       " + TN + "     " + FN)
    println("准确率为：" + accuracy)
    println("recall：" + recall)
    println("precision:" + precision)
    println("f值:" + f1)



    //k-fold
    //    val splits = parsedData.randomSplit(Array(0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1))
    //    var total = 0
    //    for (i <- 0 to 9) {
    //      val trainingData = splits(i)
    //      val testData = (splits.dropRight(10 - i) union splits.drop(i + 1)).reduce(_ union _)
    //      //      val (trainingData, testData) = (splits(0), splits(1))
    //      trainingData.cache()
    //
    //      val model = train(trainingData)
    //      val testErr = test(testData, model)
    //
    //      println("第" + (i + 1) + "次错误率：" + testErr)
    //      total = total + 1
    //    }
    //    println("平均错误率："+ total / 10)

    //    println(model.trees(0).toDebugString)
    // Save and load model
    //    model.save(sc, "myModelPath")
    //    val sameModel = DecisionTreeModel.load(sc, "myModelPath")
  }


  //返回TruePositive, FalsePositive, TrueNegetive, FalseNegetive
  def test(testData: RDD[LabeledPoint], model: RandomForestModel) = {
    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    //流失用户为positive，存留用户为negetive
    var TP = sc.accumulator(0, "TP")
    var FP = sc.accumulator(0, "FP")
    var TN = sc.accumulator(0, "TN")
    var FN = sc.accumulator(0, "FN")
    //    val testErr = labelAndPreds.filter(r => {
    //      r._1 != r._2
    //    }).count.toDouble / testData.count()
    labelAndPreds.foreach(r => {
      val a = r._1
      val b = r._2
      if (a < 0.01 && b < 0.01) TP += 1 //预测为流失用户，实际为流失用户
      else if (a > 0.99 && b < 0.01) FP += 1 //预测为流失用户，实际为未流失用户
      else if (a < 0.01 && b > 0.99) FN += 1 //预测为未流失用户，实际为流失用户
      else if (a > 0.99 && b > 0.99) TN += 1 //预测为未流失用户，实际为未流失用户
      else println(a + "   " + b)
    })
    //    println("Test Error = " + testErr)
    //    println("Learned classification tree model:\n" + model.toString)
    Array(TP.value, FP.value, TN.value, FN.value)
  }


  def decisionTreeTrain(trainingData: RDD[LabeledPoint]) = {
    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    //    val numClasses = 2
    //    val categoricalFeaturesInfo = Map[Int, Int]()
    //    val impurity = "gini"
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val maxDepth = 12

    val maxBins = 100

    /*ResultWriter.println("决策树分类")
    ResultWriter.println("maxBins:" + maxBins)
    ResultWriter.println("maxDepth:" + maxDepth)*/

    //    val model = RandomForest.trainClassifier(trainingData,
    //      treeStrategy, numTrees, featureSubsetStrategy, seed = 12345)
    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, "gini", maxDepth, maxBins)
    //        val model = NaiveBayes.train(trainingData, lambda = 1.0)
    //        val model = SVMWithSGD.train(trainingData, numIterations)
    //    model.clearThreshold()

    //    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
    //      impurity, maxDepth, maxBins)

    model
  }

  def randomForestTrain(trainingData: RDD[LabeledPoint]) = {
    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    //    val numClasses = 2
    //    val categoricalFeaturesInfo = Map[Int, Int]()
    //    val impurity = "gini"

    val numTrees = 20
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 10
    val maxBins = 20

    /*ResultWriter.println("随机森林分类")
    ResultWriter.println("numTrees:" + numTrees)
    ResultWriter.println("maxDepth:" + maxDepth)
    ResultWriter.println("maxBins:" + maxBins)*/

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    //    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, "gini", maxDepth, maxBins)
    //        val model = NaiveBayes.train(trainingData, lambda = 1.0)
    //        val model = SVMWithSGD.train(trainingData, numIterations)
    //    model.clearThreshold()

    //    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
    //      impurity, maxDepth, maxBins)

    model
  }

  def naiveBayesTrain(trainingData: RDD[LabeledPoint]) = {
    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    //    val numClasses = 2
    //    val categoricalFeaturesInfo = Map[Int, Int]()
    //    val impurity = "gini"

    val lambda = 1.0
    /*ResultWriter.println("朴素贝叶斯分类")
    ResultWriter.println("lambda:" + lambda)*/

    //    val model = RandomForest.trainClassifier(trainingData,
    //      treeStrategy, numTrees, featureSubsetStrategy, seed = 12345)
    val model = NaiveBayes.train(trainingData, lambda)
    //        val model = SVMWithSGD.train(trainingData, numIterations)
    //    model.clearThreshold()

    //    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
    //      impurity, maxDepth, maxBins)

    model
  }


  def svmTrain(trainingData: RDD[LabeledPoint]) = {
    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    //    val numClasses = 2
    //    val categoricalFeaturesInfo = Map[Int, Int]()
    //    val impurity = "gini"
    val numIterations = 100

    /*ResultWriter.println("svm分类")
    ResultWriter.println("numIterations:" + numIterations)*/

    //    val model = RandomForest.trainClassifier(trainingData,
    //      treeStrategy, numTrees, featureSubsetStrategy, seed = 12345)
    //    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, "gini", maxDepth, maxBins)
    //        val model = NaiveBayes.train(trainingData, lambda = 1.0)
    val model = SVMWithSGD.train(trainingData, numIterations)
    //    model.clearThreshold()

    //    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
    //      impurity, maxDepth, maxBins)

    model
  }

  def logisticRegressionTrain(trainingData: RDD[LabeledPoint]) = {
    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    //    val numClasses = 2
    //    val categoricalFeaturesInfo = Map[Int, Int]()
    //    val impurity = "gini"

//    ResultWriter.println("逻辑回归分类")

    //    val model = RandomForest.trainClassifier(trainingData,
    //      treeStrategy, numTrees, featureSubsetStrategy, seed = 12345)
    //    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, "gini", maxDepth, maxBins)
    //        val model = NaiveBayes.train(trainingData, lambda = 1.0)
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(trainingData)
    //    model.clearThreshold()

    //    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
    //      impurity, maxDepth, maxBins)

    model
  }


  def linearRegressionTrain(trainingData: RDD[LabeledPoint]) = {
    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    //    val numClasses = 2
    //    val categoricalFeaturesInfo = Map[Int, Int]()
    //    val impurity = "gini"

    val numIterations = 100
    /*ResultWriter.println("线性回归分类")
    ResultWriter.println("numIterations:" + numIterations)*/

    //    val model = RandomForest.trainClassifier(trainingData,
    //      treeStrategy, numTrees, featureSubsetStrategy, seed = 12345)
    //    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, "gini", maxDepth, maxBins)
    //        val model = NaiveBayes.train(trainingData, lambda = 1.0)
    val model = LinearRegressionWithSGD.train(trainingData, numIterations)
    //    model.clearThreshold()

    //    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
    //      impurity, maxDepth, maxBins)

    model
  }

  def boostedTreesTrain(trainingData: RDD[LabeledPoint]) = {
    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    //    val numClasses = 2
    //    val categoricalFeaturesInfo = Map[Int, Int]()
    //    val impurity = "gini"

    val numIterations = 20
    val maxDepth = 10
    /*ResultWriter.println("boosting trees")
    ResultWriter.println("numIterations:" + numIterations)
    ResultWriter.println("maxDepth::" + maxDepth)*/

    // Train a GradientBoostedTrees model.
    //  The defaultParams for Classification use LogLoss by default.
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.setNumIterations(numIterations) // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.setNumClasses(2)
    boostingStrategy.treeStrategy.setMaxDepth(maxDepth)
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    //    boostingStrategy.treeStrategy.setCategoricalFeaturesInfo(Map[Int, Int]())

    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)
    //    model.clearThreshold()

    //    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
    //      impurity, maxDepth, maxBins)

    model
  }


  def parseData(data: RDD[String]) = {

//      .filter {
//      a => {
//        val s = a.split('\t')
//        if (s(7) == "NULL") false
//        else {
//          s(7).toDouble > 7
//        }
//      }
//    }
    val rdd1 = data.map { line =>
      val lineArray = line.split('\t').tail.dropRight(1).map(
        a => {
          if (a.equals("a")) 1
          else if (a.equals("b")) 0
          else if (a.equals("c")) 0
          else if (a == "NULL") 0
          else {
            val d = a.toDouble
            if (d <= 100000000 && d >= -100000000) d.toDouble
            else if (d > 100000000) 100000000
            else if (d < -100000000) -100000000
            else 0
          }
        }
      )
      Array(lineArray(0), lineArray(1), lineArray(2), lineArray(3), lineArray(4), lineArray(5), lineArray(6),
        lineArray(7), lineArray(8), if (lineArray(1).abs >= 0.001) lineArray(6) / lineArray(1) else 0,
        if (lineArray(8).abs >= 0.001) lineArray(6) / lineArray(8) else 0,
        lineArray(6) / (lineArray(7) + 1))
      //            lineArray
    }

    //    minMaxNormalization(rdd1).map(a => LabeledPoint(a(0), Vectors.dense(a.tail)))
    rdd1.map(a => LabeledPoint(a(0), Vectors.dense(a.tail)))


  }

  //min-max normalization
  //第一个是类标号，将其他的属性规范化
  def minMaxNormalization(rdd1: RDD[Array[Double]]): RDD[Array[Double]] = {

    val minArray = rdd1.reduce((a, b) => {
      val length = a.length
      val min = new Array[Double](length)
      for (i <- 1 to length - 1) {
        if (a(i) < b(i))
          min.update(i, a(i))
        else
          min.update(i, b(i))
      }
      min
    })

    val maxArray = rdd1.reduce((a, b) => {
      val length = a.length
      val max = new Array[Double](length)
      for (i <- 1 to length - 1) {
        if (a(i) < b(i))
          max.update(i, b(i))
        else
          max.update(i, a(i))
      }
      max
    })

    rdd1.map(a => {
      val length = a.length
      for (i <- 1 to length - 1) {
        a(i) = 1.0 * (a(i) - minArray(i)) / (maxArray(i) - minArray(i))
      }
      a
    })
  }

}
