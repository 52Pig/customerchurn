package com.leaf.future

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object App {
//  println( "Hello World!" )


  def main(args: Array[String]) {

    trainRFTest

//    println(getData())
//    println(getData().size)
  }

  def getData(): mutable.HashMap[Int, Int] = {
    val data = new mutable.HashMap[Int,Int]()
    for(i <- 1 to 50){
      data += (i -> (2*i))
    }
    data
  }

  def testAgg(): Unit ={
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    val cc = sc.parallelize(Array((1,2,3,4),(0,1,2,3)))
    //    val dd = cc.treeAggregate((0,0,0,0))(
    //      seqOp = (m,n) => (m._1+n._1,m._2+n._2,m._3+n._3,m._4+n._4),
    //      combOp = (v1,v2)=>(v1._1+v2._1,v1._2+v2._2,v1._3+v2._3,v1._4+v2._4)
    //    )
    //    println(dd)

    val dd = cc.treeAggregate(0, 0.0, 0L)(
      seqOp = (c, v) =>{
        //            val cc = c._1 //DenseVector[Double] breeze
        // c: (grad, loss, count) v: (label, features)
        (c._1, c._2 + 1, c._3 + 1)
      },
      combOp = (c1, c2) => {
        // c: (grad, loss, count)
        (c1._1 + c2._1, c1._2 + c2._2, c1._3 + c2._3)
      })
    println(dd)
  }

  def test(): Unit ={
//    require(numTrees > 0, "TreeEnsembleModel cannot be created without trees.")
    //其它代码省略

    //通过投票实现最终的分类
    /**
      * Classifies a single data point based on (weighted) majority votes.
      */
    /*private def predictByVoting(features: Vector): Double = {
      val votes = mutable.Map.empty[Int, Double]
      trees.view.zip(treeWeights).foreach { case (tree, weight) =>
        val prediction = tree.predict(features).toInt
        votes(prediction) = votes.getOrElse(prediction, 0.0) + weight
      }
      votes.maxBy(_._2)._1
    }*/


    /**
      * Predict values for a single data point using the model trained.
      *
      * @param features array representing a single data point
      * @return predicted category from the trained model
      */
    //不同的策略采用不同的预测方法
    /*def findSplitsBins(features: Vector): Double = {
      (algo, combiningStrategy) match {
        case (Regression, Sum) =>
          predictBySumming(features)
        case (Regression, Average) =>
          predictBySumming(features) / sumWeights
        case (Classification, Sum) => // binary classification
          val prediction = predictBySumming(features)
          // TODO: predicted labels are +1 or -1 for GBT. Need a better way to store this info.
          if (prediction > 0.0) 1.0 else 0.0
        //随机森林对应 predictByVoting 方法
        case (Classification, Vote) =>
          predictByVoting(features)
        case _ =>
          throw new IllegalArgumentException(
            "TreeEnsembleModel given unsupported (algo, combiningStrategy) combination: " +
              s"($algo, $combiningStrategy).")
      }
    }*/
  }

  def trainRFTest(): Unit ={
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    import org.apache.spark.mllib.tree.RandomForest
    import org.apache.spark.mllib.util.MLUtils

    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, "E:/source-code/spark-1.3.0/data/mllib/sample_libsvm_data.txt")
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 3 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>

      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    labelAndPreds.foreach(println)
    /*val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification forest model:\n" + model.toDebugString)

    // Save and load model
    model.save(sc, "myModelPath")
    val sameModel = RandomForestModel.load(sc, "myModelPath")*/
  }

  def trainTest(): Unit ={
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    import org.apache.spark.mllib.tree.DecisionTree
    import org.apache.spark.mllib.util.MLUtils

    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, "E:/source-code/spark-1.3.0/data/mllib/sample_libsvm_data.txt")
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    labelAndPreds.foreach(println)
    /*val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification tree model:\n" + model.toDebugString)*/
  }


}
