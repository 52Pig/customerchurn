package com.leaf.exams

import com.leaf.oldcustomerchurn.Utils
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.{LBFGS, LogisticGradient, SquaredL2Updater}
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object LogisticRegWithAUCExam {
  def main(args: Array[String]) {
    //    trainDTR()
    trainDTROptimize()
  }

  def trainDTROptimize(): Unit = {
    val sc = Utils.initial("abc", true)
    val d = sc.textFile("E:/items/items/data3/guid_all_uv_train_head300.txt")

    val data = MLUtils.loadLibSVMFile(sc, "E:/items/items/data3/guid_all_uv_train_head300.txt")
    val numFeatures = data.take(1)(0).features.size
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).map(x => (x.label, MLUtils.appendBias(x.features))).cache()
    val test = splits(1)

    val numCorrections = 10
    val convergenceTol = 1e-4
    val maxNumIterations = 20
    val regParam = 0.1
    val initialWeightsWithIntercept = Vectors.dense(new Array[Double](numFeatures + 1))

    val (weightsWithIntercept, loss) = LBFGS.runLBFGS(
      training,
      new LogisticGradient(),
      new SquaredL2Updater(),
      numCorrections,
      convergenceTol,
      maxNumIterations,
      regParam,
      initialWeightsWithIntercept)

    val model = new LogisticRegressionModel(
      Vectors.dense(weightsWithIntercept.toArray.slice(0, weightsWithIntercept.size - 1)),
      weightsWithIntercept(weightsWithIntercept.size - 1))
    model.clearThreshold()
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()
    println("Loss of each step in training process")
    loss.foreach(println)
    println("Area under ROC = " + auROC)
  }

  def trin: Unit ={
    val conf = new SparkConf().setAppName("LRAPITest").setMaster("local")
    val sc = new SparkContext(conf)
    val url = "E:/spark/AboutSpark/spark-1.1.1/spark/data/mllib/ridge-data/lpsa.data"
    val data = sc.textFile(url)

    val parsedData = data.map{line=>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1).split(' ').map(x=>x.toDouble)))
    }
    val numIterations = 20
    val model = LinearRegressionWithSGD.train(parsedData,numIterations)
    val valueAndPreds = parsedData.map{point=>
      val prediction = model.predict(point.features)
      (point,prediction)
    }

    valueAndPreds.foreach{case (v,p) => print("["+ v.label+","+p+"]");
      v.features.toArray.foreach(println);
      println("")
    }
    val isSucceed = valueAndPreds.map{case (v,p) => math.pow((p-v.label),2)
    }.reduce(_+_)/valueAndPreds.count
    println(isSucceed)
  }

}
