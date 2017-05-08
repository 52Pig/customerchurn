package com.leaf.exams

import com.leaf.oldcustomerchurn.Utils
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object SVMClaExam {
  def main(args: Array[String]) {
    val sc = Utils.initial("abc",true)
    val data = sc.textFile("E:/items/items/sample1")
    val parsedData = data.map { line =>
      val parts = line.split('\t').map(word=>

        if(word.equals("c"))
          3
        else if(word.equals("b"))
          2
        else if(word.equals("a"))
          1
        else word.toDouble
      )
//      System.out.print(parts)
      LabeledPoint(parts(0) , Vectors.dense(parts.tail))
    }
    val splits = parsedData.randomSplit(Array(0.6,0.4),seed = 11L)
    val (training,test) = (splits(0),splits(1))

    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

//    val model = DecisionTree.trainClassifier(training, numClasses, categoricalFeaturesInfo,impurity, maxDepth, maxBins)
//    val model =  LinearRegressionWithSGD.train(training,100)
    //  val model = NaiveBayes.train(training, lambda = 1.0)
//    DecisionTree.trainClassifier(training,3,Map[Int,Int](),"gini",5,32)
    val model = new LogisticRegressionWithLBFGS()
    .setNumClasses(10)
    /*val labelAndPreds = test.map{point=>
      val prediction = model.predict(point.features)
      (point.label,prediction)
    }
    val trainErr = labelAndPreds.filter(r => r._1!=r._2).count().toDouble/parsedData.count
    print(trainErr)*/
//    val recall = labelAndPreds.filter(r=>r._1==r._2).count.toDouble/labelAndPreds.count
//    println("recall :"+recall)

//    val precision = labelAndPreds.filter(r=>r._1.equals(2)==r._2.equals(2)).count.toDouble/labelAndPreds.filter(k=>k.equals(2)).count
//    println("precision :" +precision)
//    val metrics = new BinaryClassificationMetrics(labelAndPreds)
//    val auROC = metrics.areaUnderROC()
//    println("Area under ROC " +auROC)  //Area under ROC 0.5

  }

  def svmtrain(args: Array[String]): Unit ={
    //    val conf = new SparkConf().setAppName("Classfy Test").setMaster("spark://a268.datanode.hadoop.qingdao.youku:7077")
    val conf = new SparkConf().setAppName("Classfy Test").setMaster("local")
    val sc = new SparkContext(conf)
    //    val data = sc.textFile(args(0))
    //    val data = sc.hadoopFile[BytesWritable,Text,BZip2Codec](args(0))
    val data = sc.textFile(args(0))

    /*val parsedData = data.map{ line =>
        val parts = line.split("\t").map(word=>
          if(word.equals("a")||word.equals("NULL")||word.equals("NaN")||word.toDouble<0)
            0
          else if(word.equals("b"))
            1
          else if(word.getClass.isInstance(new String()))
            word.toDouble
          else word.toDouble
        )
      LabeledPoint(parts(0) , Vectors.dense(parts.tail))
    }*/

    val parsedData = data.map { line =>
      val parts = line.split('\t').map(word=>
        if(word.equals("a")||word.equals("NULL")||word.equals("NaN")||word.equals("SEQ")||word.toDouble<0)
          0
        else if(word.equals("b"))
          1
        else word.toDouble
      )
      LabeledPoint(parts(0) , Vectors.dense(parts.tail))
    }
    //    data.map(line=>line.split('\t')(0)).foreach(println)
    //设置迭代次数
    val numIterations = 20

    //    System.out.println(model.weights)
    val splits = parsedData.randomSplit(Array(0.7,0.3),seed=11l)

    val training = splits(0).cache
    val test = splits(1)

    val svmAlg = new SVMWithSGD()
    svmAlg.optimizer.setNumIterations(200).setRegParam(0.1).setUpdater(new L1Updater)
    val modelL1 = svmAlg.run(training)
    //    val model = SVMWithSGD.train(training,numIterations)

    modelL1.clearThreshold()

    val labelAndPreds = test.map{
      point=>
        val prediction = modelL1.predict(point.features)
        (point.label,prediction)
    }
    val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble/parsedData.count
    println("Training Error : " + trainErr)   //Training Error 0.300477

    val metrics = new BinaryClassificationMetrics(labelAndPreds)
    val auROC = metrics.areaUnderROC()
    println("Area under ROC " +auROC)  //Area under ROC 0.5

  }

}
