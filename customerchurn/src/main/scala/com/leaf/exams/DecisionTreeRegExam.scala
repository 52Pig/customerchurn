package com.leaf.exams

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.DoubleRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}

object DecisionTreeRegExam {

   def main(args: Array[String]) {

     val conf = new SparkConf().setAppName("DecisionTree Regression").setMaster("local")
     val sc = new SparkContext(conf)

     val data = MLUtils.loadLibSVMFile(sc,"E:\\spark\\spark-1.1.1\\spark-1.1.1\\data\\mllib\\ridge-data\\lpsa.data").cache

     val categoricalFeaturesinfo = Map[Int,Int]()
     val impurity = "variance"
     val maxDepth = 5
     val maxBins = 100
     val model = DecisionTree.trainRegressor(data,categoricalFeaturesinfo,impurity,maxDepth,maxBins)
     val labelsAndPreds = data.map{point =>
       val prediction = model.predict(point.features)
       (point.label,prediction)
     }
     //StandardScaler能够把feature按照列转换成mean=0,standard deviation=1的正态分布
     val trainMSE = labelsAndPreds.map{a => math.pow((a._1-a._1),2)}
     val a = new DoubleRDDFunctions(trainMSE)
     println(a.mean())
     val rdd1 = sc.parallelize(Array((1,2), (1,4), (2,5)))
     rdd1.map(a => a._1 - a._2)
     println("Training Mean Squared Error " + trainMSE)
     println("Learned regression tree model :\n" +model)
   }

}
