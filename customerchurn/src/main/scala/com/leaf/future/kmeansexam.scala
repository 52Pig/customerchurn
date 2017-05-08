package com.leaf.future

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object kmeansexam {
  /* kmeans_data.txt
0.0 0.0 0.0
0.1 0.1 0.1
0.2 0.2 0.2
9.0 9.0 9.0
9.1 9.1 9.1
9.2 9.2 9.2
   */
  def main(args: Array[String]) {
    // 屏蔽日志
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local").setAppName("kmeans")
    val sc = new SparkContext(conf)
    val data = sc.textFile("F:\\java\\spark\\spark\\data\\mllib\\kmeans_data.txt")
    val parsedData = data.map(s=> Vectors.dense(s.split(" ").map(_.toDouble)))
    //将数据集聚类，2个类，20次迭代，形成数据模型
    val numClusters = 2
    val numIterations = 20
//    KMeans.train(parsedData,numClusters,numIterations)
    val model = KMeans.train(parsedData,numClusters,numIterations)
    //数据模型的中心点
    println("Cluster centers")
    for(c <- model.clusterCenters){
      println(" " + c.toString)
    }

    //使用误差平方和来评估模型
    val cost = model.computeCost(parsedData)
    println("Within Set Sum of Squared Errors="+cost)
    //使用模型测试单点数据
    println("Vectors 0.2 0.2 0.2 is belongs to clusters:" + model.predict(Vectors.dense("0.2 0.2 0.2".split(" ").map(_.toDouble))))
    //交叉评估1
    val testdata = data.map(s=>Vectors.dense(s.split(" ").map(_.toDouble)))
    val result1 = model.predict(testdata)
    result1.saveAsTextFile("F:\\java\\spark\\spark\\data\\mllib\\test2.txt")
    //交叉评估2返回结果和数据集
    val result2 = data.map{
      line =>
        val lineverctor = Vectors.dense(line.split(" ").map(_.toDouble))
        val prediction = model.predict(lineverctor)
        line + " " + prediction
    }.saveAsTextFile("F:\\java\\spark\\spark\\data\\mllib\\test2.txt")
    sc.stop()
  }
}
