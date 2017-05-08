package com.leaf.exams

import org.apache.spark.{SparkConf, SparkContext}

object KMeansExam {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("KmeansTest").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile("E:\\spark\\AboutSpark\\spark-1.1.1\\spark\\data\\mllib\\kmeans_data.txt")

    val parsedData = data.map( _.split(' ').map(_.toDouble).toVector)
    val numIterations = 20
    val numClusters = 2

    /*val clusters= KMeans.train(parsedData,numClusters,numIterations)
    //统计聚类错误的样本比例
    val WSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors " + WSSE) */// 计算集内均方差总和
  }
}
