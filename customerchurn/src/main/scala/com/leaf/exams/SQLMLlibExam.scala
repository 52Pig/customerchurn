package com.leaf.exams

import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.catalyst.expressions.Row
//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SQLMLlibExam {
  def main(args : Array [String]) {
    //屏幕不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //设置运行环境
    val conf = new SparkConf().setAppName("SQLMLlib")
    val sc = new SparkContext(conf)
    /*val hiveContext = new HiveContext(sc)

    //使用spark sql查出每个店的销售数量和金额
    hiveContext.sql("use saledata")
    hiveContext.sql("SET spark.sql.shuffle.partitions=20")
    val sqldata = hiveContext.sql("select a.locationid,sum(b.qty) totalqty,sum(b.amount) totalamount from tblStock a join tblstockdetail b on a.ordernumber=b.ordernumber group by a.locationid")

    //将查询数据转换成向量
    val parseData = sqldata.map{
      case Row(_, totalqty,totalamount) =>
        val features = Array[Double](totalqty.toString.toDouble,totalamount.toString.toDouble)
        Vectors.dense(features)
    }
    //对数据集聚类，3个类，20次迭代，形成数据模型
    //设置partitions为20
    val numClusters = 3
    val numIterations = 20
    val model = KMeans.train(parseData,numIterations,numClusters)
    //用模型对读入的数据进行分类,并输出
    //由于partition没设置，输出为200个小文件，可以使用hdfs dfs -getmerge 合并并下载
    sqldata.map{
      case Row(locationid,totalqty,totalamount) =>
        val features = Array[Double](totalqty.toString.toDouble,totalamount.toString.toDouble)
        val linevector = Vectors.dense(features)
        val prediction = model.predict(linevector)
        locationid+" "+ totalqty +" "+ totalamount +" "+prediction
    }.saveAsTextFile(args(0))
    sc.stop*/
  }
}
