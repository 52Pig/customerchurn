package com.leaf.oldcustomerchurn.featureengineering

import org.apache.spark.{SparkConf, SparkContext}

object FilterOnline {
  def main(args: Array[String]) {

    if(args.length!=2){
      System.err.println("Usage filteronline.jar in out")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Filter Online")
//    JavaRDD a = new SparkContext(conf)
    val sc = new SparkContext(conf)

    sc.textFile(args(0)).filter(_.trim.contains("play_types=net")).flatMap(_.split("\t")).filter(_.contains("&")).flatMap(x=>x.trim.split("&")).filter(_.contains("guid=")).map(line=>(line.substring(5))).map((_,1)).reduceByKey(_+_).saveAsTextFile(args(1))

    sc.stop
  }
}
