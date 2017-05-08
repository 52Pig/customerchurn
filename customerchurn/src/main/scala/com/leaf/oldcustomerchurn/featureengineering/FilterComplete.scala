package com.leaf.oldcustomerchurn.featureengineering

import org.apache.spark.{SparkConf, SparkContext}

object FilterComplete {
  def main(args: Array[String]) {
    if(args.length!=2){
      System.err.print("Usage filtercomplete.jar in out")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("filter complete")
    val sc = new SparkContext(conf)
//  sc.textFile(args(0)).flatMap(_.trim.split("\\s+")).map((_,1)).reduceByKey(_ + _).saveAsTextFile(args(1))
    sc.textFile(args(0)).filter(_.trim.contains("complete=1")).flatMap(_.split("\t")).filter(_.contains("&")).flatMap(x=>x.trim.split("&")).filter(_.contains("guid=")).map(line=>(line.substring(5))).map((_,1)).reduceByKey(_+_).saveAsTextFile(args(1))
    sc.stop
  }

}
