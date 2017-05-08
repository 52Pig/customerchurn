package com.leaf.app_antispam

import org.apache.spark.{SparkConf, SparkContext}


object AntiSpamFlags {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("app_antispam_flags")
    val sc = new SparkContext(conf)
    val data = sc.textFile(args(0))
//    val data = sc.textFile("F:/samples/allguid.txt")
    val line = data.map(_.split("\t"))
    val allGuidOsidKey = line.map{x=>
      (x(0)+"\t"+x(1),x.mkString("\t"))
    }
//    allGuidOsidKey.foreach(println)
    val errorGuidOsidData = sc.textFile(args(1))
//    val errorGuidOsidData = sc.textFile("F:/samples/errorguid.txt")
    val errorGuidOsid = errorGuidOsidData.map(_.split("\t")).map{x=>
      (x(0)+"\t"+x(2),x.mkString("\t"))
    }
//    errorGuidOsid.foreach(println)
    val positiveGuid = allGuidOsidKey.join(errorGuidOsid).map{case (x,(y,z))=>(x,"1\t"+y)}
//    positiveGuid.foreach(println)

    val negativeGuid = allGuidOsidKey.subtractByKey(errorGuidOsid).map{case (x,y)=>(x,"0\t"+y)}
//    negativeGuid.foreach(println)
    val allGuidOsidWithFlags = positiveGuid ++ negativeGuid
//    allGuidOsidWithFlags.foreach(println)
    val app_antispam_flags = allGuidOsidWithFlags.map{case (x,y)=>y}.persist()
    app_antispam_flags.saveAsTextFile(args(2))
//    viewLast2Epi.map{case (k,(v1,v2))=>((k,v1),v2)}.join(viewHead2Epi.map{case (k,(v1,v2))=>((k,v1),v2)})
  }
}
// args(0):30.7 G
// args(1):6.9 G
// 结果：31.1 G
//消耗时间：	11mins, 27sec

