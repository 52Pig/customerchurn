package com.leaf.exams

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.io.Source
object Proc {

  def main(args: Array[String]) {
//    prc1("E:/items/proccess/usercenter_home_channelVideoClick.20150608","E:/items/proccess/result.txt")
//    println("我是Proc，可执行jar打包成功！")
    val conf = new SparkConf().setAppName("spark-streaming-test").setMaster("local[0]")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" ")) // not necessary since Spark 1.3
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()


  }

  def prc1(datas: String, out: String): Unit = {
//    for(line <- Source.fromFile(datas,"utf-8").mkString.split("\t")){
//        println(line.length+":"+line)
//      var reads = line.toString.split("\t")
//      reads.foreach(println)
      /*for(a <- reads){
          println(a)
      }*/
//      print(reads)
//    }

    val sources = Source.fromFile(datas,"utf-8").getLines
    val iters = sources.toArray
    var row_index = 0;
    /*while(iters.hasNext&&row_index==0){
      for(i<-iters){
        println(i)
      }
      row_index=row_index+1;
    }*/
    /*for(line <- sources){

    }*/

    /*val das = sources.mkString("\n")
    val ds = das.split("\t")
    val cc = ds(0)+"\t"+ds(1)+"\t"+ds(2)+"\t"+ds(3)+"\t"+ds(4)+"\t"+ds(5)
*/
//    val sbkey = ds.init.init.mkString("\t")
//    val sbvalue = ds(6)+"\t"+ds(7)
//    val t_sb = Map(sbkey,sbvalue)

//    sbkey.foreach(println)
    /*for(dd <- ds){
      println(dd)
    }*/
//    println(das)
  }
}
