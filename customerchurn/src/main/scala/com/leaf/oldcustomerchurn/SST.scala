package com.leaf.oldcustomerchurn

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Seconds}


object SST {

  def main(args: Array[String]) {

//local[0]
    val conf = new SparkConf().setMaster("local").setAppName("SDK_Streaming_Test")
//    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(conf,Seconds(60))
//    val lines = ssc.textFileStream("hdfs:///source/mobile/otherlog/20151015/2340_10.109.0.114.gz")
//    val lines = ssc.textFileStream("file:///D:\\stream.txt")
    val lines = ssc.socketTextStream("localhost",9999)

//    println(lines.count())

//    val events = shuffleData(lines)

    /*val res = events.foreachRDD{datas=>
      val uvs = datas.map{m=>
        (m._5,(m._1,m._2,m._3,m._4))
      }.distinct().map(x=>(x._2,1)).reduceByKey(_+_).sortByKey(false)
      val pvs = datas.map{m=>((m._1,m._2,m._3,m._4),1)}.reduceByKey(_+_).sortByKey(false)
      val cc = uvs.join(pvs)
     cc.take(10)
    }*/



  }

//ReceiverInput

  /*def shuffleData(lines : DStream[String]):  DStream[(String, String, String, String, String)] ={
    val res = lines.map{line=>
      val data = line.trim.split("\t")
      var refercode = ""
      var guid = ""
      var ouid = ""
      var osver = ""
      var pid = ""
      var channel_page = ""
      var videoname = ""
      var pos = ""
      var vid = ""
      if(data.length>75 && !data(73).isEmpty &&
        data(73).contains("y1.home.channelVideoClick")||
        data(73).contains("y4.home.channelVideoClick")||
        data(73).contains("y5.home_shome.channelVideoClick")||
        data(73).contains("y2.home_shome.channelVideoClick")
      ){
        try{
          refercode = URLDecoder.decode(URLEncoder.encode(data(73).trim(), "UTF-8"), "UTF-8")
        }catch {
          case e : java.lang.IllegalArgumentException => ""
        }
        /*val c = Try(URLDecoder.decode(data(73).trim(), "UTF-8")).flatMap{
          x => Try(URLDecoder.decode(URLEncoder.encode(data(73).trim(), "UTF-8"), "UTF-8"))
        }*/

        guid = data(1).trim
        ouid = data(3).trim
        osver = data(10).trim
        pid = if(data.length>0) data(0) else ""
        vid = if(refercode.contains("y2.home_shome.channelVideoClick")||refercode.contains("y5.home_shome.channelVideoClick")) refercode.split("\t")(4).trim else refercode.split("\t")(3).trim
        pos = refercode.split("_").last

      }
      for(dd <- data) {
        if(data(58).contains("ct")||data(58).contains("cn")||data(58).contains("cs")){
          /*try {
            URLDecoder.decode(URLEncoder.encode(data(58).trim(), "UTF-8"), "UTF-8")
          }catch {
            case e : Exception => return
          }*/

          val str = Try(URLDecoder.decode(URLEncoder.encode(data(58).trim(), "UTF-8"), "UTF-8")) match {
            case Success(strs) => strs
            case Failure(e) => ""
          }
          val ps = getreq_params(str)
          if(refercode.contains("y2.home_shome.channelVideoClick")||refercode.contains("y5.home_shome.channelVideoClick")){
            channel_page = ps.get("ct").toString
            videoname = ps.get("title").toString
          }else{
            channel_page = ps.get("cn").toString
            videoname = ps.get("title").toString
          }
        }
      }
      (channel_page,pos,vid,videoname,if (refercode.contains("y2.home_shome.channelVideoClick")||refercode.contains("y5.home_shome.channelVideoClick")) ouid else guid)
    }
    res
  }*/

/*  def getreq_params(req_args:String) :  Map[String, String] ={
    val params = req_args.split("&")
    val cc = for (param <- params) yield {
      if (param == null || param.equals("=")) {
      }
      val key_value = param.split("=")
      val key = key_value(0)
      var value = ""

      if (param.endsWith("=")) {
        val offset = param.indexOf("=")
        value = param.substring(offset + 1)
      } else {
        if (key_value.length == 2) {
          value = key_value(1)
        } else if (key_value.length > 2) {
          val offset = param.indexOf("=")
          value = param.substring(offset + 1)
        }
      }
//      val mm : HashMap[String,String] = new HashMap[String,String]
//      (key,value)
val c = Map(key,value)
      c
    }
    val d = cc.toList.flatten.toMap
    d
  }*/


}
