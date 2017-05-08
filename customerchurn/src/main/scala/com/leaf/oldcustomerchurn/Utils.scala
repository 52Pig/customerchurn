package com.leaf.oldcustomerchurn

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Utils {
  def initial(name : String,isLocal :Boolean) : SparkContext ={
    val conf = new SparkConf().setAppName(name)
    if(isLocal)
      conf.setMaster("local")
    val sc = new SparkContext(conf)
    sc
  }

  def parseDTR(data:RDD[String]): RDD[LabeledPoint] ={
    val validData = data.map{x=>
      val parts = x.split("\t").tail.tail
      val label = parts.last.toDouble
      val features = parts.init.map{x=>x.toDouble}
      LabeledPoint(label,Vectors.dense(features))
    }
//    validData.map{_.toString}.take(7).foreach(println)
    validData
  }

  def parseMatches(data:RDD[String]): RDD[Array[String]] ={
    val validData = data.filter(x=>x.split("\t")(20)=="电视剧"&&(x.split("\t")(15).matches("[\\u4e00-\\u9fa5]+\\s*\\d+")))
//                           ||x.split("\t")(15).matches("[\\u4e00-\\u9fa5]+\\s*[\\u4e00-\\u9fa5]*\\s*\\d+\\s*[\\u4e00-\\u9fa5]+")))
      /*val validData = data.filter(x=>x.split("\t")(20)=="电视剧")*/
//    println("all:" + validData.count) //all:378605

//    val a = validData.map{x=>x.split("\t")(15)}
//    a.filter(x=>x.matches("[\\u4e00-\\u9fa5]+\\s*[\\u4e00-\\u9fa5]*\\s*\\d+\\s*[\\u4e00-\\u9fa5]+")).foreach(println)
    val parsed = validData.map { line =>
//      val parts = line.split('\t').drop(1).map(word =>word.toString)
      val parts = line.split('\t').map(word =>word.toString)
//      println(parts.length)
      val guid = parts(0);val flag = parts(1);val active_day=parts(2);val ts_avg=parts(3);val end_date_num = parts(4);val duration=parts(5);val complete=parts(6)
      val adv_before_duration=parts(7);val before_duration=parts(8);val os=parts(9);val operator=parts(10);val video_times=parts(11);val video_format=parts(12);
      val user_id=parts(13);val vid=parts(14);val title=parts(15);val videolength=parts(16);val showid=parts(17);val channelId1=parts(18);
      val showname=parts(19);val channelId2=parts(20);val showtotallength=parts(21);val keyword=parts(22);val episodetotal=parts(23)
      val createdate_col=parts(24);val completed=parts(25);val network=parts(26);val date_type=parts(27);val datecol=parts(28);

      Array(guid,flag,active_day,ts_avg,end_date_num,duration,complete,adv_before_duration,before_duration,os,operator,video_times,video_format,user_id,vid,
        title,videolength,showid,channelId1,showname,channelId2,showtotallength,keyword,episodetotal,createdate_col,completed,network,date_type,datecol
      )
    }
    parsed
  }
//  (episodetotal-2)/

  def parsedData(data : RDD[String]) : RDD[LabeledPoint] = {
    val parsed = data.map { line =>
      val parts = line.split('\t').map(word =>
        if (word.equals("c")||word.equals("b"))
          1
        else if (word.equals("a")||word.equals("NaN")||word.equals("Infinity")||word.equals("NULL"))
          0
        else {
          val d = word.toDouble
          if (d <= 100000000 && d >= -100000000) d.toDouble
          else if (d > 100000000) 100000000
          else if (d < -100000000) -100000000
          else 0
        }
      )
      LabeledPoint(parts(0), Vectors.dense(parts.tail))
    }
    parsed
  }

  def parsedData1(data : RDD[String]) : RDD[LabeledPoint] = {
    val parsed = data.map { line =>
      val parts = line.split('\t').map(word =>
        if (word.equals("b") )
          1
        else if (word.equals("a")||word.equals("NaN")||word.equals("Infinity")||word.equals("NULL"))
          0
        else word.toDouble
      )
      LabeledPoint(parts(0), Vectors.dense(parts.tail))
    }
    parsed
  }

  def parsedData2(data:RDD[String]):RDD[Vector] = {
    val parsed = data.map { line =>
      val parts = line.split('\t').map(word =>
        if (word.equals("c") )
          1
        else if (word.equals("a")|| word.equals("b")||word.equals("NaN")||word.equals("Infinity"))
          0
        else word.toDouble
      )
      Vectors.dense(parts.tail)
    }
    parsed
  }

  def parsedHDFSData(data:RDD[String]) : RDD[LabeledPoint] = {
    val parsed = data.map { lines =>
      val parts = lines.split("\t").tail.map(word=>
         if(word.equals("b"))
           1
         else if(word.equals("a"))
           0
         else word.toDouble
      )
      LabeledPoint(parts(0),Vectors.dense(parts.tail))
    }
    parsed
  }

  def setStreamingLogLevels(): Unit = {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      Logger.getRootLogger.setLevel(Level.WARN)
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
      Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    }
  }

  def getVELEN(data: RDD[((String, String), String)]) : RDD[(String, (String, Double))] ={
    def num_pattern = "[\\d]+".r

    val res_kv = data.map{case ((k1,k2),value)=>
      for(words <- num_pattern findAllIn value) yield {
        if(words==""&&words.toDouble>100)
          (k1,k2,0.0)
        else(k1,k2,words.toDouble)
      }
    }
    val temp_kv = res_kv.flatMap(x=>x.toList).map{case (x,y,z)=>((x,y),z)}.groupByKey
    //        temp_kv.foreach(println)

    val t_kv = temp_kv.map{case ((key1,key2),values)=>
      val v = for(value<-values) yield {if(value.toDouble>100) 0 else value.toDouble}
      (key1,key2,v)
    }
    //    t_kv.foreach(println)
    val result_kv = t_kv.map{case (k1,k2,v)=>
      val vn = v.toIterator.toList.sortWith(_>_).distinct
      (k1,k2,vn)
    }
    val r_kv = result_kv.map { case (k1, k2, v) =>
      if (v.isEmpty == false) {
        val len = v.distinct.length.toDouble
        (k1, (k2, len))
      }else (k1,(k2,0.0))
    }
    r_kv
  }

  def getVE(data:RDD[((String,String),String)],size:String) : RDD[(String, (String, Double))] ={
    def num_pattern = "[\\d]+".r

    val res_kv = data.map{case ((k1,k2),value)=>
      for(words <- num_pattern findAllIn value) yield {
        if(words==""&&words.toDouble>100)
          (k1,k2,0.0)
        else(k1,k2,words.toDouble)
      }
    }
    val temp_kv = res_kv.flatMap(x=>x.toList).map{case (x,y,z)=>((x,y),z)}.groupByKey
    //        temp_kv.foreach(println)

    val t_kv = temp_kv.map{case ((key1,key2),values)=>
      val v = for(value<-values) yield {if(value.toDouble>100) 0 else value.toDouble}
      (key1,key2,v)
    }
    //    t_kv.foreach(println)
    val result_kv = t_kv.map{case (k1,k2,v)=>
      val vn = v.toIterator.toList.sortWith(_>_).distinct
      (k1,k2,vn)
    }
    //    result_kv.foreach(println)
    val r_kv = result_kv.map{case (k1,k2,v)=>
      if(v.isEmpty==false){
        val len = v.distinct.length.toDouble
        if(len==1){
          //          if(size=="max"||size=="max_2"||size=="min"||size=="min_2")(k1,k2,v(0))
          (k1,k2,0)
        }else if(len==2){
          //          if(size=="max"||size=="min_2"){(k1,k2,v.head)}else if(size=="min"||size=="max_2"){(k1,k2,v(v.length-1))}
          (k1,k2,0)
        }else if(len==3){
          if(size=="max"){(k1,k2,v(0))}else if(size=="max_2"||size=="min_2"){(k1,k2,v(1))}else if(size=="min"){(k1,k2,v(2))}
        }else if(len>3){
          if(size=="max"){
            (k1,k2,v.head)
          }else if(size=="max_2"){
            (k1,k2,v(1))
          }else if(size=="min"){
            (k1,k2,v(v.length-1))
          }else if(size=="min_2"){
            (k1,k2,v(v.length-2))
          }
        }
      }
    }
    val result = r_kv.map{case (x,y,z)=> (x.toString,(y.toString,z.toString.toDouble))}.distinct
    //        result.foreach(println)
    result
  }


  def parseMatchesNew(data: RDD[String]) :RDD[Array[String]] = {
    //        val validData = data.filter(x=>x.split("\t")(20)=="电视剧"&&(x.split("\t")(15).matches("[\\u4e00-\\u9fa5]+\\s*\\d+")))
    //                           ||x.split("\t")(15).matches("[\\u4e00-\\u9fa5]+\\s*[\\u4e00-\\u9fa5]*\\s*\\d+\\s*[\\u4e00-\\u9fa5]+")))
    val validData = data.filter(x=>x.split("\t")(19)!=""&&x.split("\t")(20)=="电视剧")
    //        println(validData.count)   //3000000
    //        validData.foreach(println)
    //    val a = validData.map{x=>x.split("\t")(15)}
    //    a.filter(x=>x.matches("[\\u4e00-\\u9fa5]+\\s*[\\u4e00-\\u9fa5]*\\s*\\d+\\s*[\\u4e00-\\u9fa5]+")).foreach(println)
    val parsed = validData.map { line =>
      val parts = line.split('\t').map(word =>word.toString)
      //      println(parts.length)
      val guid = parts(0);val flag = parts(1);val active_day=parts(2);val ts_avg=parts(3);val end_date_num = parts(4);val duration=parts(5);val complete=parts(6)
      val adv_before_duration=parts(7);val before_duration=parts(8);val os=parts(9);val operator=parts(10);val video_times=parts(11);val video_format=parts(12);
      val user_id=parts(13);val vid=parts(14);val title=parts(15);val videolength=parts(16);val showid=parts(17);val channelId1=parts(18);
      val showname=parts(19);val channelId2=parts(20);val showtotallength=parts(21);val keyword=parts(22);val episodetotal=parts(23)
      val createdate_col=parts(24);val completed=parts(25);val network=parts(26);val date_type=parts(27);val datecol=parts(28);

      Array(guid,flag,active_day,ts_avg,end_date_num,duration,complete,adv_before_duration,before_duration,os,operator,video_times,video_format,user_id,vid,
        title,videolength,showid,channelId1,showname,channelId2,showtotallength,keyword,episodetotal,createdate_col,completed,network,date_type,datecol
      )
    }
    parsed   //      val parts = line.split('\t').drop(1).map(word =>word.toString)
  }


    def getViewEpiSodeRealKV(data:RDD[(String,String)],size:String): RDD[(String,(String,Double))] ={
      def chi_pattern = "[\\u4e00-\\u9fa5]+".r
      def num_pattern = "[\\d]+".r

      val res_kv = data.map{case (key,value)=>
        for(words1 <- chi_pattern findAllIn value) yield {
          for(words2 <- num_pattern findAllIn value) yield (key,words1.distinct,words2)
        }
      }
      val temp_kv = res_kv.flatMap(x=>x.toList).flatMap(x=>x.toList).map{case (x,y,z)=>((x,y),z)}.groupByKey
      //  temp_kv.foreach(println)

      val t_kv = temp_kv.map{case ((key1,key2),values)=>
        val v = for(value<-values) yield {if(value.toDouble>100) 0 else value.toDouble}
        (key1,key2,v)
      }
      //    t_kv.foreach(println)
      val result_kv = t_kv.map{case (k1,k2,v)=>
        val vn = v.toIterator.toList.sortWith(_>_).distinct
        (k1,k2,vn)
      }
      //    result_kv.foreach(println)
      val r_kv = result_kv.map{case (k1,k2,v)=>
        if(v.isEmpty==false){
          val len = v.distinct.length.toDouble
          if(len==1){
            //          if(size=="max"||size=="max_2"||size=="min"||size=="min_2")(k1,k2,v(0))
            (k1,k2,0)
          }else if(len==2){
            //          if(size=="max"||size=="min_2"){(k1,k2,v.head)}else if(size=="min"||size=="max_2"){(k1,k2,v(v.length-1))}
            (k1,k2,0)
          }else if(len==3){
            if(size=="max"){(k1,k2,v(0))}else if(size=="max_2"||size=="min_2"){(k1,k2,v(1))}else if(size=="min"){(k1,k2,v(2))}
          }else if(len>3){
            if(size=="max"){
              (k1,k2,v.head)
            }else if(size=="max_2"){
              (k1,k2,v(1))
            }else if(size=="min"){
              (k1,k2,v(v.length-1))
            }else if(size=="min_2"){
              (k1,k2,v(v.length-2))
            }
          }
        }
      }
      val result = r_kv.map{case (x,y,z)=> (x.toString,(y.toString,z.toString.toDouble))}.distinct
      result
    }


    def getViewEpiSodeRealKVLen(data:RDD[(String,String)]): RDD[(String,(String,Double))] ={
      def chi_pattern = "[\\u4e00-\\u9fa5]+".r
      def num_pattern = "[\\d]+".r

      val res_kv = data.map{case (key,value)=>
        for(words1 <- chi_pattern findAllIn value) yield {
          for(words2 <- num_pattern findAllIn value) yield (key,words1.distinct,words2)
        }
      }
      val temp_kv = res_kv.flatMap(x=>x.toList).flatMap(x=>x.toList).map{case (x,y,z)=>((x,y),z)}.groupByKey
      //  temp_kv.foreach(println)

      val t_kv = temp_kv.map{case ((key1,key2),values)=>
        val v = for(value<-values) yield {if(value.toDouble>100) 0 else value.toDouble}
        (key1,key2,v)
      }
      //    t_kv.foreach(println)
      val result_kv = t_kv.map{case (k1,k2,v)=>
        val vn = v.toIterator.toList.sortWith(_>_).distinct
        (k1,k2,vn)
      }
      //    a.foreach(println)
      val r_kv = result_kv.map{case (k1,k2,v)=>
        if(v.isEmpty==false){
          val len = v.distinct.length.toDouble
          (k1,(k2,len))
        }
      }
      val result = r_kv.distinct.map{case (x,(y,z))=>(x.toString,(y.toString,z.toString.toDouble))}
      result
    }

  /**
   *
   * @param data  输入数据     key:guid    value:content  showname
   * @return
   */
 /* def getViewEpiSodeKV(data:RDD[(String,String)]): RDD[(String,(String,Double))] = {
    def chi_pattern = "[\\u4e00-\\u9fa5]+".r
    def num_pattern = "[\\d]+".r

    val res_kv = data.map{case (key,value)=>
      for(words1 <- chi_pattern findAllIn value) yield {
        for(words2 <- num_pattern findAllIn value) yield (key,words1.distinct,words2)
      }
    }
    val temp_kv = res_kv.flatMap(x=>x.toList).flatMap(x=>x.toList).map{case (x,y,z)=>((x,y),z)}.groupByKey
    //  temp_kv.foreach(println)

    val t_kv = temp_kv.map{case ((key1,key2),values)=>
      val v = for(value<-values) yield {if(value.toDouble>100) 0 else value.toDouble}
      (key1,key2,v)
    }
    //    t_kv.foreach(println)
    val result_kv = t_kv.map{case (k1,k2,v)=>
      val vn = v.toIterator.toList.sortWith(_>_).distinct
      (k1,k2,vn)
    }


  }*/


  @deprecated
  def getEpiSodeRealKV(data:RDD[((String,Double),String)]): RDD[(String, (String, Double))] ={
    def chi_pattern = "[\\u4e00-\\u9fa5]+".r
    def num_pattern = "[\\d]+".r

    val res_kv = data.map{case ((k1,k2),value)=>
      for(words1 <- chi_pattern findAllIn value) yield {
        for(words2 <- num_pattern findAllIn value) yield ((k1,k2),words1.distinct,words2)
      }
    }
    val temp_kv = res_kv.flatMap(x=>x.toList).flatMap(x=>x.toList).map{case (x,y,z)=>((x,y),z)}.groupByKey
    //    temp_kv.foreach(println)

    val t_kv = temp_kv.map{case ((key1,key2),values)=>
      val v = for(value<-values) yield value.toDouble
      (key1,key2,v)
    }
    //        t_kv.foreach(println)
    val result_kv = t_kv.map{case (k1,k2,v)=>
      val vn = v.toIterator.toList.sortWith(_>_).distinct
      (k1,k2,vn)
    }
    //    result_kv.foreach(println)

    val result = result_kv.map{case ((k,v2),v1,v3)=>(k,(v1,v2))}
    //    result.foreach(println)
    result
  }

  @deprecated
  def getViewTotalEpiReal(data : RDD[String]) : RDD[Double] = {
    def chi_pattern = "[\\u4e00-\\u9fa5]+".r
    def num_pattern = "[\\d]+".r
    val k = data.map(x=>
      for(words <- chi_pattern findAllIn x) yield words
    )
    val v = data.map(x=>
      for(words <- num_pattern findAllIn x) yield words.toDouble
    )
    val key = k.map(x=>x.toList.toString)
    val value = v.map(x=>x.toList)
    //    println(key.count)
    //    println(value.count)
    val maps = key.zip(value)
    val eles = maps.groupByKey
    //    eles.foreach(println)
    val sort_eles = eles.map{case (key,value) => (value.flatMap(x=>x) ,key)}
    //    sort_eles.foreach(println)
    val res_eles = for(i<-sort_eles.keys) yield{
      i.iterator.toList.sortWith(_>_).distinct
    }
    //    res_eles.foreach(println)
    val len = res_eles.map{ele=>ele.length.toDouble}
    len
  }

  @deprecated
  def getViewEpiSodeReal(data : RDD[String],size : String): RDD[Double] ={
    def chi_pattern = "[\\u4e00-\\u9fa5]+".r
    def num_pattern = "[\\d]+".r
    val k = data.map(x=>
      for(words <- chi_pattern findAllIn x) yield words
    )
    //    val a = data.map(line=>line.split("\t")).map(x=> for (words <- reg findAllIn x(0)) yield words)
    /*for ( key<-k){
      key.foreach(println)
    }*/

    val v = data.map(x=>
      for(words <- num_pattern findAllIn x) yield words.toDouble
    )
    val key = k.map(x=>x.toList.toString)
    val value = v.map(x=>x.toList)
    //    println(key.count)
    //    println(value.count)
    val maps = key.zip(value)
    val eles = maps.groupByKey
    //    eles.foreach(println)
    val sort_eles = eles.map{case (key,value) => (value.flatMap(x=>x) ,key)}
    //    sort_eles.foreach(println)
    val res_eles = for(i<-sort_eles.keys) yield{
      i.iterator.toList.sortWith(_>_).distinct
    }
    //    res_eles.foreach(println)
    val result = res_eles.map{ele=>
      if(ele.isEmpty==false){
        val len = ele.length.toDouble
        if(len==1){
          ele(0)
        }else if(len>=2){
          if(size=="max"){
            ele.head
          }else if(size=="max_2"){
            ele(1)
          }else if(size=="min"){
            ele(ele.length-1)
          }else if(size=="min_2"){
            ele(ele.length-2)
          }
        }
      }
    }
    result.map(_.toString.toDouble)
  }


  /**
   * 计算所观看的剧集是第几集
   * @param data 所给RDD数据
   * @param size max:观看的最后一集；max_2观看的倒数第二集;min观看的第一集;min_2观看的第二集
   * @return 返回所需结果
   */
  @deprecated
  def getViewEpiSode(data : RDD[String],size :String) : RDD[Double] = {
    val k = data.map(x=>if (x.length>5&&x.contains(" ")) x.substring(0,x.lastIndexOf(" ")).trim else x)
    val v = data.map(x=>
      if (x.length > 5 && x.contains(" ")) x.substring(x.lastIndexOf(" "), x.length).trim.toDouble else x.toDouble
    )

    val maps = k.zip(v)
    val eles = maps.groupByKey
    val sort_eles = eles.map{case (key,value)=> (value,key)}

    //    sort_eles.keys.foreach(println)
    val res_eles = for(i <- sort_eles.keys) yield {
      i.iterator.toList.sortWith(_>_)
    }

    val result = res_eles.map{ele=>
      if(ele.isEmpty==false){
        val len = ele.length.toDouble
        if(len==1){
          ele(0)
        }else if(len>=2){
          if(size=="max"){
            ele.head
          }else if(size=="max_2"){
            ele(1)
          }else if(size=="min"){
            ele(ele.length-1)
          }else if(size=="min_2"){
            ele(ele.length-2)
          }
        }
      }
    }
    result.map(_.toString.toDouble)
  }

  /**
   * 返回用户观看剧集的总数
   * @param data 输入RDD数据
   * @return 返回用户观看剧集的总数
   */
  @deprecated
  def getViewTotalEpi(data : RDD[String]) : RDD[Int] = {

    val k = data.map(x=>if (x.length>5&&x.contains(" ")) x.substring(0,x.lastIndexOf(" ")).trim else x)
    val v = data.map(x=>
      try {
        if (x.length > 5 && x.contains(" ")) x.substring(x.lastIndexOf(" "), x.length).trim.toDouble else x

      } catch {
        case e : NumberFormatException => println(x)
      }
    )
    val maps = k.zip(v)
    val eles = maps.groupByKey
    val sort_eles = eles.map{case (key,value)=> (value,key)}
    val len = sort_eles.keys.distinct.map(x=>x.toList.length)
    //    sort_eles.keys.map(x=>x.toList.length).foreach(println)
    len
  }

  def getStr(str:String): Unit = {
    for(item<-str.split("&")){
      if(item.startsWith("guid=")){
        return item.substring(5)
      }
    }

}}
