package com.leaf.app_antispam

import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD

object AntiSpamFeatureEngineering {
  def featureEngineeringProccess( data: RDD[Array[String]]) : RDD[(String, Array[String],  Double, Vector)] ={
    val userLabelAndFeatures = data.map{x=>
      val flags = x(0).toDouble
      val guid = x(1)
      val pv = x(4).toDouble
      val urlcount = x(5).toDouble
      val initnum = x(6).toDouble
      val homenum= x(7).toDouble
      val startpagenum = x(8).toDouble
      val playnum = x(9).toDouble
      val videodetailnum = x(10).toDouble
      val vvnum = x(11).toDouble
      val tsnum = x(12).toDouble
      //      val tshour = x(13)
      val iseqver = if(x(14).toBoolean) 1.0 else -1.0
      val iseqosid = if(x(15).toBoolean) 1.0 else -1.0
      val historynum = x(16).toDouble
      val localvv = x(17).toDouble
      val loginnum = x(18).toDouble
      val interval = x(19).toDouble
      //      `pids` string,
      val ipcount = x(21).toDouble
      val provcount = x(22).toDouble
      //      val vers = x(23)
      val idfanum = x(23).toDouble
      val osversionnum = x(24).toDouble
      val videoidnum = x(25).toDouble
      val ispvthreshold = if(x(26).toBoolean) 1.0 else -1.0
      val advnum = x(27).toDouble
      val banneradvnum = x(28).toDouble
      val responsenum = x(29).toDouble
      val cornerscreennum = x(30).toDouble
      val audionum = x(31).toDouble
      val friendinfonum = x(32).toDouble
      val videocommentnum = x(33).toDouble
      val subscribeupdatenum = x(34).toDouble
      val subscribelistnum = x(35).toDouble
      val sessionidnum = x(36).toDouble
      //      val sidtimedur = x(37)
      val useridnum = x(38).toDouble
      val duration = x(39).toDouble
      val commonurlnum = x(40).toDouble
      val durationnum = x(41).toDouble
      val features = Array(pv, urlcount, initnum, homenum, startpagenum, playnum, videodetailnum,vvnum,tsnum,
        iseqver, iseqosid, historynum, localvv, loginnum, interval, ipcount, provcount, idfanum,
        osversionnum, videoidnum, ispvthreshold, advnum, banneradvnum, responsenum, cornerscreennum,
        audionum, friendinfonum, videocommentnum, subscribeupdatenum, subscribelistnum, sessionidnum,
        useridnum,duration,commonurlnum,durationnum)
        (guid, x, flags, Vectors.dense(features))
    }
    userLabelAndFeatures
  }
}
