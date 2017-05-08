package com.leaf.nn

import breeze.optimize.linear.PowerMethod
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object TestDBN {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DBNTest").setMaster("local")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)
    val data_path = "E:\\Items\\items\\data\\DBNtest.txt"
    val examples = sc.textFile(data_path).cache()
//    examples.foreach(println)
//    examples.map{x=>x.split("\\s+")(2)}.foreach(println)
    val train_d1 = examples.map{line=>
      val f1 = line.split("\\s+")
//      println(f1.toString)
      val f = f1.map(f=>f.toDouble)
      val id = f(0)
      val y = Array(f(1))
//      y.foreach(println)
      val x = f.slice(2,f.length)
//      x.foreach(println)
      (id,new PowerMethod.BDM(1,y.length,y),new PowerMethod.BDM(1,x.length,x))
    }
//    train_d1.foreach(println)
    val train_d = train_d1.map(f => (f._2,f._3))
    val opts = Array(100.0,20.0,0.0)
    //设置训练参数，建立DBN模型
    val DBNmodel = new DBN().setSize(Array(5,7)).setLayer(2).setMomentum(0.1).setAlpha(1.0).DBNtrain(train_d,opts) //输出花费时间

    //DBN模型转化为NN模型
    val mynn = DBNmodel.dbnunfoldtonn(1)
    val nnopts = Array(100.0,50.0,0.0)
    val numExamples = train_d.count()
//    println(numExamples)
//    println(s"numExamples = $numExamples.") // 12
//    println(mynn._2) //3
    for(i <- 0 to mynn._1.length - 1){
      print(mynn._1(i)+"\t")        //5	7	1
    }
    println()
    println("mynn_W1")
    val tmpw1 = mynn._3(0)
    for(i <- 0 to tmpw1.rows - 1){
      for(j <- 0 to tmpw1.cols - 1){
        print(tmpw1(i,j)+"\t")
      }
      println()
    }
    val NNmodel = new NeuralNet().
      setSize(mynn._1).
      setLayer(mynn._2).
      setActivation_function("sigm").
      setOutput_function("sigm").
      setInitW(mynn._3).
      NNtrain(train_d, nnopts)

    //5 NN模型测试
    val NNforecast = NNmodel.predict(train_d)
    val NNerror = NNmodel.Loss(NNforecast)
    println(s"NNerror = $NNerror.")
    val printf1 = NNforecast.map(f => (f.label.data(0), f.predict_label.data(0))).take(200)
    println("预测结果——实际值：预测值：误差")
    for (i <- 0 until printf1.length)
      println(printf1(i)._1 + "\t" + printf1(i)._2 + "\t" + (printf1(i)._2 - printf1(i)._1))



  }
}
