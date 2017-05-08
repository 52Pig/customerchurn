package com.leaf.future

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}

object Rec {

  def main(args: Array[String]) {
    exam
  }

  def exam(): Unit ={
    /*
    * test.data :
    * 1,1,5.0
    1,2,1.0
    1,3,5.0
    1,4,1.0
    2,1,5.0
    2,2,1.0
    2,3,5.0
    2,4,1.0
    3,1,1.0
    3,2,5.0
    3,3,1.0
    3,4,5.0
    4,1,1.0
    4,2,5.0
    4,3,1.0
    4,4,5.0
    * */
    val conf = new SparkConf().setMaster("local").setAppName("ALS")
    val sc = new SparkContext(conf)
    val data = sc.textFile("F:\\java\\spark\\spark\\data\\mllib\\als\\test.data")
    val ratings = data.map(_.split(",")) map{case Array(user,item,rate) =>
      Rating(user.toInt,item.toInt,rate.toDouble)
    }
    //使用最小二乘法(ALS)建立评估模型
    //rank 模型中隐语义模型的个数  lambda正则化参数
    //numBlocks 用于并行计算化计算的分块个数(设置为-1为自动配置)
    //implicitPrefs: 决定是用显性反馈ALS的版本还是用适用隐性反馈数据集的版本
    val rank = 10
    val numberIterations = 20
    val model = ALS.train(ratings,rank,numberIterations,0.01)
    // 评估数据模型
    val usersProducts = ratings.map{case Rating(user,product,rate) => (user, product)}
    val predictions = model.predict(usersProducts).map{case Rating(user,product,rate)=>((user, product),rate)}
    val ratesAndPreds = ratings.map{case Rating(user,products,rate)=>((user, products),rate)}.join(predictions)
    val MSE = ratesAndPreds.map{case ((user,product),(r1,r2))=>
      val err = (r1 - r2)
      err * err
    }.mean()
    println("Mean Squared Error="+MSE) // 用均方误差评估推荐模型
    //Save and load model
    model.save(sc, "myModel")
    val myModel = MatrixFactorizationModel.load(sc, "myModel")
    val alpha = 0.01
    val model2 = ALS.trainImplicit(ratings,rank,numberIterations,lambda = 0.001,alpha)

  }


}
