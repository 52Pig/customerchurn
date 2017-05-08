package com.leaf.exams

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

object ALSExam {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ALS Test").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.textFile("E:\\spark\\spark-1.1.1\\spark-1.1.1\\data\\mllib\\als\\test.data")
    val ratings = data.map(_.split(',') match {
      case Array(user,item,rate) => Rating(user.toInt,item.toInt,rate.toDouble)
    })

    val numIterations = 20
    val model = ALS.train(ratings,1,numIterations,0.01)
    //对推荐模型进行评分
    val usersProducts = ratings.map{
      case Rating(user,product,rate) => (user,product)
    }
    val predictions = model.predict(usersProducts).map{
      case Rating(user,product,rate) => ((user,product),rate)
    }

    val ratinsAndPreds = ratings.map{
      case Rating(user,product,rate) => ((user,product) ,rate)
    }
    /*ratinsAndPreds.join(predictions)

    val MSE = ratesAndPreds.map{
      case((user,product),(r1,r2)) => math.pow((r1- r2), r2)
    }.reduce(_+_)/ratesAndPres.count

    println("Mean Squared Error = "+MSE)*/
  }
}
