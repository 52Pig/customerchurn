package com.leaf.future

import breeze.linalg.{DenseVector => DV, SparseVector => DSV, Vector => DDV}
//import GD
import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LRMine {

  def main(args: Array[String]) {
    val data = readData()
    //(0,[-1.7917643361694628,-1.7945466673894237,-1.2686326993518091,-0.7189969073078432,-0.43633318808699484,-0.05464630422394534,-1.5289349033791129,-1.10680533081282,-3.180622888340963,-1.7326355811040044])
    val parsedData = parseData(data)
    parsedData.foreach(println)
//    generateModel(parsedData)
//    parsedData.take(1000).foreach(println)
  }

  def readData(): RDD[String] ={
    val conf = new SparkConf().setMaster("local[2]").setAppName("LRMine")
    val sc = new SparkContext(conf)
//    val data = sc.textFile("./src/main/scala/com/leaf/data/sample_libsvm_data.txt")
    val data = sc.textFile("./src/main/scala/com/leaf/data/lr_data.txt")
    data
  }

  def parseData(data:RDD[String]): RDD[(Int, Vector)] = {
    val flagAndFeatures = data.map(_.split(' ')).map{line =>
      val flag = if(line(0).toInt == -1) 0 else 1
      val features = Vectors.dense(line.tail.map(java.lang.Double.parseDouble))
      (flag,features)
    }
    flagAndFeatures
  }

  def generateModel(parsedData : RDD[(Int, Vector)]): Unit ={
    val intercept : Double = 0.0
    val numIterations = 3000
    val threshold : Double = 0.5
    val numFeatures = parsedData.map{case (flag, features)=> features }.first().size
//    val initialWeights = DenseVector.fill(numFeatures)(0.5)
    val w = Vectors.dense(new Array[Double](numFeatures))
//    println(initialWeights.t)
    val numExams = parsedData.count()
    // 权重迭代计算
    for( i<- 1 to numIterations){
      val bcWeights = parsedData.context.broadcast(w)

      //计算每个样本的权重向量、误差值，然后对所有样本权重向量及误差值进行累加
      parsedData.map{case (flag, features)=>
//      val linearSum : Double = initialWeights dot features
        val linearSum = dot(w,features) + intercept

        val n = w.size

        val (gradientSum, lossSum, miniBatchSize) = parsedData.treeAggregate(DV.zeros[Double](n), 0.0, 0L)(
          seqOp = (c, v) =>{
//            val cc = c._1 //DenseVector[Double] breeze
            // c: (grad, loss, count) v: (label, features)
            val l = GD.compute(v._2, v._1,w,new DenseVector(c._1.toArray)) //data: Vector,label: Double,weights: Vector,cumGradient: Vector
            (c._1, c._2 + 1, c._3 + 1)
          },
          combOp = (c1, c2) => {
            // c: (grad, loss, count)
            (c1._1 += c2._1, c1._2 + c2._2, c1._3 + c2._3)
          }
        )
    }





//        w = w + 0.001 * (flag - linearSum) * features

//        val score = 1.0 / (1.0 + math.exp(-linearSum))
//        val a = if(score > threshold) 1.0 else 0.0
//       val linearSum = -1 * (initialWeights dot features)
//       val multipler = getSigmoid(linearSum) - flag
//       multipler * features.t
//
//
//        var theta += (1/2 * multipler * features.t)


    }
//    val cc = (parsedData, initialWeights)
//    LogisticRegressionWithSGD





  }

/*  def compute(data:Vector,label:Double,weight:Vector):(Vector,Double) = {
    val gradient = Vectors.zeros(weight.size)
    val loss = compute(data)
  }*/

  def getSigmoid(z : Double): Double ={
    return 1.0 / (1.0 + Math.exp(-z))
  }

  def dot(x : Vector, y : Vector) : Double = {
    require(x.size == y.size)
    (x, y) match {
      case (ax:DenseVector,ay:DenseVector) => dot(ax, ay)
      case _ => throw new IllegalArgumentException("dot error!")
    }

  }

}
