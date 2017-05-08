package com.leaf.matrix

import breeze.linalg.{*, DenseMatrix, SparseVector, DenseVector}
import breeze.numerics.{floor, ceil}
import breeze.stats.distributions.Poisson
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.ArrayBuffer


object App {
  def main(args: Array[String]) {
    val a = DenseVector.zeros[Double](20) // 创建列向量,向量初始值为0
    println(a)
    println("===============")
    val b = new DenseVector((1 to 20).toArray).map(_.toDouble)
    println(b)
    println(floor(b))
    println(ceil(b))
    println("===============")
    val c = a + b
//    val c1 = a dot b  //两个向量的点积
//    println(c1)
//    println(c)
    val d = new org.apache.spark.mllib.linalg.DenseVector(a.data)  //将breeze的DenseVector转成spark里linalg的DenseVector
//    println(d)
    val d1 = new breeze.linalg.DenseVector(d.values)   //将spark里linalg的DenseVector转成breeze的DenseVector
//    println(d1)
    val e1   = 4.0
    def dense(firstValue: Double, otherValues: Double*) = new DenseVector((firstValue +: otherValues).toArray)
//    println(dense(1.0,3.0,4.0,8.0,9.7))
    b(3 to 6) := .5     //从0开始将3到6包括6位置的元素替换成0.5
//    println(b)
    b(0 to 1) := DenseVector(.1,.2)  //将0和1位置的向量替换成0.1和0.2
//    println(b)

    val f = DenseMatrix.zeros[Int](5,5)
    println(f)
    println((f.rows,f.cols))
    println(f(::,1))
    println(f(4,::))
    f(4,::) := DenseVector(1,2,3,4,5).t  //将矩阵第5行替换成1,2,3,4,5
    println(f)
    println("===============")
    f(0 to 1,0 to 1) := DenseMatrix((3,1),(-1,-2)) //将矩阵第0行的第0-1列替换成3,1;将矩阵第1行的第0-1列替换成-1,-2
    println(f)
    println(floor(f))
    println("============================================================")
    val dm = DenseMatrix((1.0,2.0,3.0),(4.0,5.0,6.0))
    println(dm)
    /**
      * 1.0  2.0  3.0
      * 4.0  5.0  6.0
      */
    val ad = dm(::,*) + DenseVector(3.0,4.0)
    println(ad)
    /**
      * 4.0  5.0  6.0
      * 8.0  9.0  10.0
      */
    ad(::,*) := DenseVector(3.0,4.0)
    println(ad)
    /**
      * 3.0  3.0  3.0
      * 4.0  4.0  4.0
      */
    import breeze.stats.mean
    println(mean(dm(*,::)))  //mean函数求矩阵的平均值
    /**
      * DenseVector(2.0, 5.0)
      */
    val poi = new Poisson(3.0)
    val s = poi.sample(5)
    println(s)
    /**
      * Vector(2, 3, 2, 5, 2)
      */
    println("=========")
    println(s map {poi.probabilityOf(_)})
    /**
      * Vector(0.22404180765538775, 0.22404180765538775, 0.22404180765538775, 0.10081881344492458, 0.22404180765538775)
      */
    val s1 = for(i <- poi) yield i.toDouble
    println(s1)  //MappedRand(Poisson(3.0),<function1>)
    println(breeze.stats.meanAndVariance(s1.samples.take(1000))) //MeanAndVariance(2.9630000000000005,2.862493493493492,1000)
    println((poi.mean,poi.variance))  //(3.0,3.0)

    //    println(poi.sample(5))

    //    val e3 = (e1 +: e2:Double*).toArray
    //    val x = DenseVector(0.0,2.0,0.0,5.0,4.2)
//    val c = x(3 to 4)
//    := .5
//    println(c)
    /*val rand = new Random(5)
    val d = new DoubleMatrix(1,5,Array.fill[Double](5)(rand.nextGaussian()):_*)
    println(d.columns)
    println(d.rows)*/
    /*for(cc <- c){
      println(cc)
    }*/
//    d.fill(Array.fill[Double]())
//    loadData()
//    getSSModel(contents)
//    contents.foreach(println)
//    contents.map{x=>
//         val c = x._2

//    }
  }

  def loadData()  = {
      val conf = new SparkConf().setAppName("scalasvm").setMaster("local[2]")
      val sc = new SparkContext(conf)
//      val contents = sc.textFile("E:/spark/data/sample_svm_data.txt")
      val contents = sc.textFile("E:/spark/data/heart_scale")
//    /vol/lfl/spark-1.5.2/data/mllib/sample_svm_data.txt
      contents.map(_.split(" ")).map{x=>
        val c = x.inits
        c.foreach(println)
//  new IndexedRow(x(0).toInt,for(i<-1 to 12) yield {x(i)}.toVector )
      }

  }
//+1 1:0.708333 2:1 3:1 4:-0.320755 5:-0.105023 6:-1 7:1 8:-0.419847 9:-1 10:-0.225806 12:1 13:-1
  /*def getSSModel(rdd : RDD[String],numIters : Int): Unit ={
      var y = 0
      for(i <- 0 to numIters){
        rdd.map{contents=>
          if(contents(0).equals("+1")) y=1 else y = -1
          for(i <- 1 to 12){
            contents
            contents.split(":")
          }

        }
      }


  }*/

}



class SVMClazz{
    val x = Array.ofDim[Double](3,4)
    val y = ArrayBuffer[Int]()

    def apply(i: Int): Array[Double] = x.apply(i)

}
//(override val x : Array[Array[Double]],override val y : ArrayBuffer[Int] )
//class SVMClazz private ( val x : Array[Array[Double]]){
   /* val x
  val y*/


//}