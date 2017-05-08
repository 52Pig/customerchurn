package com.leaf.nn

import java.util
import java.lang.{Double => JavaDouble, Integer => JavaInteger, Iterable => JavaIterable}
import java.util.Random


//import scala.annotation.varargs
//import scala.collection.JavaConverters._
//import breeze.linalg.{Matrix => mt, DenseVector => BDV, SparseVector => BSV, Vector => BV}
//import org.json4s.DefaultFormats
//import org.json4s.JsonDSL._
//import org.json4s.jackson.JsonMethods.{compact, render, parse => parseJson}
//import org.apache.spark.SparkException
//import org.apache.spark.annotation.{AlphaComponent, Since}
//import org.apache.spark.mllib.util.NumericParser
//import org.apache.spark.sql.catalyst.InternalRow
//import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
//import org.apache.spark.sql.catalyst.util.GenericArrayData
//import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object BPSelf {

  def sigmoid(x: Double): Double = {
    1.0 / (1.0 + math.exp(-x))
  }

  var inputHiddenWights: Matrix = _
  var hiddenOutputWights: Matrix = _

  var eta = 0.1
  var numItr = 100000
  var theta = 1.0

  def forward(forwardLayer: Vector,
              weights: Matrix): Vector = {
    val bDvFL = forwardLayer.toBreeze.toDenseVector
    val bDFL = bDvFL.toDenseMatrix
    val bDW = weights.toBreeze.toDenseMatrix
    val sum = bDW * bDFL.t :+ theta
    val bDBL = sum.map(sigmoid)
    Vectors.fromBreeze(bDBL.toDenseVector)
  }

  def updateWeights(weights: Matrix,
                    gradient: Matrix) = {
    val bW = weights.toBreeze.toDenseMatrix
    val bG = gradient.toBreeze.toDenseMatrix
    //bG.foreachPair((x,y)=>println(x,y))
    bW -= bG :* eta
    Matrices.fromBreeze(bW)
  }

  def outputGradient(hiddenLayer: Vector,
                     outputLayer: Vector,
                     teacherSignal: Vector) = {
    val bHL = hiddenLayer.toBreeze.toDenseVector.toDenseMatrix
    val bOL = outputLayer.toBreeze.toDenseVector
    val bTS = teacherSignal.toBreeze.toDenseVector
    val optDelta = (bOL - bTS) :* bOL :* (1d - bOL)
    val dmOD = optDelta.toDenseMatrix
    val gradient = dmOD.t * bHL
    (Matrices.fromBreeze(gradient), Vectors.fromBreeze(optDelta))
  }

  def hiddenGradient(inputLayer: Vector,
                     forwardLayer: Vector,
                     backwardWeights: Matrix,
                     backwardDelta: Vector) = {
    val bFL = forwardLayer.toBreeze.toDenseVector.toDenseMatrix
    val bBW = backwardWeights.toBreeze.toDenseMatrix
    val bBD = backwardDelta.toBreeze.toDenseVector.toDenseMatrix
    val bIL = inputLayer.toBreeze.toDenseVector.toDenseMatrix
    val sum = bBW.t * bBD.t
    val forwardDelta = bFL :* (1d - bFL) :* sum.t
    val gradient = forwardDelta.t * bIL
    (Matrices.fromBreeze(gradient), Vectors.fromBreeze(forwardDelta.toDenseVector))
  }

  def seqOp(gradient: (Matrix, Matrix, Double, Double),
            data: (Matrix, Matrix, Double)) = {
    val optGradient = gradient._1.toBreeze
    val hidGradient = gradient._2.toBreeze
    val optData = data._1.toBreeze
    val hidData = data._2.toBreeze
    val comOptGradient = Matrices.fromBreeze(optGradient + optData)
    val comHidGradient = Matrices.fromBreeze(hidGradient + hidData)
    (comOptGradient, comHidGradient, gradient._3 + 1, gradient._4 + data._3)
  }

  def combOp(delta1: (Matrix, Matrix, Double, Double), delta2: (Matrix, Matrix, Double, Double)) = {
    val optHidMatrix = Matrices.fromBreeze(delta1._1.toBreeze.+(delta2._1.toBreeze))
    val hidIptMatrix = Matrices.fromBreeze(delta1._2.toBreeze.+(delta2._2.toBreeze))
    (optHidMatrix, hidIptMatrix, delta1._3 + delta2._3, delta1._4 + delta2._4)
  }

  def predict(inputLayer: Vector) = {
    val hiddenLayer = forward(inputLayer, inputHiddenWights)
    val outputLayer = forward(hiddenLayer, hiddenOutputWights)
    outputLayer
  }

  def train(data: RDD[LP], eta: Double, nItr: Int) = {
    this.eta = eta
    this.numItr = nItr

    val numFeatures = data.first.features.size
    val numOutputs = 1
    val numHidden = math.sqrt(numFeatures + numOutputs).toInt + 1
    val rng = new Random(1)
    inputHiddenWights = DenseMatrix.rand(numHidden, numFeatures,rng)
    hiddenOutputWights = DenseMatrix.rand(numOutputs, numHidden,rng)
    var outputError = Double.MaxValue

    var i = 0
    while (i < numItr & outputError > 0.01) {
      val (optGradientSum, hidGradientSum, count, errorSum) =
        data.map(x => {
          val teacherSignal = Vectors.dense(Array[Double](x.label))
          val cc = x.features
          val hiddenLayer = forward(x.features, inputHiddenWights)
          val outputLayer = forward(hiddenLayer, hiddenOutputWights)
          val errorVector = outputLayer.toBreeze - teacherSignal.toBreeze
          outputError = errorVector.fold(0.0)(math.pow(_, 2) + math.pow(_, 2))
          println(outputError)
          val optGradient = outputGradient(hiddenLayer, outputLayer, teacherSignal)
          val hidGradient = hiddenGradient(x.features, hiddenLayer, hiddenOutputWights, optGradient._2)
          (optGradient._1, hidGradient._1, outputError)
        }).treeAggregate(Matrices.zeros(numOutputs, numHidden),
          Matrices.zeros(numHidden, numFeatures), 0.0, 0.0)(seqOp, combOp)

      outputError = errorSum / count
      val avgOptGradient = optGradientSum.toBreeze.toDenseMatrix :/ count
      val avgHidGradient = hidGradientSum.toBreeze.toDenseMatrix :/ count
      inputHiddenWights = updateWeights(inputHiddenWights, Matrices.fromBreeze(avgHidGradient))
      hiddenOutputWights = updateWeights(hiddenOutputWights, Matrices.fromBreeze(avgOptGradient))
      i += 1
    }
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("BP").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val tData = sc.textFile("E:\\b.txt")

    val data = tData.map { line =>
      val parts = line.split("\t")
      val ab = new ArrayBuffer[Double]
      LP(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache
    train(data, eta, numItr)
    data.sortBy(_.label).foreach(x =>
      println(x.label + "\t" + predict(x.features)))
    println(predict(Vectors.dense(Array[Double](0.0,1.0,1.0,1.0))))
  }

}
