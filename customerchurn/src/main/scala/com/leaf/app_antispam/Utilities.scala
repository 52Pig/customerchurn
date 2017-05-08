package com.leaf.app_antispam

import breeze.linalg.{DenseMatrix, DenseVector, inv, sum}
import breeze.numerics.pow
import org.apache.spark.sql.functions.udf
import breeze.stats._

object Utilities {

  def factorial(n: Double): Double = n match {
    case 0 => 1
    case _ => n * factorial(n-1)
  }

  def probEst(lambda: Double, x: Double): Double = {
    // lambda是parameter，x是value，若分母n!为无穷，则概率返回0。
    val denom = factorial(x)
    val num = math.pow(lambda, x) * math.exp(-lambda)
    val prob = num/denom
    if (prob.isInfinite || prob.isNaN || prob < 0.0) {
      return 0.0
    } else {
      return prob
    }
  }

  def poissonZ(p: Double, v: Double): Double = {
    // Transformed Poisson Distribution: z = w*exp(w)
    // 计算vv对应的变换后的 z 值
    val f = factorial(v)
    if (f.isInfinite) {
      return Double.NegativeInfinity
    } else {
      val pf = p * f
      val epf = math.pow(pf, 1/v)
      val z = -epf/v
      return z
    }
  }

  // Lambert W Function 后面三个函数都是，从 GSL 改写而来。
  // 代码主要是在处理可能出现的特殊值，w本身的值用得是近似公式。
  def halleyIteration(z: Double, w0: Double, imax: Integer): (Double, Double) = {
    val EPSILON = Double.MinPositiveValue
    var w = w0
    var e = math.exp(w)
    var p = w + 1.0
    var t = w*e - z
    var tol = math.abs(t)
    var i = 1
    while (i <= imax && math.abs(t) >= tol) {
      e = math.exp(w)
      p = w + 1.0
      t = w*e - z
      if (w > 0) {
        t = (t/p)/e
      } else {
        t /= e*p - 0.5*(p + 1.0)*t/p
      }
      w -= t
      tol = 10 * EPSILON * math.max(math.abs(w), 1.0/(math.abs(p)*e))
      i += 1
    }
    if (i < imax) {
      return (w, 2.0*tol)
    } else {
      return (w, math.abs(w))
    }
  }

  def seriesEval(r: Double): Double = {
    val c: List[Double] = List( -1.0,
      2.331643981597124203363536062168,
      -1.812187885639363490240191647568,
      1.936631114492359755363277457668,
      -2.353551201881614516821543561516,
      3.066858901050631912893148922704,
      -4.175335600258177138854984177460,
      5.858023729874774148815053846119,
      -8.401032217523977370984161688514,
      12.250753501314460424,
      -18.100697012472442755,
      27.029044799010561650 )
    val t8 = c(8) + r*(c(9) + r*(c(10) + r*c(11)))
    val t5 = c(5) + r*(c(6) + r*(c(7)  + r*t8))
    val t1 = c(1) + r*(c(2) + r*(c(3)  + r*(c(4) + r*t5)))
    return c(0) + r*t1
  }

  def lambertW0(z: Double): (Double, Double) = {
    val EPSILON = Double.MinPositiveValue
    val ie = 1/math.E
    val q = z + ie
    var w = q
    var err = q
    if (z.isInfinite) {
      w = 0.0
      err = Double.NaN
    } else if (z == 0.0) {
      w = 0.0
      err = 0.0
    } else if (q < 0.0) {
      w = 0.0
      err = math.sqrt(-q)
    } else if (q == 0.0) {
      w = -1.0
      err = EPSILON
    } else if (q < 1.0E-03) {
      val r = math.sqrt(q)
      w = seriesEval(r)
      err = 2.0 * EPSILON * math.abs(w)
    } else {
      val MAXITERS = 20
      var w0 = 0.0
      if (z < 1.0) {
        val p = math.sqrt(2.0 * math.E * q)
        w0 = -1.0 + p*(1.0 + p*(-1.0/3.0 + p*11.0/72.0))
      } else {
        w0 = math.log(z)
        if (z > 3.0) {
          w0 -= math.log(w0)
        }
      }
      val result = halleyIteration(z, w0, MAXITERS)
      w = result._1
      err = result._2
    }
    return (w, err)
  }

  // Linear Regression: THEORATICAL WAY
  def simpleLinear(lambda: DenseVector[Double], num: DenseVector[Double]): (Double, Double) = {
    val y = lambda(lambda :!= 0.0)
    val x = num(lambda :!= 0.0)
    val n = y.length
    val meany = mean(y)
    val meanx = mean(x)
    val sdy = stddev(y)
    val sdx = stddev(x)
    val cor = 1.0/(n-1.0) * sum(((x-meanx)/sdx) :* ((y-meany)/sdy))
    val beta = cor * sdy / sdx
    val alpha = meany - beta * meanx
    return (beta, alpha)
  }

  def multipleLinear(lambda: DenseVector[Double], r: DenseVector[Double], num: DenseVector[Double]): DenseVector[Double] = {
    val y = DenseVector(r(lambda :!= 0.0).toArray)
    val x = num(lambda :!= 0.0).toArray
    val n = x.length
    val one = DenseVector.ones[Double](n).toArray
    val X = DenseMatrix(one, x, pow(x,2), pow(x,3), pow(x,4)).t
    val Beta = inv(X.t * X) * X.t * y
    return Beta
  }

  // 用于 DataFrame 列操作，用几何平均合并新概率与原概率
  def likelihood(like: Double, power: Double, likenew: Double): Double = {
    math.pow(math.pow(like,power)*likenew, 1.0/(power+1.0))
  }

  val likelihoodUDF = udf(likelihood _)
}
