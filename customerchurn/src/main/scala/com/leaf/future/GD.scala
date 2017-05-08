package com.leaf.future

import org.apache.spark.mllib.linalg.{DenseVector, Vector}

object GD {

  def compute(
               data: Vector,
               label: Double,
               weights: Vector,
               cumGradient: Vector): Double = {
    val dataSize = data.size
    // marginY is margins(label - 1) in the formula.
    var marginY = 0.0
    var maxMargin = Double.NegativeInfinity
    var maxMarginIndex = 0

    val weightsArray = weights match {
      case dv: DenseVector => dv.values
      case _ =>
        throw new IllegalArgumentException(
          s"weights only supports dense vector but got type ${weights.getClass}.")
    }

    val cumGradientArray = cumGradient match {
      case dv: DenseVector => dv.values
      case _ =>
        throw new IllegalArgumentException(
          s"cumGradient only supports dense vector but got type ${cumGradient.getClass}.")
    }

    val margins = Array.tabulate(2) { i =>
      var margin = 0.0
      data.foreachActive { (index, value) =>
        if (value != 0.0) margin += value * weightsArray((i * dataSize) + index)
      }
      if (i == label.toInt - 1) marginY = margin
      if (margin > maxMargin) {
        maxMargin = margin
        maxMarginIndex = i
      }
      margin
    }

    /**
      * When maxMargin > 0, the original formula will cause overflow as we discuss
      * in the previous comment.
      * We address this by subtracting maxMargin from all the margins, so it's guaranteed
      * that all of the new margins will be smaller than zero to prevent arithmetic overflow.
      */
    val sum = {
      var temp = 0.0
      if (maxMargin > 0) {
        for (i <- 0 until 2) {
          margins(i) -= maxMargin
          if (i == maxMarginIndex) {
            temp += math.exp(-maxMargin)
          } else {
            temp += math.exp(margins(i))
          }
        }
      } else {
        for (i <- 0 until 2) {
          temp += math.exp(margins(i))
        }
      }
      temp
    }

    for (i <- 0 until 2) {
      val multiplier = math.exp(margins(i)) / (sum + 1.0) - {
        if (label != 0.0 && label == i + 1) 1.0 else 0.0
      }
      data.foreachActive { (index, value) =>
        if (value != 0.0) cumGradientArray(i * dataSize + index) += multiplier * value
      }
    }

    val loss = if (label > 0.0) math.log1p(sum) - marginY else math.log1p(sum)

    if (maxMargin > 0) {
      loss + maxMargin
    } else {
      loss
    }

  }

}
