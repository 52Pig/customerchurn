package com.leaf.nn

import breeze.linalg.{CSCMatrix => BSM, DenseMatrix => BDM, DenseVector => BDV, Matrix => BM, SparseVector => BSV, Vector => BV}

import scala.collection.mutable.ArrayBuffer

/**配置参数 */
case class DBNConfig(
                      size: Array[Int],
                      layer: Int,
                      momentum: Double,
                      alpha: Double) extends Serializable

class DBNModel(
                val config: DBNConfig,
                val dbn_W: Array[BDM[Double]],
                val dbn_b: Array[BDM[Double]],
                val dbn_c: Array[BDM[Double]]) extends Serializable {

  /**
   * DBN模型转化为NN模型
   * 权重转换*/
  def dbnunfoldtonn(outputsize: Int): (Array[Int], Int, Array[BDM[Double]]) = {
    //1 size layer 参数转换
    val size = if (outputsize > 0) {
      val size1 = config.size
      val size2 = ArrayBuffer[Int]()
      size2 ++= size1
      size2 += outputsize
      size2.toArray
    } else config.size
    val layer = if (outputsize > 0) config.layer + 1 else config.layer

    //2 dbn_W 参数转换
    var initW = ArrayBuffer[BDM[Double]]()
    for (i <- 0 to dbn_W.length - 1) {
      initW += BDM.horzcat(dbn_c(i), dbn_W(i))
    }
    (size, layer, initW.toArray)
  }

}
