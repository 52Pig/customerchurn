package com.leaf.statistics

import com.leaf.oldcustomerchurn.Utils
import org.apache.spark.mllib.stat.Statistics

object StatisExam {
  def main(args: Array[String]): Unit = {
  }

  def getStat(): Unit ={
    val sc = Utils.initial("Statistics",true)
    val data = sc.textFile("E:/items/items/sample_1cols")
    val validData = Utils.parsedData2(data)

    val summary = Statistics.colStats(validData)
    println("mean:"+summary.mean)
    println("variance:"+summary.variance)
    println("非零统计量个数:"+summary.numNonzeros)
    println("最大值:"+summary.max)
    println("最小值:"+summary.min)
  }
}
