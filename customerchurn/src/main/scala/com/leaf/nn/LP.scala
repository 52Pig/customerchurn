package com.leaf.nn

import scala.beans.BeanInfo
import org.apache.spark.SparkException

/**
  * Class that represents the features and labels of a data point.
  *
  * @param label Label for this data point.
  * @param features List of features for this data point.
  */
@BeanInfo
case class LP  (label: Double,features: Vector) {
  override def toString: String = {
    s"($label,$features)"
  }
}


object LP {

  def parse(s: String): LP = {
    if (s.startsWith("(")) {
      NumericParser.parse(s) match {
        case Seq(label: Double, numeric: Any) =>
          LP(label, Vectors.parseNumeric(numeric))
        case other =>
          throw new SparkException(s"Cannot parse $other.")
      }
    } else { // dense format used before v1.0
    val parts = s.split(',')
      val label = java.lang.Double.parseDouble(parts(0))
      val features = Vectors.dense(parts(1).trim().split(' ').map(java.lang.Double.parseDouble))
      LP(label, features)
    }
  }
}
