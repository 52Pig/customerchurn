package com.leaf.matrix

import org.jblas.DoubleMatrix

object JBlasMatrix {

  def main(args: Array[String]): Unit = {
    val matrix1 = DoubleMatrix.ones(10,1) //创建所有值为1的10*1矩阵
    val matrix2 = DoubleMatrix.zeros(10,1) //创建所有值为0的10*1矩阵
    //     matrix1.put(1,-10)

    //     val absSum = matrix1.norm1() //绝对值之和
    //     val euclideNorm = matrix1.norm2() //欧几里德距离
    val matrix3 = (matrix1.addi(matrix2))
       val matrix4 = new DoubleMatrix(1,10,(1 to 10).map(_.toDouble):_*)  //创建Double的向量对象

    println("matrix1:" +matrix1)
    println("matrix2:" +matrix2)
    println("matrix3:" +matrix3)
    println("print init value:matrix3="+matrix3)
    println("print init value:matrix4="+matrix4)
    println("matrix sub matrix:"+matrix3.sub(matrix4)+","+matrix4.sub(10))
    println("matrix add matrix:"+matrix3.add(matrix4)+","+matrix4.add(10))
    println("matrix mul matrix:"+matrix3.mul(matrix4)+","+matrix4.mul(10))
    println("matrix div matrix:"+matrix3.div(matrix4)+","+matrix4.div(10))
    println("matrix dot matrix:"+matrix3.dot(matrix4)) //向量积

    val matrix5 = DoubleMatrix.ones(10,10)
    println("N*M Vector Matrix sub OP:" +matrix5.subRowVector(matrix4) + ","+matrix5.subColumnVector(matrix4)) //多对象减法运算

  }
}
