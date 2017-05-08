package com.leaf.oldcustomerchurn

object SGDDemo {
  def main(args: Array[String]) {
    val featuresMatrix : List[List[Double]] = List(List(1,4),List(2,5),List(5,1),List(4,2))   //特征矩阵
    val labelMatrix : List[Double] = List(19,26,19,20)  //真实值向量

    var theta:List[Double] = List(0, 0)
    var loss :Double = 10.0
    for{
      i <- 0 until 1000   //迭代次数
      if(loss>0.01)      //收敛条件loss<=0.01
    } {
      var error_sum = 0.0    //总误差
      var j = i % 4
      var h = 0.0
      for(k <- 0 until 2){
        h+= featuresMatrix(j)(k) * theta(k)
      }  //计算给出的测试数据集中第j个对象的计算类标签
      error_sum = labelMatrix(j) - h
      //计算给出的测试数据集中类标签与计算的类标签的误差值
      var cacheTheta : List[Double] = List()
      for(k<- 0 until 2){
        val updaterTheta = theta(k) + 0.001 * (error_sum) * featuresMatrix(j)(k)
        cacheTheta = updaterTheta +: cacheTheta
      }    //更新权重向量
      cacheTheta.foreach(t=>print(t + ","))
      print(error_sum + "")
      theta = cacheTheta
      //更新误差率
      var currentLoss : Double = 0
      for(j<- 0 until 4){
        var sum = 0.0
        for(k<- 0 until 2){
          sum+=featuresMatrix(j)(k) * theta(k)
        }
        currentLoss += (sum - labelMatrix(j)) * (sum - labelMatrix(j))
      }
      loss = currentLoss
      println("loss>>>>>"+loss/4 +",i>>>>>"+i)
    }
  }
}
