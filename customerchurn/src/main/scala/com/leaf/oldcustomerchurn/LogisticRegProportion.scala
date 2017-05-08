package com.leaf.oldcustomerchurn

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.tree.{RandomForest, DecisionTree}

object LogisticRegProportion {
  def main(args: Array[String]) {
//    testAll()
//    test3_1()
//    test_4_1()
//    test11(args(0))
    test11()
  }

  def testAll(): Unit ={
    val sc = Utils.initial("logistic regregasstion",true)
    /*val t = sc.parallelize(Array(1,2,3,4,5))
    t.saveAsTextFile("file:///D:/a/a.txt")
    sc.stop()*/

    val data = sc.textFile("E:/items/items/sample_400_all")
    val parsedData = Utils.parsedData(data)
    /*val parsedData = data.map { line =>
      val parts = line.split('\t').map(word=>
        if(word.equals("c")||word.equals("b"))
          2
        else if(word.equals("a"))
          1
        else word.toDouble
      )
      LabeledPoint(parts(0) , Vectors.dense(parts.tail))
    }*/
    val splits = parsedData.randomSplit(Array(0.6,0.4),seed = 11L)
    val (training,test) = (splits(0),splits(1))

//    val model = new LogisticRegressionWithLBFGS().setNumClasses(2)
//      .run(training)
//    val model = LinearRegressionWithSGD.train(training,20)
    val model = NaiveBayes.train(training, lambda = 1.0)   //Additive smoothing的值为1.0（默认值）

    val labelsAndPreds = test.map{point =>
      val prediction = model.predict(point.features)
      (point.label,prediction)
    }
//    println(1-labelsAndPreds.filter(r=>r._1!=r._2).count.toDouble/parsedData.count)   //0.8891

    //P正元组数   N负元组数
    val P = labelsAndPreds.filter(r=>r._1==2).count
    val N = labelsAndPreds.filter(r=>r._1==1).count
    val PN = test.count

    println("P:"+P)
    println("N:"+N)
    println("PN:"+PN)

    val TP = labelsAndPreds.filter(r=>r._1==2&&r._2==2).count.toDouble
    val TN = labelsAndPreds.filter(r=>r._1==1&&r._2==1).count.toDouble
    val FP = labelsAndPreds.filter(r=>r._1==1&&r._2==2).count.toDouble
    val FN = labelsAndPreds.filter(r=>r._1==2&&r._2==1).count.toDouble

    println("TP:" + TP)
    println("TN:" + TN)
    println("FP:" + FP)
    println("FN:" + FN)

    val accuracy = (TP + TN)/PN
    val precision = TP/(TP+FP)
    val recall = TP/P

    //准确率
    println("accuracy:" + accuracy)
    //精度
    println("precision:" + precision)
    //召回率
    println("recall:" + recall)
  }

  def test3_1(): Unit ={
    val sc = Utils.initial("naivebayes test 3:1",true)
    val data = sc.textFile("E:/items/items/sample_400_1_1")
    val validData = Utils.parsedData(data)
//    val model = LinearRegressionWithSGD.train(validData,10)   //得不到结果
//    val model = LogisticRegressionWithSGD.train(validData,10)
//    val model = SVMWithSGD.train(validData,10)
//    val model = NaiveBayes.train(validData, lambda = 1.0)
    val model = DecisionTree.trainClassifier(validData,3,Map[Int,Int](),"gini",5,32)
//    val model = RandomForest.trainClassifier(validData,3,Map[Int,Int](),10,"auto","gini",8,32)

    /*val labelAndPreds = validData.map{point=>
      val prediction = model.predict(point.features)
      (point.label,prediction)
    }*/

    //P正元组数   N负元组数
    /*val P = labelAndPreds.filter(r=>r._1==1).count
    val N = labelAndPreds.filter(r=>r._1==0).count
    val PN = validData.count

    println("P:"+P)
    println("N:"+N)
    println("PN:"+PN)

    val TP = labelAndPreds.filter(r=>r._1==1&&r._2==1).count.toDouble  //被分类器正确分类的正元组
    val TN = labelAndPreds.filter(r=>r._1==0&&r._2==0).count.toDouble //被分类器正确分类的负元组
    val FP = labelAndPreds.filter(r=>r._1==0&&r._2==1).count.toDouble  //被错误地标记为正元组的负元组
    val FN = labelAndPreds.filter(r=>r._1==1&&r._2==0).count.toDouble //被错误地标记为负元组的正元组

    println("TP:" + TP)
    println("TN:" + TN)
    println("FP:" + FP)
    println("FN:" + FN)

    val accuracy = (TP + TN)/PN
    val precision = TP/(TP+FP)
    val recall = TP/P

    //准确率
    println("accuracy:" + accuracy)
    //精度
    println("precision:" + precision)
    //召回率
    println("recall:" + recall)*/

    val data1 = sc.textFile("E:/items/items/sample_400_all1")
    val validData1 = Utils.parsedData(data1)

    val labelAndPreds1 = validData1.map{point=>
      val prediction = model.predict(point.features)
      (point.label,prediction)
    }

    //P正元组数   N负元组数
    val P1 = labelAndPreds1.filter(r=>r._1==1).count
    val N1 = labelAndPreds1.filter(r=>r._1==0).count
    val PN1 = validData1.count

    println("P1:"+P1)
    println("N1:"+N1)
    println("PN1:"+PN1)

    val TP1 = labelAndPreds1.filter(r=>r._1==1&&r._2==1).count.toDouble  //被分类器正确分类的正元组
    val TN1 = labelAndPreds1.filter(r=>r._1==0&&r._2==0).count.toDouble //被分类器正确分类的负元组
    val FP1 = labelAndPreds1.filter(r=>r._1==0&&r._2==1).count.toDouble  //被错误地标记为正元组的负元组
    val FN1 = labelAndPreds1.filter(r=>r._1==1&&r._2==0).count.toDouble //被错误地标记为负元组的正元组

    println("TP1:" + TP1)
    println("TN1:" + TN1)
    println("FP1:" + FP1)
    println("FN1:" + FN1)

    val accuracy1 = (TP1 + TN1)/PN1
    val precision1 = TP1/(TP1+FP1)
    val recall1 = TP1/P1

    //准确率
    println("accuracy1:" + accuracy1)
    //精度
    println("precision1:" + precision1)
    //召回率
    println("recall1:" + recall1)

    val F = (2*precision1*recall1)/(precision1+recall1)
    println("F:"+F)

  }

  def test_4_1(): Unit ={
    val sc = Utils.initial("naivebayes test 4:1",true)
    val data = sc.textFile("E:/items/items/sample_400_4_1")
    val validData = Utils.parsedData(data)
    val model = NaiveBayes.train(validData, lambda = 1.0)
    val labelAndPreds = validData.map{point=>
      val prediction = model.predict(point.features)
      (point.label,prediction)
    }

    //P正元组数   N负元组数
    val P = labelAndPreds.filter(r=>r._1==2).count
    val N = labelAndPreds.filter(r=>r._1==1).count
    val PN = validData.count

    println("P:"+P)
    println("N:"+N)
    println("PN:"+PN)

    val TP = labelAndPreds.filter(r=>r._1==2&&r._2==2).count.toDouble  //被分类器正确分类的正元组
    val TN = labelAndPreds.filter(r=>r._1==1&&r._2==1).count.toDouble //被分类器正确分类的负元组
    val FP = labelAndPreds.filter(r=>r._1==1&&r._2==2).count.toDouble  //被错误地标记为正元组的负元组
    val FN = labelAndPreds.filter(r=>r._1==2&&r._2==1).count.toDouble //被错误地标记为负元组的正元组

    println("TP:" + TP)
    println("TN:" + TN)
    println("FP:" + FP)
    println("FN:" + FN)

    val accuracy = (TP + TN)/PN
    val precision = TP/(TP+FP)
    val recall = TP/P

    //准确率
    println("accuracy:" + accuracy)
    //精度
    println("precision:" + precision)
    //召回率
    println("recall:" + recall)
  }

  def test11(): Unit ={
    Utils.setStreamingLogLevels()

    val sc = Utils.initial("RF_local_test",true)
//    val data = sc.textFile("E:/items/items/new/sam_train_6w4.txt")
//    val data = sc.textFile("E:\\items\\items\\new\\decision_tree_classification_test_6.txt")
    val data = sc.textFile("E:\\items\\items\\new\\result_train_model")
//    val data = sc.textFile("E:\\items\\items\\new\\result_view_model")

//    val data = sc.textFile("E:/items/items/new/train_three_model")
//      val data = sc.textFile("E:/items/items/new/three_feature")
//    val data = sc.textFile("E:/items/items/new/train_three_feature_model")//qi
//    val data = sc.textFile("file:///home/mobile/liangfeilong/three/sample_300_test")
    val validData = Utils.parsedData(data)
//    val splits = validData.randomSplit(Array(0.6,0.4))
//    val (training,test) = (splits(0),splits(1))
//        val model = LinearRegressionWithSGD.train(training,10)   //得不到结果
//        val model = LogisticRegressionWithSGD.train(training,10)
//        val model = SVMWithSGD.train(training,10)
//        val model = NaiveBayes.train(training, lambda = 1.0)
//        val model = DecisionTree.trainClassifier(validData,2,Map[Int,Int](),"gini",5,32)
//    validData.cache()
    val model = RandomForest.trainClassifier(validData,2,Map[Int,Int](),10,"auto","gini",10,32)

//    println(model.toDebugString)
    // Train a GradientBoostedTrees model.
//    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
//    boostingStrategy.setNumIterations(56) // Note: Use more in practice
//    val model = GradientBoostedTrees.train(validData,boostingStrategy)

//    val test = sc.textFile("E:/items/items/sample_300_reals")
//    val scs = Utils.initial("RF_test",true)
//    val test = sc.textFile("E:\\items\\items\\new\\pre_realss.txt")
//    val test = sc.textFile("E:\\items\\items\\new\\pred_model_data")
    val test = sc.textFile("E:\\items\\items\\new\\sam_train_result.txt")
//    val test = sc.textFile("E:\\items\\items\\new\\three_model")
//    val testData = Utils.parsedHDFSData(test)
    val testData = Utils.parsedData(test)
//    testData.cache()
    val labelAndPreds = testData.map{point=>
      val prediction = model.predict(point.features)
//      print(point.label)
      (point.label,prediction)
    }

    //P正元组数   N负元组数
    val P = labelAndPreds.filter(r=>r._1==1).count
    val N = labelAndPreds.filter(r=>r._1==0).count
//    val PN = validData.count

//    println("P:"+P)
//    println("N:"+N)
//    println("PN:"+PN)

    val TP = labelAndPreds.filter(r=>r._1==1&&r._2==1).count.toDouble  //被分类器正确分类的正元组
    val TN = labelAndPreds.filter(r=>r._1==0&&r._2==0).count.toDouble //被分类器正确分类的负元组
    val FP = labelAndPreds.filter(r=>r._1==0&&r._2==1).count.toDouble  //被错误地标记为正元组的负元组
    val FN = labelAndPreds.filter(r=>r._1==1&&r._2==0).count.toDouble //被错误地标记为负元组的正元组

    println("\nTP:" + TP)
    println("TN:" + TN)
    println("FP:" + FP)
    println("FN:" + FN)

    val accuracy = (TP + TN)/(P+N)
    val precision = TP/(TP+FP)
    val recall = TP/P

    //准确率
    println("accuracy:" + accuracy)
//    println("accuracy1:" + ((TP + TN)/validData.count))
//    println("accuracy2 " + (labelAndPreds.filter(r=>r._1==r._2).filter(x=>x._1==1).count.toDouble/validData.count))
    //精度
    println("precision:" + precision)
    //召回率
    println("recall:" + recall)

    val F = (2 * precision * recall) / (precision + recall)
    println("F:"+F)
//    val TPR = TP/P
//    val FPR = FP/N
//    println("TPR:" + TPR)
//    println("FPR:" + FPR)
//    print(model.toDebugString)
  }

}
