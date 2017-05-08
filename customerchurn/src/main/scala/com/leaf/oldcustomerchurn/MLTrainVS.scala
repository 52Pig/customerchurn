package com.leaf.oldcustomerchurn

import org.apache.spark.mllib.tree.DecisionTree

object MLTrainVS {

    def main(args: Array[String]) {

//    val sc = Utils.initial("Decision_Tree_Test",true)
//    val data = sc.parallelize("1\t2\t3\t4".split("\t").drop(0)).foreach(println)
//    data.map{x=>x.dr}
//    getTrain

//    val data = sc.textFile("E:\\Items\\items\\new\\title.txt")
//    val data = sc.textFile("E:\\Items\\items\\new\\abc.txt")
//    data.map(lines=>lines.matches("^[\u4e00-\u9fa5]$")).foreach(println)
//    println(data.filter(lines=>lines.matches("[^\\x00-\\xff]+\\s?[0-9]+$")).count)  //82671
//    println(data.filter(lines=>lines.matches("[^\\x00-\\xff]+\\s?[^\\x00-\\xff]+\\s?[0-9]+$")).count)  //89342
//    println(data.filter(lines=>lines.matches("[^\\x00-\\xff]+\\s?[^\\x00-\\xff]+\\s?[^\\x00-\\xff]+\\s?[0-9]+$")).count)  //90510
//    println(data.filter(lines=>lines.matches("[^\\x00-\\xff]+\\s?[^\\x00-\\xff]+\\s?[^\\x00-\\xff]+\\s?[^\\x00-\\xff]+\\s?[0-9]+$")).count)  //87295
//    data.filter(lines=>lines.matches("[^\\x00-\\xff]+\\s?[^\\x00-\\xff]+\\s?[^\\x00-\\xff]+\\s?[0-9]+$"))
//    data.map(x=>x.substring(0,x.lastIndexOf(" ")).trim).foreach(println)
//     println(getMaxViewEpiSode(data))
    /*val rdd = getViewEpiSode(data,"min_2")
    rdd.foreach(println)*/
//    val rdd = getViewTotalEpi(data)
//    rdd.foreach(println)
//      getViewTotalEpi(data)

//      getTrain()
      getTrainNew()
  }



    def getTrainNew(): Unit ={
        Utils.setStreamingLogLevels()
        val sc = Utils.initial("Episode",true)
        val d = sc.textFile("E:\\items\\items\\data\\episode_found.txt")
        val filterData = Utils.parseMatchesNew(d)
        val filData = filterData.map{x=>((x(0),x(19)),x(15))}   //guid showname title
//        getVE(filData,"max_2")
        val viewLast2Epi = Utils.getVE(filData,"max_2")
        //    println(viewLast2Epi.count)    //27992
        //    viewLast2Epi.foreach(println)
        val viewHead2Epi = Utils.getVE(filData,"min_2")
        val viewFirstEpi = Utils.getVE(filData,"min")
        val viewLastEpi = Utils.getVE(filData,"max")

//        println("viewHead2Epi : " + viewHead2Epi.count)  //viewHead2Epi : 211742
//        println("viewFirstEpi : " + viewHead2Epi.count)  //viewFirstEpi : 211742
        //连续性观看
        val viewL2_H2 = viewLast2Epi.map{case (k,(v1,v2))=>((k,v1),v2)}.join(viewHead2Epi.map{case (k,(v1,v2))=>((k,v1),v2)})
        val subViewL2_H2 = viewL2_H2.map{case ((k1,k2),(v1,v2))=>((k1,k2),v1-v2+1)}

        val totalEpi = Utils.getVELEN(filData).distinct
//            println(totalEpi.count)   //211742

        val res_S = totalEpi.map{case (k,(v1,v2))=>((k,v1),v2-2)}
        //    res_S.foreach(println)
        val series = res_S.distinct.join(subViewL2_H2).map{case ((k1,k2),(v1,v2))=>if(v2<=0||v1<=2)(k1,k2,0.0)else(k1,k2,(v1-2)/v2)}
        //    series.foreach(println)
        //    series.map{case (k,v1,v2)=>(k,v2)}.reduceByKey(_+_).foreach(println)
        val avg_Series = series.map{case (k,v1,v2)=>(k,v2)}.groupByKey()
//        avg_Series.foreach(println)
        val avgSeries = avg_Series.map{x=>(x._1,x._2.sum/x._2.size)}
//        avgSeries.foreach(println)
//        val prev = sc.textFile("E:/Items/items/data/view_training.txt")
      val prev = sc.textFile("E:/Items/items/data/showid_head_400.txt")
        //    println(prev.count)     //6000000
        //    C.foreach(println)

        val kvPrevData = prev.map{x=>(x.split("\t")(0),x.split("\t").tail)}
        val kvPreRes = kvPrevData.leftOuterJoin(avgSeries)
        val kvPreResult = kvPreRes.map{case (k,(v1,v2))=>
            val vn = v2 match {
                case Some(n) => n
                case None => 0.0
            }
            (k,(v1,vn))
        }
        //    kvPreResult.foreach(println)
        val data1 = kvPreResult.map{case (x,(y,z))=> (y,z)}.map{case (x,y)=>x.head+"\t"+x.tail.mkString("\t")+"\t"+y}
        val trainData = data1.filter(x=>x.split("\t").last.toDouble != 0.0)
//        println(trainData.count)   //600 0000

        val splits = trainData.randomSplit(Array(0.6,0.4))

//        val A = splits(0).filter(x=>x.split("\t")(0)=="a").takeSample(false,427999,888L)
        val A = splits(0).filter(x=>x.split("\t")(0)=="a").takeSample(false,48345,888L)
        val B = splits(0).filter(x=>x.split("\t")(0)=="b").takeSample(false,20093,888L)
        val C = splits(0).filter(x=>x.split("\t")(0)=="c").takeSample(false,17815,888L)
        /*println(" A: "+A.count)   //A: 2554952
        println(" B: "+B.count)   //B: 724248
        println(" C: "+C.count)   //C: 320096
*/
        val datas = sc.parallelize(A++B++C)
        val trains = Utils.parsedData(datas)
        val test = Utils.parsedData(splits(1))
//        test.persist
        //    a.foreach(println)
        val model = DecisionTree.trainClassifier(trains,2,Map[Int,Int](),"gini",5,32)

//      val tesss = sc.textFile("E:\\items\\items\\data\\showid_tail_400.txt").map{x=>x.split("\t").drop(1).mkString("\t")}
//      val test = Utils.parsedData(tesss)

        val labelAndPreds = test.map{point=>
            val predict = model.predict(point.features)
            (point.label,predict)
        }.persist

        //P正元组数   N负元组数
        val P = labelAndPreds.filter(r=>r._1==1).count
        val N = labelAndPreds.filter(r=>r._1==0).count

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

    }

  def getTrain() : Unit ={
    Utils.setStreamingLogLevels()
    val sc = Utils.initial("Decision_Tree_Test",true)
    val data = sc.textFile("E:\\items\\items\\new\\active_train_attr_detail.txt")
//    println("all:"+data.count)  //all:1101652
//    println(data.filter(x=>x.split('\t')(20)=="电视剧").count())
//    val splits = data.randomSplit(Array(0.6,0.4))

    val classA = data.filter(x=>x.split("\t")(1)=="a")
    val classB = data.filter(x=>x.split("\t")(1)=="b")
    val classC = data.filter(x=>x.split("\t")(1)=="c")

    /*println("A : "+ classA.count+" B : "+classB.count + " C : " + classC.count)
    println("A+B+C : " +(classA ++ classB ++ classC).count)*/
    val training = Utils.parseMatches(classA ++ classB ++ classC)
//    println(training.count)
    /*println("A : "+Utils.parsedData(classA).count())
    println("B : "+Utils.parsedData(classB).count())
    println("C : "+Utils.parsedData(classC).count())*/
//    println(training.foreach(arrays=>for( a  <- arrays){  a}))

//    val epi = training.map(x=>x(22).toDouble)     //200\u96c6

 //    guid_content.take(10).foreach(println)
 //    getViewEpiSodeReal(content,"max_2")
     /*println("max_2 : " + getViewEpiSodeRealKV(guid_content,"max_2").count)
     println("max : " + getViewEpiSodeRealKV(guid_content,"max").count)
     println("min : " + getViewEpiSodeRealKV(guid_content,"min").count)
     println("min_2 : " + getViewEpiSodeRealKV(guid_content,"min_2").count)*/
    val guid_content = training.map{x=>(x(0),x(15))}   //剧集内容
//    println(guid_content.count) //376941
    val viewLast2Epi = Utils.getViewEpiSodeRealKV(guid_content,"max_2")
//    println(viewLast2Epi.count)    //27992
//    viewLast2Epi.foreach(println)
    val viewHead2Epi = Utils.getViewEpiSodeRealKV(guid_content,"min_2")
    val viewFirstEpi = Utils.getViewEpiSodeRealKV(guid_content,"min")
    val viewLastEpi = Utils.getViewEpiSodeRealKV(guid_content,"max")

    //连续性观看
//    viewLast2Epi.filter{case (k,(v1,v2))=> k=="3b49c1bdeb9d8c3cf6852fb45ddd94aa"&&v1=="一不小心爱上你"}.foreach(println)
//    viewHead2Epi.filter{case (k,(v1,v2))=> k=="3b49c1bdeb9d8c3cf6852fb45ddd94aa"&&v1=="一不小心爱上你"}.foreach(println)
    val viewL2_H2 = viewLast2Epi.map{case (k,(v1,v2))=>((k,v1),v2)}.join(viewHead2Epi.map{case (k,(v1,v2))=>((k,v1),v2)})
//    viewL2_H2.foreach(println)
//    println("viewL2_H2:"+viewL2_H2.count) //27992
//    viewL2_H2.filter{case ((k1,k2),(v1,v2))=>k1=="5b6df5c313feee9814af09774df08b93"&&k2=="罪域"}.foreach(println)
    val subViewL2_H2 = viewL2_H2.map{case ((k1,k2),(v1,v2))=>((k1,k2),v1-v2+1)}
//    println("subViewL2_H2:"+subViewL2_H2.count)  //27992
//    subViewL2_H2.foreach(println)
    val totalEpiTemp = training.map{x=>(x(0),x(15))}.distinct   //一共29列,23列是episodetotal
    val totalEpi = Utils.getViewEpiSodeRealKVLen(totalEpiTemp).distinct
//    println(totalEpi.count)   //27992
    val res_S = totalEpi.map{case (k,(v1,v2))=>((k,v1),v2-2)}
//    res_S.foreach(println)
    val series = res_S.distinct.join(subViewL2_H2).map{case ((k1,k2),(v1,v2))=>if(v2<=0||v1<=2)(k1,k2,0.0)else(k1,k2,(v1-2)/v2)}
//    series.foreach(println)
//    series.map{case (k,v1,v2)=>(k,v2)}.reduceByKey(_+_).foreach(println)
    val avg_Series = series.map{case (k,v1,v2)=>(k,v2)}.groupByKey()
//    avg_Series.foreach(println)

    val avgSeries = avg_Series.map{x=>(x._1,x._2.sum/x._2.size)}
//    avgSeries.foreach(println)
//    println(avgSeries.count)  //7227
    /*val prev = sc.textFile("E:/Items/items/data/view_training.txt")
//    println(prev.count)     //6000000
//    C.foreach(println)

    val kvPrevData = prev.map{x=>(x.split("\t")(0),x.split("\t").tail)}
    val kvPreRes = kvPrevData.leftOuterJoin(avgSeries)
//      .map{case (k,(v1,v2))=>if(v2=="None")(k,v1,0)else(k,v1,v2)}
    val kvPreResult = kvPreRes.map{case (k,(v1,v2))=>
        val vn = v2 match {
          case Some(n) => n
          case None => 0.0
        }
      (k,(v1,vn))
    }
//    kvPreResult.foreach(println)
    val data1 = kvPreResult.map{case (x,(y,z))=> (y,z)}.map{case (x,y)=>x.head+"\t"+x.tail.mkString("\t")+"\t"+y}
//    data1.foreach(println)

    val splits = data1.randomSplit(Array(0.6,0.4))

    val A = splits(0).filter(x=>x.split("\t")(0)=="a").takeSample(false,427999,888L)
    val B = splits(0).filter(x=>x.split("\t")(0)=="b").takeSample(false,119546,888L)
    val C = splits(0).filter(x=>x.split("\t")(0)=="c").takeSample(false,157569,888L)
    /*println(" A: "+A.count)   //A: 2554952
    println(" B: "+B.count)   //B: 724248
    println(" C: "+C.count)   //C: 320096*/

    val datas = sc.parallelize(A++B++C)
    val trains = Utils.parsedData(datas)
    val test = Utils.parsedData(splits(1))
    test.persist
//    a.foreach(println)
    /*val v = validData.map{case (x,y,z)=>(y,z.toArray)}
    v.foreach(println)*/
//    val rddPrev = Utils.parsedData(prev)



    /*val splits = prev.randomSplit(Array(0.6,0.4))

    val A = splits(0).filter(x=>x.split("\t")(1)=="a")
    val B = splits(0).filter(x=>x.split("\t")(1)=="b")
    val C = splits(0).filter(x=>x.split("\t")(1)=="c")

    println("A:"+A.count)   //A:2555980
    println("B:"+B.count)   //B:725100
    println("C:"+C.count)   //C:320342*/



//    LabeledPoint(training.map{x=>x(0)},training.map{x=>x.tail})

    val model = DecisionTree.trainClassifier(trains,2,Map[Int,Int](),"gini",5,32)

    val labelAndPreds = test.map{point=>
      val predict = model.predict(point.features)
      (point.label,predict)
    }.persist

    //P正元组数   N负元组数
    val P = labelAndPreds.filter(r=>r._1==1).count
    val N = labelAndPreds.filter(r=>r._1==0).count

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
    println("F:"+F)*/
    //    val TPR = TP/P
    //    val FPR = FP/N
    //    println("TPR:" + TPR)
    //    println("FPR:" + FPR)
//    print(model.toDebugString)
  }
}
