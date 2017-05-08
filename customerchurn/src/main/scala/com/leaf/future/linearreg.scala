package com.leaf.future

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * lpsa.data:
-0.4307829,-1.63735562648104 -2.00621178480549 -1.86242597251066 -1.02470580167082 -0.522940888712441 -0.863171185425945 -1.04215728919298 -0.864466507337306
-0.1625189,-1.98898046126935 -0.722008756122123 -0.787896192088153 -1.02470580167082 -0.522940888712441 -0.863171185425945 -1.04215728919298 -0.864466507337306
-0.1625189,-1.57881887548545 -2.1887840293994 1.36116336875686 -1.02470580167082 -0.522940888712441 -0.863171185425945 0.342627053981254 -0.155348103855541
-0.1625189,-2.16691708463163 -0.807993896938655 -0.787896192088153 -1.02470580167082 -0.522940888712441 -0.863171185425945 -1.04215728919298 -0.864466507337306

  prediction   label
  -1.8436477363151376	-0.4307829
-1.5035919949140735	-0.1625189
-1.124474539125353	-0.1625189
-1.6232248191318206	-0.1625189
-0.44286136656526814	0.3715636
-1.8722996888694121	0.7654678
-0.35102145767266274	0.8544153
-0.4988377051905049	1.2669476
-1.1627671252728424	1.2669476
-0.351173160607566	1.2669476
-0.7376738795527422	1.3480731
-0.06712890377112857	1.446919
-0.6682297820505219	1.4701758
-0.14328220414114157	1.4929041
-1.9536346223202807	1.5581446
-0.015363372330267744	1.5993876
-0.3664065886942614	1.6389967
-1.1745425616800071	1.6956156
0.27896275650985314	1.7137979
0.8099465804041586	1.8000583
0.9778619534248947	1.8484548
-0.7420911171558946	1.8946169
-0.24812868508986563	1.9242487
0.1828082276246784	2.008214
-1.111241397408087	2.0476928
-1.89931161996239	2.1575593
-1.318564372842266	2.1916535
  */
object linearreg {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("linear regression")
    val sc = new SparkContext(conf)
    val input_data = "F:\\java\\spark\\spark\\data\\mllib\\ridge-data\\lpsa.data"
    val data = sc.textFile(input_data)
    val examples = data.map{ line =>
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    }.cache()
    val numExams = examples.count()
    val numIterations = 1000
    val stepSize = 1
    val miniBatchFraction = 1.0
    val model = LinearRegressionWithSGD.train(examples,numIterations,stepSize,miniBatchFraction)
//    model.intercept
//    model.weights
    val prediction = model.predict(examples.map(_.features))
    val predictionAndLabel = prediction.zip(examples.map(_.label))
    val print_predict = predictionAndLabel.take(50)
    for(i <- 0 to print_predict.length -1){
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }
    //误差
    val loss = predictionAndLabel.map{
      case(p, 1)=>
        val err = p - 1
        err * err
    }.reduce(_+_)
    val rmse = math.sqrt(loss / numExams)
    println(s"RMSE = $rmse")
//    val modelPath = "file:/d:test/model.txt"
//    model.save(sc, modelPath)
//    val sameModel = LinearRegressionModel.load(sc, modelPath)




  }
}
