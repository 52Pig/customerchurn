package com.leaf.app_antispam

import java.util.Calendar
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, DoubleType}
import org.apache.spark.sql.functions.{sqrt => Sqrt, col, udf, when, broadcast}
import org.apache.hadoop.fs.{FileSystem,Path}
import breeze.linalg.{DenseVector, DenseMatrix, *, inv, diag, sum, max}
import breeze.numerics.{pow, sqrt, floor, abs}
import breeze.stats.{mean, stddev}

object Antispm extends App {
  val config = new SparkConf().setAppName("Fraud View Detection")
  val sc = new SparkContext(config)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  //===============================================================
  // Utilities
  //===============================================================
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
  //===============================================================

  //===============================================================
  // Procedures
  //===============================================================
  def mudata(prob: DenseVector[Double], num: DenseVector[Double]): DenseVector[Double] = {
    // INPUT: VV/TS数列及其对应概率的数列
    // OUTPUT: VV/TS数的对应lambda数列
    // 构建概率和VV/TS的数据表，列组合
    val numtemp = DenseMatrix(prob.toArray, num.toArray).t
    // 逐行求取转换后的 z 值
    val z = numtemp(*,::).map(r => poissonZ(r(0),r(1)))
    // 求取各 z 值对应的 w 值
    val w = z.map(r => lambertW0(r)._1)
    // 将 w 值换算回 lambda
    val lambda = -(w :* num)
    return lambda
  }

  def fitdata (lambda: DenseVector[Double], num: DenseVector[Double]): (DenseVector[Double], DenseVector[Double], DenseVector[Double]) = {
    // INPUT: VV/TS数列及其对应lambda的数列
    // OUTPUT: lambda的拟合值、lambda拟合值的残差、lambda拟合值的残差的拟合值
    // 拟合得到三角图的lambda拟合直线
    val (lbeta, lalpha) = simpleLinear(lambda, num)
    // 求取lambda实际值与估计值的差值（残差）
    val r = lambda :- (num * lbeta + lalpha)
    // 拟合得到lambda残差的回归系数
    val rbeta = multipleLinear(lambda, r, num)
    // 制作用于求取残差拟合值的X矩阵
    val one = DenseVector.ones[Double](num.length).toArray
    val X = DenseMatrix(one, num.toArray, pow(num.toArray,2), pow(num.toArray,3), pow(num.toArray,4)).t
    // 求取残差拟合值
    val rfitted = X * rbeta
    // 加总lambda直线拟合值和残差拟合值得到lambda的估计值
    var lambdafitted = num * lbeta + lalpha + rfitted
    // 检验估计值，残差拟合线与水平零线有两个交叉点，取第一个交叉点前的部分分析
    // 确定交叉点的位置
    val i = r.toArray.indexWhere(_<0) - 1
    // 构建lambda拟合值与VV/TS的矩阵
    var numtemp = DenseMatrix(lambdafitted.toArray, num.toArray).t
    // 用该矩阵求取每个VV/TS在总量中的概率（比重），即从lambda换算回概率
    var probtemp = numtemp(*,::).map(l => probEst(l(0), l(1)))
    // 求取各VV/TS用lambda估计值换算出概率的阶梯差值，即VV等于1时与VV等于2时的概率（比重）差值
    var probdiff = probtemp(0 to (probtemp.length-2)) :- probtemp(1 to (probtemp.length-1))
    // 求取差值的阶梯之比（因为假设VV越小，相对于下一个VV的差值就应该越大），用于让结果分布更合理
    var probdivid = probdiff(0 until i)/probdiff(1 to i)
    // 设置k的初始值，用于控制迭代次数不超过VV值的个数（即240以内）。
    var k = num.length.toDouble - 1.0
    // 若存在VV对应的概率小于0，或者第一个VV的概率对第二个VV的概率的差值比第二个VV的概率对第三个VV的概率的差值小的情况，
    // 则重新计算lambda估计值，方法是抬高第一步直线拟合的曲线，即取更小的截距（截距alpha小于0）。
    while ((!probtemp.toArray.find(_<0.0).isEmpty || !probdivid.toArray.find(_<1.0).isEmpty) && k > 0.0) {
      lambdafitted = num * lbeta + (lalpha * k/num.length) + rfitted
      numtemp = DenseMatrix(lambdafitted.toArray, num.toArray).t
      probtemp = numtemp(*,::).map(l => probEst(l(0), l(1)))
      probdiff = probtemp(0 to (probtemp.length-2)) :- probtemp(1 to (probtemp.length-1))
      probdivid = probdiff(0 until i)/probdiff(1 to i)
      k -= 1.0
    }
    // 返回的残差值、残差拟合值都主要用于后期绘图，lambdafitted是所需的估计值，也用于后期绘图。
    return (r, rfitted, lambdafitted)
  }

  def augdata (lambdafitted: DenseVector[Double], num: DenseVector[Double], freqest: DenseVector[Double], augest: DenseVector[Double]): (DenseVector[Double], DenseVector[Double]) = {
    // INPUT: VV/TS数列及其对应lambda的数列、估计正常频数、估计作弊频数
    // OUTPUT: 更新后的正常频数和作弊频数
    // 构建lambda拟合值与VV/TS的矩阵
    val numtemp = DenseMatrix(lambdafitted.toArray, num.toArray).t
    // 用该矩阵求取每个VV/TS在总量中的概率（比重），即从lambda换算回概率
    val probtemp = numtemp(*,::).map(l => probEst(l(0), l(1)))
    // 根据换算的概率求取各VV/TS对应的正常量
    var freqtemp = floor(probtemp * sum(freqest)/sum(probtemp))
    // 根据换算的概率求取各VV/TS对应的作弊量
    var augtemp = freqest - freqtemp
    // 若根据换算的概率得到的作弊量小于0，则取正常量为原估计值
    freqtemp(augtemp:<0.0) := freqest(augtemp:<0.0)
    // 若根据换算的概率得到的作弊量小于0，则取作弊量为0
    augtemp(augtemp:<0.0) := 0.0
    // 求取新作弊量，加总原作弊量估计值和本次的作弊量估计值
    val aug = augest :+ augtemp
    // 求取新正常量，即直接赋值
    val freq = freqtemp
    return (freq, aug)
  }

  def probdata (ostable: DataFrame, os: Int, dev: Int, masebiggest: Double = 0.01, maxbiggest: Double = 0.08): RDD[Row] = {
    // 总量作弊概率估算的主函数，调用了上面的mudata、fitdata和augdata。
    // INPUT: VV/TS数的频数表，表列包含VV/TS的num、os、dev和count；拟合力度控制变量 MASE 和 MAX 的阈值
    // OUTPUT: VV/TS数、os、dev、VV/TS数对应的原始频数、估计正常频数、估计作弊频数、初始lambda、估计lambda、初始lambda残差、估计lambda残差
    // 按照OS和Device取数据表，限制num小于等于240，意味着直接将大于240的VV/TS视为刷量。
    val osdata = ostable.filter(ostable("os")===os && ostable("dev")===dev && ostable("num")<=240)
    // 获取VV/TS值列，num原本是从0开始的，为了能顺利拟合，普增1。
    val num = DenseVector(osdata.map(_.getAs[Long]("num").asInstanceOf[Double]+1).collect)
    // 获取VV/TS频数列
    val freq = DenseVector(osdata.map(_.getAs[Long]("count").asInstanceOf[Double]).collect)
    // 求取各VV/TS的原始概率
    val prob = freq/sum(freq)
    // 各VV/TS频数正常量估计值的初始值，等于原始量
    var freqest = freq
    // 各VV/TS频数作弊量估计值的初始值，均为零
    var augest = freq :- freqest
    // 计算各VV/TS的初始lambda值
    val lambda = mudata(prob, num)
    // 计算各VV/TS的lambda估计值的初始值
    var lambdaest = lambda
    // 得到lambda的残差值、残差拟合值和lambda拟合值
    var fittuple = fitdata(lambdaest, num)
    var r = fittuple._1
    var rfitted = fittuple._2
    var lambdafitted = fittuple._3
    // 计算lambda的残差的残差
    // var rres = r(lambda :!= 0.0) :- rfitted(lambda :!= 0.0)
    var range = lambda.size
    while (lambda(range-1) == 0.0) {
      range -= 1
    }
    var rres = r(0 until range) :- rfitted(0 until range)
    // 检验估计值，残差拟合线与水平零线有两个交叉点，取第一个交叉点前的部分分析。确定交叉点的位置。
    var i = r.toArray.indexWhere(_<0)
    // 取lambda的残差的残差中第一个交叉点前的部分分析。确定交叉点的位置。
    var rresslice = rres(0 until i)
    // 取lambda的残差的残差中第一个交叉点前的部分的最大值作为控制指标之一。
    var imax = max(abs(rresslice))
    // 取lambda的残差的残差中第一个交叉点前的部分的均方和作为控制指标之一。
    var imase = sqrt(sum(pow(rresslice, 2)))/i
    // 将均方和不为0作为循环的标准之一。
    var control = imase
    var jmase = 0.0
    println(control, imase, imax)
    // 循环优化lambda值，每次都用新得到的VV/TS频数的正常量来计算，以期逼近符合分布的VV/TS频数分布
    // 继续的标准包括两部分，一部分是均方和不为0，若为0则终止迭代。另一部分由四个条件组成，任意一个成立就需要继续迭代：
    // 1. 交叉点前的lambda的残差的残差存在 NaN 或正负无穷大，即有lambda是计算不出来的无穷大
    // 2. 交叉点前的lambda的残差的残差均方和大于最大允许的 MASE 值
    // 3. 交叉点前的lambda的残差的残差最大值大于最大允许的 MAX 值
    while ((rresslice.toArray.contains(Double.NaN) || rresslice.toArray.contains(Double.PositiveInfinity) || rresslice.toArray.contains(Double.NegativeInfinity) || imase > masebiggest || imax > maxbiggest) && control != 0.0) {
      val freqtuple = augdata(lambdafitted, num, freqest, augest)
      freqest = freqtuple._1
      augest = freqtuple._2
      val probest = freqest/sum(freqest)
      lambdaest = mudata(probest, num)
      fittuple = fitdata(lambdaest, num)
      r = fittuple._1
      rfitted = fittuple._2
      lambdafitted = fittuple._3
      // rres = r(lambda :!= 0.0) :- rfitted(lambda :!= 0.0)
      range = lambda.size
      while (lambda(range-1) == 0.0) {
        range -= 1
      }
      rres = r(0 until range) :- rfitted(0 until range)
      i = r.toArray.indexWhere(_<0)
      rresslice = rres(0 until i)
      // 更新最大值和均方和
      imax = max(abs(rres(0 until i)))
      imase = sqrt(sum(pow(rres(0 until i), 2)))/i
      // 计算 MASE 变化量，原本打算用之确定是否停止迭代，后来发现效果不好。
      // 但若 MASE 变化量为0，则会出现循环，因此这个指标变成了防止死循环的了。
      control = imase - jmase
      jmase = imase
      println(control, imase, imax)
    }
    // 将 lambda 估计量为0的残差估计量也平滑为0，避免出现两者相加时出现不合逻辑的值。
    r(lambdaest:==0.0) := 0.0
    // 生成数据集以便 RDD 化，这里把之前给 num 普增的 1 再减回来。
    val fitarray = Array(Array.fill(num.length)(os.toDouble), Array.fill(num.length)(dev.toDouble), (num:-1.0).toArray, freq.toArray, freqest.toArray, augest.toArray, lambda.toArray, lambdafitted.toArray, r.toArray, rfitted.toArray).transpose
    // 生成 RDD。
    val fitrdd = sc.parallelize(fitarray).map(f => Row(f(0).toInt,f(1).toInt,f(2).toInt,f(3).toInt,f(4).toInt,f(5).toInt,f(6),f(7),f(8),f(9)))
    return fitrdd
  }

  def guiddata (ostable: DataFrame, fittable: DataFrame, os: Int, dev: Int): DataFrame = {
    // 将拟合结果表转换为全VV/TS表，因为拟合时VV/TS截取了240，超过该值的VV/TS也需分配概率
    // INPUT: ostable 是包含所有VV/TS数的表；fittable 是拟合得到的概率表，包含VV/TS，各VV/TS的正常概率和作弊概率
    // OUTPUT: 所有VV/TS数及其正常概率和作弊概率组成的表
    var result = ostable.filter(ostable("os")===os && ostable("dev")===dev)
    // 以ostable为基准join拟合表，选出vv/ts值、频数、作弊量估计值等生成新表，并排序
    result = result.as("l").join(fittable.as("f"), $"l.num"===$"f.num", "left_outer").select($"l.num", $"l.count".cast(DoubleType).as("freq"), $"f.augest".cast(DoubleType).as("aug1")).sort("num")
    // 若作弊量估计者为null，意味着是在拟合表之外，将其值复赋为频数的全部
    result = result.withColumn("aug", when($"aug1".isNull, $"freq").otherwise($"aug1")).drop("aug1")
    // 计算各VV/TS的作弊概率和正常，若作弊量/正常量估计值为0，则赋值为 0.000001（替代0值，避免几何平均全部为0）
    result = result.withColumn("like", when($"aug"/$"freq">0.000001, $"aug"/$"freq").otherwise(0.000001)).withColumn("ulike", when(($"freq"-$"aug")/$"freq">0.000001, ($"freq"-$"aug")/$"freq").otherwise(0.000001))
    // 删除join表中不需要的部分（感觉前面用了select，这部分的os、dev、prob、fraud应该没有意义，暂无影响，需要再试验了）
    result = result.drop("os").drop("dev").drop("freq").drop("aug").drop("prob").drop("fraud")
    return result
  }
  //===============================================================

  //===============================================================
  // Main
  //===============================================================
  val fs = FileSystem.get(sc.hadoopConfiguration)
  // 获取放置urlcount表的文件夹路径
  val fpath = new Path("hdfs:///logdata/wireless/mds/l_mobile_mds_guid_urlcount_detail/")
  // 获取文件夹中的文件夹的路径
  var flist = fs.listStatus(fpath).map(_.getPath)
  // 第一个路径总是20160101，所以删除之。
  flist = flist.drop(1)

  for (f <- flist) {
    // 逐日循环生成数据，先检测 urlcount 数据是否存在
    val datafileexists = fs.exists(f)
    // 生成文件名
    val filename = f+"/*"
    // 获取日期字串
    val date = filename.split("/")(7)
    // 检测当日概率表是否已经生成
    val resultfileexists = fs.exists(new Path("hdfs:///user/analyst/gww/"+date+"/like/_SUCCESS"))

    // 若urlcount表存在且当日概率表没有生成，则开始生成数据表
    if (datafileexists && !resultfileexists) {
      println(date)
      // 先删除可能存在的未完成的概率表文件夹，文件夹中会包括概率表、VV的总量概率表、TS的总量概率表
      fs.delete(new Path("hdfs:///user/analyst/gww/"+date), true)
      // 获取urlcount全量数据
      val daterdd = sc.textFile(filename)
      // 取urlcount中的guid、os、dev，以及vv和ts并生成 DataFrame
      val daterows = daterdd.map(l => l.split("\t")).map(cols => Row(cols(0).trim,
                                                                     cols(1).trim,
                                                                     cols(2).trim,
                                                                     cols(10).trim,
                                                                     cols(11).trim))
      val dateschema = StructType(List(StructField("guid",     StringType, true),
                                       StructField("os",       StringType, true),
                                       StructField("dev",      StringType, true),
                                       StructField("vvnum",    StringType, true),
                                       StructField("tsnum",    StringType, true)))
      var datedf = sqlContext.createDataFrame(daterows, dateschema)
      datedf.persist

      // 生成vv的频数表，按端统计
      var vvtable = datedf.groupBy("os", "dev", "vvnum").count
      vvtable = vvtable.withColumn("num", $"vvnum".cast(LongType)).drop("vvnum").sort("os", "dev", "num")
      vvtable.persist
      // 生成ts的频数表，按端统计
      var tstable = datedf.groupBy("os", "dev", "tsnum").count
      tstable = tstable.withColumn("num", $"tsnum".cast(LongType)).drop("tsnum").sort("os", "dev", "num")
      tstable.persist

      // 确定只生成iOS Phone端数据，工程上需改为os和dev的循环
      val os = 52
      val dev = 1

      // 得到用于VV总量概率估计的数据表，其中VV截取240
      val vvosdata = vvtable.filter(vvtable("os")===os && vvtable("dev")===dev && vvtable("num")<=240)
      // 得到用于TS总量概率估计的数据表，其中VV截取240
      val tsosdata = tstable.filter(tstable("os")===os && tstable("dev")===dev && tstable("num")<=240)

      // 概率估计结果的RDD
      val vrdd = probdata(vvosdata, os, dev)
      val trdd = probdata(tsosdata, os, dev)

      // 将RDD转换为DataFrame
      val fitschema = StructType(List(StructField("os",          IntegerType, true),
                                      StructField("dev",         IntegerType, true),
                                      StructField("num",         IntegerType, true),
                                      StructField("freq",        IntegerType, true),
                                      StructField("freqest",     IntegerType, true),
                                      StructField("augest",      IntegerType, true),
                                      StructField("lambda",      DoubleType,  true),
                                      StructField("lambdafit",   DoubleType,  true),
                                      StructField("residual",    DoubleType,  true),
                                      StructField("residualfit", DoubleType,  true)))
      val vvfit = sqlContext.createDataFrame(vrdd, fitschema)
      val tsfit = sqlContext.createDataFrame(trdd, fitschema)

      // 按照端、VV/TS排序，然后落盘。因为文件很小，所以repartition到一台机器上。
      // 其实可以直接用RDD排序并保存的，但后续计算需要用到该表
      vvfit.repartition(1).sort("os","dev","num").rdd.saveAsTextFile("hdfs:///user/analyst/gww/"+date+"/vvfit")
      tsfit.repartition(1).sort("os","dev","num").rdd.saveAsTextFile("hdfs:///user/analyst/gww/"+date+"/tsfit")
      vvtable.unpersist
      tstable.unpersist

      // 得到全量VV/TS的概率表
      val vprobtable = guiddata(vvosdata, vvfit, os, dev)
      val tprobtable = guiddata(tsosdata, tsfit, os, dev)
      // 重命名列，便于将VV和TS的概率整合在一张表中
      val vvprob = vprobtable.withColumnRenamed("like", "vlikenew").withColumnRenamed("ulike", "vulikenew")
      val tsprob = tprobtable.withColumnRenamed("like", "tlikenew").withColumnRenamed("ulike", "tulikenew")

      // 将总量概率转换到每个GUID上的方法是用 VV/TS 数作为 key 来 join 全量GUID表和总量概率估计表
      // datadf 是个体guid表，包含guid、os、dev、vv、ts，考虑到数据会分端存储，将os和dev删除了。
      // vvprob/tsprob 是总量概率表，包含vv/ts，各vv/ts的正常概率和作弊概率
      var datedata = datedf.filter($"os"===os && $"dev"===dev).drop("os").drop("dev")
      datedata = datedata.join(broadcast(vvprob), $"vvnum"===$"num", "left_outer").drop("num").drop("vvnum")
      datedata = datedata.join(broadcast(tsprob), $"tsnum"===$"num", "left_outer").drop("num").drop("tsnum")

      // 概率大表结构定义：
      // power是出现次数（比实际次数多1，因为初始值为1；实际逻辑是用于计算概率的概率的个数）
      // vlike/vulike 分别对应根据VV计算得到的作弊概率和正常概率
      // tlike/tulike 分别对应根据TS计算得到的作弊概率和正常概率
      // like/ulike 分别对应计算VV和TS的几何均值得到的作弊概率和正常概率
      val likeschema = StructType(List(StructField("id",     StringType, true),
                                       StructField("os",     StringType, true),
                                       StructField("dev",    StringType, true),
                                       StructField("power",  StringType, true),
                                       StructField("vlike",  StringType, true),
                                       StructField("vulike", StringType, true),
                                       StructField("tlike",  StringType, true),
                                       StructField("tulike", StringType, true)))
      // 第一次计算时，概率大表是个空表
      var likedf = sqlContext.createDataFrame(sc.emptyRDD[Row], likeschema)

      // 计算当前日期的前一天的日期字串
      val dateformat = new java.text.SimpleDateFormat("yyyyMMdd")
      var cal = Calendar.getInstance()
      cal.setTime(dateformat.parse(date))
      cal.add(Calendar.DAY_OF_MONTH,-1)
      val oneDayBefore= dateformat.format(cal.getTime())
      // 检测前一天的概率大表是否存在
      val likefileexists = fs.exists(new Path("hdfs:///user/analyst/gww/"+oneDayBefore+"/like/_SUCCESS"))

      if (likefileexists) {
        // 若存在则读取这张概率大表
        val likerdd = sc.textFile("hdfs:///user/analyst/gww/"+oneDayBefore+"/like/*")
        val likerows = likerdd.map(l => l.split(',')).map(cols => Row(cols(0).trim,
                                                                      cols(1).trim,
                                                                      cols(2).trim,
                                                                      cols(3).trim,
                                                                      cols(4).trim,
                                                                      cols(5).trim,
                                                                      cols(6).trim,
                                                                      cols(7).trim))
        likedf = sqlContext.createDataFrame(likerows, likeschema)
      }

      // 实际代码合并为一行，目的是避免变量更新造成计算负担。下面的注释部分是具体逻辑。
      // var likefit = likedf.join(datedata, $"id"===$"guid", "outer")
      // 合并guid、os和dev
      // likefit = likefit.withColumn("id",  when($"id".isNull,  $"guid").otherwise($"id"))
      // likefit = likefit.withColumn("os",  when($"os".isNull,  os).otherwise($"os"))
      // likefit = likefit.withColumn("dev", when($"dev".isNull, dev).otherwise($"dev"))
      // 给未在概率大表中出现过的GUID的power、vlike、vulike、tlike、tslike赋值
      // likefit = likefit.withColumn("power",  when($"power".isNull,  1.0).otherwise($"power"))
      // likefit = likefit.withColumn("vlike",  when($"vlike".isNull,  0.5).otherwise($"vlike"))
      // likefit = likefit.withColumn("vulike", when($"vulike".isNull, 0.5).otherwise($"vulike"))
      // likefit = likefit.withColumn("tlike",  when($"tlike".isNull,  0.5).otherwise($"tlike"))
      // likefit = likefit.withColumn("tulike", when($"tulike".isNull, 0.5).otherwise($"tulike"))
      // 给当日新表中的vlikenew、vulikenew、tlikenew、tslikenew赋值，便于计算当日的综合概率
      // likefit = likefit.withColumn("vlikenew",  when($"vlikenew".isNull,  0.5).otherwise($"vlikenew"))
      // likefit = likefit.withColumn("vulikenew", when($"vulikenew".isNull, 0.5).otherwise($"vulikenew"))
      // likefit = likefit.withColumn("tlikenew",  when($"tlikenew".isNull,  0.5).otherwise($"tlikenew"))
      // likefit = likefit.withColumn("tulikenew", when($"tulikenew".isNull, 0.5).otherwise($"tulikenew"))
      // 更新vlike、vulike、tlike、tslike，若GUID在当日未出现，则取原值；若出现了，则求几何均值
      // likefit = likefit.withColumn("vlike",  when($"guid".isNull, $"vlike").otherwise(likelihoodUDF($"vlike",  $"power",$"vlikenew")))
      // likefit = likefit.withColumn("vulike", when($"guid".isNull, $"vulike").otherwise(likelihoodUDF($"vulike",$"power",$"vulikenew")))
      // likefit = likefit.withColumn("tlike",  when($"guid".isNull, $"tlike").otherwise(likelihoodUDF($"tlike",  $"power",$"tlikenew")))
      // likefit = likefit.withColumn("tulike", when($"guid".isNull, $"tulike").otherwise(likelihoodUDF($"tulike",$"power",$"tulikenew")))
      // 更新power，若GUID在当日未出现，则取原值；若出现了，则加1。
      // likefit = likefit.withColumn("power",  when($"guid".isNull, $"power").otherwise($"power"+1.0))
      // 删除当日概率表在join时添进来的列
      // likefit = likefit.drop("guid").drop("vlikenew").drop("tlikenew").drop("vulikenew").drop("tulikenew")
      val likefit = likedf.join(datedata,$"id"===$"guid","outer").withColumn("id",when($"id".isNull,$"guid").otherwise($"id")).withColumn("os",when($"os".isNull,os).otherwise($"os")).withColumn("dev",when($"dev".isNull,dev).otherwise($"dev")).withColumn("power",when($"power".isNull,1.0).otherwise($"power")).withColumn("vlike",when($"vlike".isNull,0.5).otherwise($"vlike")).withColumn("vulike",when($"vulike".isNull,0.5).otherwise($"vulike")).withColumn("tlike",when($"tlike".isNull,0.5).otherwise($"tlike")).withColumn("tulike",when($"tulike".isNull,0.5).otherwise($"tulike")).withColumn("vlikenew",when($"vlikenew".isNull,0.5).otherwise($"vlikenew")).withColumn("vulikenew",when($"vulikenew".isNull,0.5).otherwise($"vulikenew")).withColumn("tlikenew",when($"tlikenew".isNull,0.5).otherwise($"tlikenew")).withColumn("tulikenew",when($"tulikenew".isNull,0.5).otherwise($"tulikenew")).withColumn("vlike",when($"guid".isNull,$"vlike").otherwise(likelihoodUDF($"vlike",$"power",$"vlikenew"))).withColumn("vulike",when($"guid".isNull,$"vulike").otherwise(likelihoodUDF($"vulike",$"power",$"vulikenew"))).withColumn("tlike",when($"guid".isNull,$"tlike").otherwise(likelihoodUDF($"tlike",$"power",$"tlikenew"))).withColumn("tulike",when($"guid".isNull,$"tulike").otherwise(likelihoodUDF($"tulike",$"power",$"tulikenew"))).withColumn("power",when($"guid".isNull,$"power").otherwise($"power"+1.0)).drop("guid").drop("vlikenew").drop("tlikenew").drop("vulikenew").drop("tulikenew")

      likefit.rdd.map(_.mkString(",")).saveAsTextFile("hdfs:///user/analyst/gww/"+date+"/like")
      datedf.unpersist
    }
  }
}
