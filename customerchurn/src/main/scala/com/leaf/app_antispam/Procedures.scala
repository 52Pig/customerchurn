package com.leaf.app_antispam

import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType}
import org.apache.spark.sql.functions.{when}
import breeze.linalg.{DenseVector, DenseMatrix, *, inv, diag, sum, max}
import breeze.numerics.{pow, sqrt, floor, abs}

object Procedures {

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

  def mudata(prob: DenseVector[Double], num: DenseVector[Double]): DenseVector[Double] = {
    // INPUT: VV/TS数列及其对应概率的数列
    // OUTPUT: VV/TS数的对应lambda数列
    // 构建概率和VV/TS的数据表，列组合
    val numtemp = DenseMatrix(prob.toArray, num.toArray).t
    // 逐行求取转换后的 z 值
    val z = numtemp(*,::).map(r => Utilities.poissonZ(r(0),r(1)))
    // 求取各 z 值对应的 w 值
    val w = z.map(r => Utilities.lambertW0(r)._1)
    // 将 w 值换算回 lambda
    val lambda = -(w :* num)
    return lambda
  }

  def fitdata (lambda: DenseVector[Double], num: DenseVector[Double]): (DenseVector[Double], DenseVector[Double], DenseVector[Double]) = {
    // INPUT: VV/TS数列及其对应lambda的数列
    // OUTPUT: lambda的拟合值、lambda拟合值的残差、lambda拟合值的残差的拟合值
    // 拟合得到三角图的lambda拟合直线
    val (lbeta, lalpha) = Utilities.simpleLinear(lambda, num)
    // 求取lambda实际值与估计值的差值（残差）
    val r = lambda :- (num * lbeta + lalpha)
    // 拟合得到lambda残差的回归系数
    val rbeta = Utilities.multipleLinear(lambda, r, num)
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
    var probtemp = numtemp(*,::).map(l => Utilities.probEst(l(0), l(1)))
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
      probtemp = numtemp(*,::).map(l => Utilities.probEst(l(0), l(1)))
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
    val probtemp = numtemp(*,::).map(l => Utilities.probEst(l(0), l(1)))
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

  def guiddata (ostable: DataFrame, fittable: DataFrame, os: Int, dev: Int): DataFrame = {
    // 将拟合结果表转换为全VV/TS表，因为拟合时VV/TS截取了240，超过该值的VV/TS也需分配概率
    // INPUT: ostable 是包含所有VV/TS数的表；fittable 是拟合得到的概率表，包含VV/TS，各VV/TS的正常概率和作弊概率
    // OUTPUT: 所有VV/TS数及其正常概率和作弊概率组成的表
    var result = ostable.filter(ostable("os")===os && ostable("dev")===dev)
    // 以ostable为基准join拟合表，选出vv/ts值、频数、作弊量估计值等生成新表，并排序
    import org.apache.spark.sql._
    result = result.as("l").join(fittable.as("f"), $"l.num"===$"f.num", "left_outer").select($"l.num", $"l.count".cast(DoubleType).as("freq"), $"f.augest".cast(DoubleType).as("aug1")).sort("num")
    // 若作弊量估计者为null，意味着是在拟合表之外，将其值复赋为频数的全部
    result = result.withColumn("aug", when($"aug1".isNull, $"freq").otherwise($"aug1")).drop("aug1")
    // 计算各VV/TS的作弊概率和正常，若作弊量/正常量估计值为0，则赋值为 0.000001（替代0值，避免几何平均全部为0）
    result = result.withColumn("like", when($"aug"/$"freq">0.000001, $"aug"/$"freq").otherwise(0.000001)).withColumn("ulike", when(($"freq"-$"aug")/$"freq">0.000001, ($"freq"-$"aug")/$"freq").otherwise(0.000001))
    // 删除join表中不需要的部分（感觉前面用了select，这部分的os、dev、prob、fraud应该没有意义，暂无影响，需要再试验了）
    result = result.drop("os").drop("dev").drop("freq").drop("aug").drop("prob").drop("fraud")
    return result
  }

}
