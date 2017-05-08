package com.leaf.app_antispam

import java.util.Calendar

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions.{broadcast, when}
import org.apache.spark.sql.types._

object Main {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("Fraud View Detection")
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

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
      val filename = f + "/*"
      // 获取日期字串
      val date = filename.split("/")(7)
      // 检测当日概率表是否已经生成
      val resultfileexists = fs.exists(new Path("hdfs:///user/analyst/gww/" + date + "/like/_SUCCESS"))

      // 若urlcount表存在且当日概率表没有生成，则开始生成数据表
      if (datafileexists && !resultfileexists) {
        println(date)
        // 先删除可能存在的未完成的概率表文件夹，文件夹中会包括概率表、VV的总量概率表、TS的总量概率表
        fs.delete(new Path("hdfs:///user/analyst/gww/" + date), true)
        // 获取urlcount全量数据
        val daterdd = sc.textFile(filename)
        // 取urlcount中的guid、os、dev，以及vv和ts并生成 DataFrame
        val daterows = daterdd.map(l => l.split("\t")).map(cols => Row(cols(0).trim,
          cols(1).trim,
          cols(2).trim,
          cols(10).trim,
          cols(11).trim))
        val dateschema = StructType(List(StructField("guid", StringType, true),
          StructField("os", StringType, true),
          StructField("dev", StringType, true),
          StructField("vvnum", StringType, true),
          StructField("tsnum", StringType, true)))
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
        val vvosdata = vvtable.filter(vvtable("os") === os && vvtable("dev") === dev && vvtable("num") <= 240)
        // 得到用于TS总量概率估计的数据表，其中VV截取240
        val tsosdata = tstable.filter(tstable("os") === os && tstable("dev") === dev && tstable("num") <= 240)

        // 概率估计结果的RDD
        val vrdd = Procedures.probdata(vvosdata, os, dev)
        val trdd = Procedures.probdata(tsosdata, os, dev)

        // 将RDD转换为DataFrame
        val fitschema = StructType(List(StructField("os", IntegerType, true),
          StructField("dev", IntegerType, true),
          StructField("num", IntegerType, true),
          StructField("freq", IntegerType, true),
          StructField("freqest", IntegerType, true),
          StructField("augest", IntegerType, true),
          StructField("lambda", DoubleType, true),
          StructField("lambdafit", DoubleType, true),
          StructField("residual", DoubleType, true),
          StructField("residualfit", DoubleType, true)))
        val vvfit = sqlContext.createDataFrame(vrdd, fitschema)
        val tsfit = sqlContext.createDataFrame(trdd, fitschema)

        // 按照端、VV/TS排序，然后落盘。因为文件很小，所以repartition到一台机器上。
        // 其实可以直接用RDD排序并保存的，但后续计算需要用到该表
        vvfit.repartition(1).sort("os", "dev", "num").rdd.saveAsTextFile("hdfs:///user/analyst/gww/" + date + "/vvfit")
        tsfit.repartition(1).sort("os", "dev", "num").rdd.saveAsTextFile("hdfs:///user/analyst/gww/" + date + "/tsfit")
        vvtable.unpersist
        tstable.unpersist

        // 得到全量VV/TS的概率表
        val vprobtable = Procedures.guiddata(vvosdata, vvfit, os, dev)
        val tprobtable = Procedures.guiddata(tsosdata, tsfit, os, dev)
        // 重命名列，便于将VV和TS的概率整合在一张表中
        val vvprob = vprobtable.withColumnRenamed("like", "vlikenew").withColumnRenamed("ulike", "vulikenew")
        val tsprob = tprobtable.withColumnRenamed("like", "tlikenew").withColumnRenamed("ulike", "tulikenew")

        // 将总量概率转换到每个GUID上的方法是用 VV/TS 数作为 key 来 join 全量GUID表和总量概率估计表
        // datadf 是个体guid表，包含guid、os、dev、vv、ts，考虑到数据会分端存储，将os和dev删除了。
        // vvprob/tsprob 是总量概率表，包含vv/ts，各vv/ts的正常概率和作弊概率
        var datedata = datedf.filter($"os" === os && $"dev" === dev).drop("os").drop("dev")
        datedata = datedata.join(broadcast(vvprob), $"vvnum" === $"num", "left_outer").drop("num").drop("vvnum")
        datedata = datedata.join(broadcast(tsprob), $"tsnum" === $"num", "left_outer").drop("num").drop("tsnum")

        // 概率大表结构定义：
        // power是出现次数（比实际次数多1，因为初始值为1；实际逻辑是用于计算概率的概率的个数）
        // vlike/vulike 分别对应根据VV计算得到的作弊概率和正常概率
        // tlike/tulike 分别对应根据TS计算得到的作弊概率和正常概率
        // like/ulike 分别对应计算VV和TS的几何均值得到的作弊概率和正常概率
        val likeschema = StructType(List(StructField("id", StringType, true),
          StructField("os", StringType, true),
          StructField("dev", StringType, true),
          StructField("power", StringType, true),
          StructField("vlike", StringType, true),
          StructField("vulike", StringType, true),
          StructField("tlike", StringType, true),
          StructField("tulike", StringType, true)))
        // 第一次计算时，概率大表是个空表
        var likedf = sqlContext.createDataFrame(sc.emptyRDD[Row], likeschema)

        // 计算当前日期的前一天的日期字串
        val dateformat = new java.text.SimpleDateFormat("yyyyMMdd")
        var cal = Calendar.getInstance()
        cal.setTime(dateformat.parse(date))
        cal.add(Calendar.DAY_OF_MONTH, -1)
        val oneDayBefore = dateformat.format(cal.getTime())
        // 检测前一天的概率大表是否存在
        val likefileexists = fs.exists(new Path("hdfs:///user/analyst/gww/" + oneDayBefore + "/like/_SUCCESS"))

        if (likefileexists) {
          // 若存在则读取这张概率大表
          val likerdd = sc.textFile("hdfs:///user/analyst/gww/" + oneDayBefore + "/like/*")
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

        val likefit = likedf.join(datedata, $"id" === $"guid", "outer").withColumn("id", when($"id".isNull, $"guid")
          .otherwise($"id")).withColumn("os", when($"os".isNull, os).otherwise($"os")).withColumn("dev", when($"dev".isNull, dev)
          .otherwise($"dev")).withColumn("power", when($"power".isNull, 1.0)
          .otherwise($"power")).withColumn("vlike", when($"vlike".isNull, 0.5)
          .otherwise($"vlike")).withColumn("vulike", when($"vulike".isNull, 0.5)
          .otherwise($"vulike")).withColumn("tlike", when($"tlike".isNull, 0.5)
          .otherwise($"tlike")).withColumn("tulike", when($"tulike".isNull, 0.5)
          .otherwise($"tulike")).withColumn("vlikenew", when($"vlikenew".isNull, 0.5)
          .otherwise($"vlikenew")).withColumn("vulikenew", when($"vulikenew".isNull, 0.5)
          .otherwise($"vulikenew")).withColumn("tlikenew", when($"tlikenew".isNull, 0.5)
          .otherwise($"tlikenew")).withColumn("tulikenew", when($"tulikenew".isNull, 0.5)
          .otherwise($"tulikenew")).withColumn("vlike", when($"guid".isNull, $"vlike")
          .otherwise(Utilities.likelihoodUDF($"vlike", $"power", $"vlikenew"))).withColumn("vulike", when($"guid".isNull, $"vulike")
          .otherwise(Utilities.likelihoodUDF($"vulike", $"power", $"vulikenew"))).withColumn("tlike", when($"guid".isNull, $"tlike")
          .otherwise(Utilities.likelihoodUDF($"tlike", $"power", $"tlikenew"))).withColumn("tulike", when($"guid".isNull, $"tulike")
          .otherwise(Utilities.likelihoodUDF($"tulike", $"power", $"tulikenew"))).withColumn("power", when($"guid".isNull, $"power")
          .otherwise($"power" + 1.0)).drop("guid").drop("vlikenew").drop("tlikenew").drop("vulikenew").drop("tulikenew")

        likefit.rdd.map(_.mkString(",")).saveAsTextFile("hdfs:///user/analyst/gww/" + date + "/like")
        datedf.unpersist
      }
    }
  }
}
