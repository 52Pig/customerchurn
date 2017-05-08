package com.leaf.exams

import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.catalyst.expressions.Row
//import org.apache.spark.sql.hive.HiveContext

object SQLGraphXExam {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    /*val conf = new SparkConf().setAppName("PageRank").setMaster("local")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use saladata")
    val verticesdata = hiveContext.sql("select id,title from vertices")
    val edgesdata = hiveContext.sql("select srcid,distid from edges")

    //装载数据到顶点
    val vertices = verticesdata.map{
      case Row(id,title) => (id.toString.toLong,title.toString)
    }

    val edges = edgesdata.map{
      case Row(srcid,distid) => Edge(srcid.toString.toLong,distid.toString.toLong,0)
    }

    //构建图
    val graph = Graph(vertices,edges,"").persist()
    println("***************************************")
    println("PageRank计算，获取最有价值的数据")
    println("***************************************")
    val prGraph = graph.pageRank(0.001).cache()

    val titleAndPrGraph = graph.outerJoinVertices(prGraph.vertices){
      (v,title,rank) => (rank.getOrElse(0.0),title)
    }
    titleAndPrGraph.vertices.top(10){
      Ordering.by((entry:(VertexId,(Double,String)))=>entry._2._1)
    }.foreach(t => println(t._2._2 + ": "+t._2._1))

    sc.stop*/

  }
}
