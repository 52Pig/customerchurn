package com.leaf.exams

import com.leaf.oldcustomerchurn.Utils

object FunctionExam {
  def main(args: Array[String]): Unit = {
    val sc = Utils.initial("function exam", true)
    //    rdd.drop(1)
    /*val a = sc.parallelize(Array(("abcd", scala.Iterable(1.0, 2.0, 3.0))))
    a.map { case (x, y) =>
      (x, y.sum / y.size)
    }.foreach(println)*/
    val a = sc.parallelize(Array(("123", 4.0), ("456", 9.0), ("789", 9.0), ("789", Array(10.0, 20))))
    val b = sc.parallelize(Array(("123", 8.0), ("789", 10)))

    val c = a.join(b)
    c.foreach(println)
    /*
    (123,(4.0,8.0))
    (789,(9.0,10))
    */
    val d = a.cogroup(b)
    d.foreach(println)
    /*
    (456,(CompactBuffer(9.0),CompactBuffer()))
    (123,(CompactBuffer(4.0),CompactBuffer(8.0)))
    (789,(CompactBuffer(9.0),CompactBuffer(10)))
    */
    val e = a.leftOuterJoin(b)
    e.foreach(println)
    /*
    (456,(9.0,None))
    (123,(4.0,Some(8.0)))
    (789,(9.0,Some(10)))
    */
    val f = a.fullOuterJoin(b)
    f.foreach(println)
    /*
    (456,(Some(9.0),None))
    (123,(Some(4.0),Some(8.0)))
    (789,(Some(9.0),Some(10)))
    */
    val g = a.cartesian(b)
    g.foreach(println)
    /*
    ((123,4.0),(123,8.0))
    ((123,4.0),(789,10))
    ((456,9.0),(123,8.0))
    ((456,9.0),(789,10))
    ((789,9.0),(123,8.0))
    ((789,9.0),(789,10))
    */
    /*val h = a.coalesce(6,true)
    h.foreach(println)
    a.dependencies.foreach(println)*/
    val i = a.keyBy { case (k, v) => ("haha", 234) }
    i.foreach(println)
    /*
    ((haha,234),(123,4.0))
    ((haha,234),(456,9.0))
    ((haha,234),(789,9.0))
    */
    /* val cc = a.aggregateByKey{case (x,y)=>(x,y)}
    println(cc)*/
    /*val data = sc.textFile("E:\\Items\\items\\new\\abc.txt")
   //       data.map(x=>x.)
   //       val reg = "[0-9]+".r
   val reg= "[\\d]+".r
   val a = data.map(line=>line.split("\t")).map(x=> for (words <- reg findAllIn x(0)) yield words)
   for(b <-a){
     b.foreach(println)
   }*/

    //      reg.findAllIn(x(0)))
    //data.map(line=>line.split("\t")(0).matches("[^\\D]+")).foreach(println)
  }
}
