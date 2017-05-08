package com.leaf.exams

import akka.serialization.Serialization
import breeze.numerics.pow
object Enums {
   // <:  上界
   // >:  下界
   // T : M 其中M是另一个泛型类。必须存在一个类型为M[T]的"隐式值"
   // <%  视图界定  (eg: class Pair[T <% Comparable[T]])  意思是T可以被隐式转换成Comparable类型。如果new Pair时会报Int并不是Comparable[Int]的子类,但是RichInt实现了该方法，可以做隐式转换
   //视图界定T <% V 要求必须存在一个从T到V的隐式转换
   //如下是Manifest上下文界定
   def makePair[T : Manifest](first: T,second : T): Unit = {
     val r = new Array[T](2)
     r(0) = first
     r(1) = second
     r
   }//定义一个数组泛型
   //多重界定
//   T :> Lower <: Upper
//   T <; Comparable[T] with Serialization with Cloneable
//    T <% Comparable[T] <% String
//   T : Ordering : Manifest
   //类型约束
//   T =:= U
//   T <:<U
//   T <%<U
//   class Pair[T](val first:T,val second:T)(implicit ev : T<:<Comparable[T])
//  class Pair[+T](val first:T,val second:T) //+代表该类型与T协变 -代表该类型与T逆变





  //  val Red,Yellow,Green = Value
//  val White = Value
//  val Black = Value(0,"Black")

  def main(args: Array[String]) {
    println(3+4->5)//(7,5)
    pow(2,3)


    //闭包closure
    def mulBy(factor : Double) = (x : Double) => factor * x
    val triple = mulBy(3)
    val half = mulBy(0.5)
    println(triple(14) +" " +half(14))    //42.0 7.0
    //柯里化curring：将两个参数的函数转成只需一个参数的函数
    def mulOneAtTime(x : Int) = (y : Int) => x * y
    println(mulOneAtTime(6)(7))  //42
    def numOneAtTimeSim(x:Int)(y:Int) = x * y  //柯里化简写形式

//    val a = Array("ABCD","EFGH")
//    val b = Array("EFD","CDJKJS")
//    a.corresponds(b)(_.equalsIgnoreCase(_))
//    def corresponds[B](that: Seq[B])(p : (A ,B)=> Boolean): Boolean
    //换名调用参数
    def until(condition: => Boolean)(block: => Unit) {
      if(!condition){
        block
        until(condition)(block)
      }
    }
    //使用until函数
    var x = 10
    until (x == 0){
      x-=1
      println(x)
    }
//    println(Enums.Black)
//    println(TrafficLightColor.Green)
//     doWhat(TrafficLightColor.Red)  //stop
     for(c <- TrafficLightColor.values)
       println(c.id + ":" + c)

  }

  import TrafficLightColor._
  def doWhat(color : TrafficLightColor) = {
    if (color == Red) println("stop")
    else if(color == Yellow) println("hurry up")
    else println("go")
  }

}

object TrafficLightColor extends Enumeration {
  type TrafficLightColor = Value   //别的方法可以使用该类型
  val Red,Yellow,Green = Value
}
