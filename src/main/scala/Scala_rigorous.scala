package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object Scala_rigorous {
  
  def main(args:Array[String]):Unit={
    val conf=new SparkConf()
    val sc=new SparkContext(conf)
    
    val a=Array(1,2,3,4,5,6)
    
   val parA=sc.parallelize(a,5) foreach println
  }
  }