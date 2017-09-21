package com.sict.scalatest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
  * 统计字符出现次数
  */
object WordCount {
  def main(args: Array[String]) {


    val conf = new SparkConf()
    conf.setAppName("tt").setMaster("local")
    val sc = new SparkContext(conf)
    val line = sc.textFile("F:\\Spark_learn\\data\\test.txt")

    line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).collect().foreach(println)

    sc.stop()
  }
}