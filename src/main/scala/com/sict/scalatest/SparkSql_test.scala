package com.sict.scalatest

/*
  先从hdfs读取csv文件
  然后将处理后的结果写回hdfs
 */
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.databricks.spark.csv._
object Sql_test {
  def main(args:Array[String]){
    val conf = new SparkConf()
    conf.setAppName("SQL TEST").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df =sqlContext.read.format("com.databricks.spark.csv")
      .option("header","true")                                       //这里如果在csv第一行有属性的话，没有就是"false"
      .load("F:\\machinelearning\\dataset\\15\\add_type.csv")                                   //文件的路径
    df.printSchema()

    var df2=df.select("pitch1_speed")
    df2.registerTempTable("dataTable")                               //将dataframe注册成一个临时表
    val pitch1_speed = sqlContext.sql("select * from dataTable where pitch1_speed !='0'")
    println("the number is not equal 0 :"+pitch1_speed.count())

    val saveOptions = Map("header" -> "true", "path" -> "F:\\machinelearning\\dataset\\15\\result")
    pitch1_speed.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()


  }

}
