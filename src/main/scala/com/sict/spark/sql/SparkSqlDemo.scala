package com.sict.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import java.io.File

object SparkSqlDemo {

  //读取json创建DateFrame，将处理结果写入json文件
  def Read_Json(spark:SparkSession){
    val df = spark.read.json("input\\people.json")
    df.show()
    df.printSchema()
    df.select("name").show()
    df.select("name").write.format("json").save("F:\\Spark_learn\\data\\people_name.json")
  }

  //读取csv文件创建DataFrame，
  def Read_Csv(spark:SparkSession){
    val csv_data = spark.read.option("header","true").csv("F:\\machinelearning\\dataset\\15\\add_type.csv")
    csv_data.show(10)
  }
  //SparkSql使用
  def Spark_Sql(spark:SparkSession): Unit ={
    val df = spark.read.json("F:\\Spark_learn\\data\\people.json")
    df.createOrReplaceTempView("people")
    val people_name= spark.sql("select name from people")
    people_name.show()

  }
  //操作HIVE表
  def Spark_Hive(spark: SparkSession): Unit ={
    import spark.sql
    spark.catalog.listTables.show()
    sql("DROP TABLE IF EXISTS src")
    sql("CREATE TABLE src (key INT, value1 STRING,value2 STRING) USING hive OPTIONS(fileFormat 'textfile',fieldDelim '\\t')")
    sql("LOAD DATA LOCAL INPATH 'input//kv.txt' INTO TABLE src ")             //此处路径换为项目外路径会报错，只能在项目内新建input路径
    sql("SELECT * FROM src").show()

  }
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder().master("local")
      .appName("Spark SQL basic example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()                    //使得SparkSession支持hive
      .getOrCreate()

    //Read_Json(spark)
    //Read_Csv(spark)
    //Spark_Sql(spark)
    Spark_Hive(spark)



  }

}

