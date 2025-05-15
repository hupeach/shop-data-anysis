package org.example.demo

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkClean {
  def main(args: Array[String]): Unit = {
    val orderPath = if (args == null || args.length == 0) "data/market_sale_order.csv" else args(0)
    val returnPath = if (args == null || args.length == 0) "data/market_sale_return.csv" else args(1)
    val personPath = if (args == null || args.length == 0) "data/market_sale_persons.csv" else args(2)
    val output = if (args == null || args.length == 0) "output/" else args(3)

    // 创建Spark会话
    val spark = SparkSession.builder()
      .appName("SparkClean")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    // 1. 读取原始数据并去重
    val orderDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(orderPath)
      .dropDuplicates()

    val returnDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(returnPath)
      .dropDuplicates()

    val personDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(personPath)
      .dropDuplicates()

    // 2. 合并订单和退货表
    val mergedOrderReturn = orderDF.join(
      returnDF,
      Seq("订单 ID"),
      "left"
    ).withColumnRenamed("退回", "IsReturned")

    // 3. 合并销售人员信息
    val finalDF = mergedOrderReturn.join(
      personDF,
      Seq("地区"),
      "left"
    )

    // 4. 数据清洗处理
    val processedDF = finalDF
      .drop("行 ID") // 删除指定列
      .na.fill(Map("IsReturned" -> "否")) // 填充缺失值
      .dropDuplicates() // 最终去重

    processedDF.show(10)
    processedDF.write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(output)
    // 关闭Spark会话
    spark.stop()
  }
}
