package org.example.demo

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkAnalysis {
  def main(args: Array[String]): Unit = {
    val path = if (args == null || args.length == 0) "output/" else args(0)
    // 创建Spark会话
    val spark = SparkSession.builder()
      .appName("SparkAnalysis")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    // 1. 读取原始数据并去重
    val processedDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

    // 时间维度扩展
    val dfWithTime = processedDF
      .withColumn("订单日期_年", year(col("订单日期")))
      .withColumn("订单日期_月", month(col("订单日期")))

    // 订单量及销售额年变化趋势分析
    val annualTrend = dfWithTime.groupBy("订单日期_年")
      .agg(
        countDistinct("订单 ID").as("订单量"),
        sum("销售额").as("销售额"),
        sum("利润").as("利润")
      )
      .withColumn("订单增长率",
        (col("订单量") - lag("订单量", 1).over(Window.orderBy("订单日期_年")))
          / lag("订单量", 1).over(Window.orderBy("订单日期_年")))
      .withColumn("销售额增长率",
        (col("销售额") - lag("销售额", 1).over(Window.orderBy("订单日期_年")))
          / lag("销售额", 1).over(Window.orderBy("订单日期_年")))
      .withColumn("利润增长率",
        (col("利润") - lag("利润", 1).over(Window.orderBy("订单日期_年")))
          / lag("利润", 1).over(Window.orderBy("订单日期_年")))
      .na.fill(0)
    annualTrend.show(10)
    // 利润及退货次数年变化趋势分析
    val returnAnalysis = dfWithTime
      .filter(col("IsReturned") === "是")
      .groupBy("订单日期_年")
      .agg(count("IsReturned").as("退货量"))
      .join(
        dfWithTime.groupBy("订单日期_年").agg(count("订单 ID").as("总订单量")),
        Seq("订单日期_年")
      )
      .withColumn("退货率", col("退货量") / col("总订单量"))
    returnAnalysis.show(10)

    // ---------------------------------季节性分析
    val monthlySales = dfWithTime
      .groupBy("订单日期_年", "订单日期_月")
      .agg(sum("销售额").as("total_sales"))
      .orderBy("订单日期_年", "订单日期_月")
    monthlySales.show(10)
    // ---------------------------------地区销售额及利润差异分析
    val regionAnalysis = dfWithTime.groupBy("地区")
      .agg(
        sum("销售额").as("销售额总和"),
        sum("利润").as("利润总和")
      )
    regionAnalysis.show(10)

    // ---------------------------------省份销售额及利润详情
    val provSales = dfWithTime
      .groupBy("省/自治区")
      .agg(
        sum("销售额").as("销售额总和"),
        sum("利润").as("利润总和")
      )
    provSales.show(10)
    // ---------------------------------产品分析
    val productAnalysis = dfWithTime.groupBy("类别", "子类别")
      .agg(
        sum("销售额").as("销售额总和"),
        sum("利润").as("利润总和"),
        avg("折扣").as("平均折扣率"),
        countDistinct("订单 ID").as("订单量"),
      )
    productAnalysis.show(10)

    // ---------------------------------产品分析
    val productReturnAnalysis = dfWithTime
      .filter(col("IsReturned") === "是")
      .groupBy("子类别")
      .agg(count("IsReturned").as("退货量"))
      .join(
        dfWithTime.groupBy("子类别").agg(count("订单 ID").as("总订单量")),
        Seq("子类别")
      )
      .withColumn("退货率", col("退货量") / col("总订单量"))
    productReturnAnalysis.show(10)

    // ---------------------------------客户分析
    val clientAnalysis = dfWithTime.groupBy("细分")
      .agg(
        countDistinct("订单 ID").as("订单量"),
        countDistinct("客户 ID").as("客户数")
      )
    clientAnalysis.show(10)
    val properties = new java.util.Properties()
    val jdbcUrl = "jdbc:mysql://master:3306/echarts?characterEncoding=UTF-8&useUnicode=true&useSSL=false&tinyInt1isBit=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai"
    properties.put("user", "root")
    properties.put("password", "123456")
    properties.put("driver", "com.mysql.cj.jdbc.Driver")
    annualTrend.withColumnRenamed("订单日期_年", "order_date_year")
      .withColumnRenamed("订单量", "order_quantity")
      .withColumnRenamed("销售额", "sales_amount")
      .withColumnRenamed("利润", "profit")
      .withColumnRenamed("订单增长率", "order_growth_rate")
      .withColumnRenamed("销售额增长率", "sales_growth_rate")
      .withColumnRenamed("利润增长率", "profit_growth_rate")
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(jdbcUrl, "annual_trend", properties)

    returnAnalysis.withColumnRenamed("订单日期_年", "order_date_year")
      .withColumnRenamed("退货量", "return_quantity")
      .withColumnRenamed("总订单量", "total_order_quantity")
      .withColumnRenamed("退货率", "return_rate")
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(jdbcUrl, "return_analysis", properties)

    monthlySales.withColumnRenamed("订单日期_年", "order_date_year")
      .withColumnRenamed("订单日期_月", "order_date_month")
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(jdbcUrl, "monthly_sales", properties)

    regionAnalysis.withColumnRenamed("地区", "region")
      .withColumnRenamed("销售额总和", "sales_amount_sum")
      .withColumnRenamed("利润总和", "profit_sum")
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(jdbcUrl, "region_analysis", properties)

    provSales.withColumnRenamed("省/自治区", "province")
      .withColumnRenamed("销售额总和", "sales_amount_sum")
      .withColumnRenamed("利润总和", "profit_sum")
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(jdbcUrl, "prov_sales", properties)

    productAnalysis.withColumnRenamed("类别", "category")
      .withColumnRenamed("子类别", "subcategory")
      .withColumnRenamed("销售额总和", "sales_amount_sum")
      .withColumnRenamed("利润总和", "profit_sum")
      .withColumnRenamed("平均折扣率", "average_discount_rate")
      .withColumnRenamed("订单量", "order_quantity")
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(jdbcUrl, "product_analysis", properties)

    clientAnalysis.withColumnRenamed("细分", "segment")
      .withColumnRenamed("订单量", "order_quantity")
      .withColumnRenamed("客户数", "customer_count")
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(jdbcUrl, "client_analysis", properties)
    productReturnAnalysis.withColumnRenamed("子类别", "subcategory")
      .withColumnRenamed("退货量", "return_quantity")
      .withColumnRenamed("退货率", "return_rate")
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(jdbcUrl, "product_return_analysis", properties)
    // 关闭Spark会话
    spark.stop()
  }
}
