package com.bdm

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.{Logger,Level}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructType, StructField,  LongType, StringType}
//import org.apache.parquet.format.StringType

object Question1 {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)

    //Create conf object
    val conf = new SparkConf().setMaster("local[2]").setAppName("loadData")

    //create spark context object
    val sc = new SparkContext(conf)

    val SQLContext = new SQLContext(sc)
    import SQLContext.implicits._

    //Read file and create RDD

    val table_schema = StructType(Seq(
      StructField("TransID", LongType, true),
      StructField("CustID", LongType, true),
      StructField("TransTotal", LongType, true),
      StructField("TransNumItems", LongType, true),
      StructField("TransDesc", StringType, true)
    ))

    val T = SQLContext.read
      .format("csv")
      .schema(table_schema)
      .option("header","false")
      .option("nullValue","NA")
      .option("delimiter",",")
      .load(args(0))

    //    T.show(5)

    val T1 = T.filter($"TransTotal" >= 200)
    //    T1.show(5)

    val T2 = T1.groupBy("TransNumItems").agg(sum("TransTotal"), avg("TransTotal"),
      min("TransTotal"), max("TransTotal"))
    //    T2.show(500)

    T2.show()

    val T3 =  T1.groupBy("CustID").agg(count("TransID").as("number_of_transactions_T3"))
    //    T3.show(50)

    val T4 = T.filter($"TransTotal" >= 600)
    //    T4.show(5)

    val T5 = T4.groupBy("CustID").agg(count("TransID").as("number_of_transactions_T5"))
    //    T5.show(50)

    val temp = T3.as("T3").join(T5.as("T5"), ($"T3.CustID" === $"T5.CustID") )
    //    T6.show(5)
    //    print(T6.count())
    val T6 = temp.where(($"number_of_transactions_T5")*5 < $"number_of_transactions_T3")
    //    T6.show(5)

    T6.show()

    sc.stop
  }
}