package com.bdm

import org.apache.log4j.{Logger,Level}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType};
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object Question3 {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)

    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkPageRank")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val schema = StructType(Seq(StructField("from_node", LongType, true),
      StructField("to_node", LongType, true)))

    val L0 = sqlContext.read
      .format("csv")
      .schema(schema)
      .option("header","true")
      .option("delimiter", "\\t")
      .load(args(0));
    L0.persist()

    val distinct_from = L0.select(L0("from_node")).distinct
    val distinct_to = L0.select(L0("to_node")).distinct

    var all_nodes = distinct_from.union(distinct_to)
    all_nodes = all_nodes.withColumnRenamed("from_node", "url").distinct()

    val R0 = all_nodes.orderBy("url")
    var rank_table = R0.withColumn("Rank",lit(1))

    val iterations = 50

    for (i <- 1 to iterations) {
      val source_count = L0.groupBy("from_node").agg(count("*") as "count").orderBy("from_node")
      val joined = L0.as('L0).join(  source_count.as('source_count),  $"L0.from_node" === $"source_count.from_node").drop($"source_count.from_node")
      val rank = joined.as('joined).join(  rank_table.as('rank_table),  $"joined.from_node" === $"rank_table.url").drop($"rank_table.url")

      val new_rank_table = rank.withColumn("new_rank", $"Rank" / $"count")
      val new_rank_table_2 = new_rank_table.drop("Rank").withColumnRenamed("new_rank", "Rank")
      val new_rank_table_final = new_rank_table_2.groupBy("to_node").agg(sum("Rank") as "Rank").orderBy("to_node").withColumnRenamed("to_node", "url")

      val update = rank_table.as('rank_table).join(new_rank_table_final.as('new_rank_table_final), $"rank_table.url" === $"new_rank_table_final.url", "left_outer").drop($"rank_table.Rank").drop($"new_rank_table_final.url").orderBy("url")
      rank_table = update
    }

    print("Top 100 nodes along with their	final	rank:")
    rank_table.show(100)

    sc.stop

  }

}