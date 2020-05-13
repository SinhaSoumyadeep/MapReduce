package wc


import java.io.{BufferedWriter, FileWriter, PrintWriter}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable


/**
 * This is the main class and it will be use to perform all the rdd operations, dataframe operations mentioned
 * in the question and this class will also be responsible for determining the triangle count with various methods
 * like Reduce side join (RDD and DF) and Replicated join(RDD and DF)
 */
object TwitterCountMain {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 4) {
      logger.error("Usage:\nwc.TwitterCountMain <input dir> <output dir> <max filter>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("TwitterCount")
    val sc = new SparkContext(conf)
    print(args(3))
    val k = args(3).toInt
    generateGraph(args, k)
    generateInitialRank(args, k)
    generateVertex(args, k)

    val ranks = sc.textFile(args(0) + "/rank.txt").flatMap(line => line.split(" ")).map(ranks => {
      val r = ranks.split(",")
      (r(0), r(1).toDouble)
    })
    val links = sc.textFile(args(0) + "/graph.txt").flatMap(line => line.split(" ")).map(edge => {
      val user = edge.split(",")
      (user(0), user(1))
    })
    val vertex = sc.textFile(args(0) + "/vertex.txt").flatMap(line => line.split(" ")).map(edge => {
      val v = edge.split(",")
      (v(0), v(1).toDouble)
    })

    pageRankRDD(args, k, ranks, links, vertex)
    //pageRankDF(args, k, ranks, links, vertex)

  }

  def pageRankDF(args: Array[String], k: Int, x: RDD[(String, Double)], links: RDD[(String, String)], vertex: RDD[(String, Double)]): Unit = {
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("TwitterCount")
      .getOrCreate()

    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    var rankTable = x.toDF("vertex", "rank")
    val linkTable = links.toDF("src", "dest")
    val vertexTable = vertex.toDF("vertex", "notused")

    for (i <- 1 to args(2).toInt) {
      rankTable.persist()
      linkTable.persist()
      vertexTable.persist()
      val pr = linkTable.join(rankTable, linkTable("src") <=> rankTable("vertex"))
      val temp = pr.drop("vertex").drop("src").groupBy("dest").sum("rank")
      val delta = temp.filter($"dest" === "0").drop("dest").first().getDouble(0) / (k * k)
      val r = vertexTable.join(temp, vertexTable("vertex") === temp("dest"), "left_outer").drop("dest").drop("notused")
      var nr = r.toDF("v", "r")
      nr = nr.na.fill(0.0)
      nr = nr.select($"v", $"r" + delta).toDF("vertex", "rank")
      nr = nr.withColumn("rank", when(col("vertex") === 0, 0.0).otherwise(col("rank")))
      rankTable = nr
    }

    rankTable.coalesce(1).write.csv(args(1) + "[Data frame]")

  }

  def pageRankRDD(args: Array[String], k: Int, x: RDD[(String, Double)], links: RDD[(String, String)], vertex: RDD[(String, Double)]) {
    var ranks = x
    for (i <- 1 to args(2).toInt) {
      ranks.persist()
      links.persist()
      vertex.persist()
      val tempRank = links.join(ranks).map((k) => (k._2._1, k._2._2)).reduceByKey(_ + _)
      val delta: Double = tempRank.filter((x) => (x._1 == "0")).first()._2 / (k * k)
      val temp3 = tempRank.rightOuterJoin(vertex).
        map { case (x, (None, y)) => (x, y)
        case (x, (Some(z), y)) => {
          if (x == "0") {
            ("0", 0.toDouble)
          } else {
            (x, z)
          }
        }
        }
      ranks = temp3.map { (x) => {
        if (x._1 == "0") {
          (x._1, x._2)
        } else {
          (x._1, x._2 + delta)
        }
      }
      }
      println("the lineage of "+i+" iteration")
      println(ranks.toDebugString)
    }

    ranks.coalesce(1).saveAsTextFile(args(1) + "[RDD]")

  }

  def generateGraph(args: Array[String], k: Int) {
    var graph = ""
    for (a <- 1 to k * k) {
      if (a % k == 0) {
        graph += a.toString + "," + "0" + "\n"
      }
      else {
        graph += a.toString + "," + (a + 1).toString + "\n"
      }
    }

    new PrintWriter(args(0) + "/graph.txt") {
      write(graph);
      close
    }
  }

  def generateInitialRank(args: Array[String], k: Int) {

    var rank = ""
    for (a <- 0 to k * k) {
      if (a == 0) {
        rank = rank + a.toString + "," + (0).toString + "\n"
      }
      else {
        rank = rank + a.toString + "," + (1.toFloat / (k * k)).toString + "\n"
      }

    }

    new PrintWriter(args(0) + "/rank.txt") {
      write(rank);
      close
    }
  }

  def generateVertex(args: Array[String], k: Int) {
    var v = ""
    for (a <- 0 to k * k) {
      v = v + a.toString + "," + (0).toString + "\n"
    }
    new PrintWriter(args(0) + "/vertex.txt") {
      write(v);
      close
    }
  }

}
