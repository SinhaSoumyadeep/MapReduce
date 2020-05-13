package wc


import java.io.{BufferedWriter, FileWriter, PrintWriter}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
 * This is the main class and it will be use to perform all the rdd operations, dataframe operations mentioned
 * in the question and this class will also be responsible for determining the triangle count with various methods
 * like Reduce side join (RDD and DF) and Replicated join(RDD and DF)
 */
object TwitterCountMain {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\nwc.TwitterCountMain <input dir> <output dir> <max filter>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("TwitterCount")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    val textFile = sc.textFile(args(0))
    //rdda(textFile, args)

    triangleCountReduceSideJoinRDD(textFile, args)
    //triangleCountReduceSideJoinDataFrame(args)

    //triangleCountReplicatedJoinRDD(textFile, args)
    //triangleCountReplicatedJoinDataFrame(args)

    //rddr(textFile, args)


  }

  /**
   * This method is used to implement the reduce by key operation.
   *
   * @param textFile the text file.
   * @param args     the arguments.
   */
  def rddr(textFile: RDD[String], args: Array[String]) {
    val count = textFile.flatMap(line => line.split(" ")).map(word => {
      val usr = word.split(",")
      (usr(1), 1)
    }).reduceByKey(_ + _)

    println(count.toDebugString)

    count.saveAsTextFile(args(1))

  }

  /**
   * This method is used to implement the group by key operation.
   *
   * @param textFile the text file.
   * @param args     the args
   */
  def rddg(textFile: RDD[String], args: Array[String]) {
    val count = textFile.flatMap(line => line.split(" ")).map(word => {
      val usr = word.split(",")
      (usr(1), 1)
    }).groupByKey().map((x) => (x._1, x._2.sum))

    println(count.toDebugString)

    count.saveAsTextFile(args(1))

  }

  /**
   * This method is used to implement the fold by key operation.
   *
   * @param textFile the text file
   * @param args     the argumnets.
   */
  def rddf(textFile: RDD[String], args: Array[String]) {
    val count = textFile.flatMap(line => line.split(" ")).map(word => {
      val usr = word.split(",")
      (usr(1), 1)
    }).foldByKey(0)(_ + _)

    println(count.toDebugString)

    count.saveAsTextFile(args(1))
  }

  /**
   * This method is used to implement the aggregate by key operation.
   *
   * @param textFile the text file.
   * @param args     the arguments.
   */
  def rdda(textFile: RDD[String], args: Array[String]) {
    val count = textFile.flatMap(line => line.split(" ")).map(word => {
      val usr = word.split(",")
      (usr(1), 1)
    }).aggregateByKey(0)(_ + _, _ + _)

    println(count.toDebugString)

    count.saveAsTextFile(args(1))
  }

  /**
   * The grouping and aggregation implemententation using DataSet, with groupBy on the appropriate column
   *
   * @param args the arguments.
   */
  def dset(args: Array[String]) {
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("TwitterCount")
      .getOrCreate()

    import sparkSession.implicits._
    val data = sparkSession.read.text(args(0)).as[String]

    val count = data.flatMap(line => line.split(" ")).map(word => {
      val usr = word.split(",")
      (usr(1), 1)
    }).groupByKey(_._1).count()

    print(count.explain())

    count.coalesce(1).write.csv(args(1))

  }


  /**
   * This method is used to perform the max filter based on the MAX_FILTER that is provided as an argument to the
   * program.
   *
   * @param textFile the text file.
   * @param args the arguments.
   */
  def max_filter(textFile: RDD[String], args: Array[String]) {

    val count = textFile.flatMap(line => line.split(" ")).filter(word => {
      val usr = word.split(",")
      usr(0).toInt < args(2).toInt && usr(1).toInt < args(2).toInt
    })

    count.coalesce(1) saveAsTextFile (args(1))

  }

  /**
   * This method is used to implement the triangle count program using Reduce side join and RDD.
   * @param textFile1 the text file
   * @param args the arguments.
   */
  def triangleCountReduceSideJoinRDD(textFile1: RDD[String], args: Array[String]) {

    val textFile = textFile1.flatMap(line => line.split(" ")).filter(word => {
      val usr = word.split(",")
      usr(0).toInt < args(2).toInt && usr(1).toInt < args(2).toInt
    })

    val from = textFile.flatMap(line => line.split(" ")).map(
      word => {
        val usr = word.split(",")
        (usr(0), usr(1))
      }
    )

    val to = textFile.flatMap(line => line.split(" ")).map(
      word => {
        val usr = word.split(",")
        (usr(1), usr(0))
      }
    )

    val twoPath = from.join(to)
    val twoPathReverse = twoPath.map(x => {
      ((x._2._1, x._2._2), x._1)
    })
    val reverseFollower = textFile.map(word => {
      val usr = word.split(",")
      ((usr(0), usr(1)), null)
    })

    val triangle = twoPathReverse.join(reverseFollower)

    //This is to count the triangle and save the file.
    val countT = triangle.map(e => {
      ("The Triangle count in ReduceSideJoinRDD: ", 1)
    }).reduceByKey(_ + _).map(res => {
      (res._1, res._2.toInt / 3)
    })

    countT.coalesce(1).saveAsTextFile(args(1))


  }

  /**
   * This method is used to implement the triangle count program using Reduce side join and DF.
   *
   * @param args the arguments.
   */
  def triangleCountReduceSideJoinDataFrame(args: Array[String]) {
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("TwitterCount")
      .getOrCreate()

    import sparkSession.implicits._
    val data1 = sparkSession.read.text(args(0)).as[String]

    val data = data1.flatMap(line => line.split(" ")).filter(word => {
      val usr = word.split(",")
      usr(0).toInt < args(2).toInt && usr(1).toInt < args(2).toInt
    })

    val user = data.map(word => {
      val usr = word.split(",")
      (usr(0), usr(1))
    })

    val table = user.toDF("follower", "user")
    val duplicate = table.toDF("follower1", "user1")

    val twoPath = table.join(duplicate, table("user") <=> duplicate("follower1"))
    twoPath.show()

    val newTable = table.toDF("follower2", "user2")
    val threePath = twoPath.join(newTable, twoPath("user1") === newTable("follower2") && twoPath("follower") === newTable("user2"))

    //This is to count the triangle and save the file.
    val countT = threePath.rdd.map(e => {
      ("The Triangle count in ReduceSideJoinDataFrame: ", 1)
    }).reduceByKey(_ + _).map(res => {
      (res._1, res._2.toInt / 3)
    })

    countT.coalesce(1).saveAsTextFile(args(1))


  }

  /**
   * This method is used to implement the triangle count program using Replicated join and RDD.
   * @param textFile1 the text file
   * @param args the arguments.
   */
  def triangleCountReplicatedJoinRDD(textFile1: RDD[String], args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("TwitterCount")
      .getOrCreate()

    val textFile = textFile1.flatMap(line => line.split(" ")).filter(word => {
      val usr = word.split(",")
      usr(0).toInt < args(2).toInt && usr(1).toInt < args(2).toInt
    })

    val table = textFile.map(edges => {
      val users = edges.split(",");
      (users(0), users(1))
    }).groupBy(_._1).mapValues(_.map(_._2).toList)

    val dc = table.collectAsMap()
    val distributedCache = sparkSession.sparkContext.broadcast(dc)

    val countT = textFile.flatMap(line => line.split(" ")).map(word => {
      val usr = word.split(",")

      val follower = usr(0)
      val user = usr(1)

      var c = 0
      if (distributedCache.value.get(user) != None) {
        val neigh = distributedCache.value(user)

        neigh foreach (n => {
          if (distributedCache.value.get(n) != None) {
            val nn = distributedCache.value(n)
            if (nn.contains(follower)) {
              c = c + 1
            }
          }
        })


      }

      ("The Triangle count in ReplicatedJoinRDD:", c)

    }).reduceByKey(_ + _).map(res => {
      (res._1, res._2.toInt / 3)
    })


    countT.coalesce(1).saveAsTextFile(args(1))

  }

  /**
   * This method is used to implement the triangle count program using Replicated join and DF.
   *
   * @param args the arguments.
   */
  def triangleCountReplicatedJoinDataFrame(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("TwitterCount")
      .getOrCreate()

    import sparkSession.implicits._

    val data1 = sparkSession.read.text(args(0)).as[String]

    val data = data1.flatMap(line => line.split(" ")).filter(word => {
      val usr = word.split(",")
      usr(0).toInt < args(2).toInt && usr(1).toInt < args(2).toInt
    })

    val users = data.map(word => {
      val usr = word.split(",")
      (usr(0), usr(1))
    })

    val broadcast = sparkSession.sparkContext.broadcast(users)

    val table = broadcast.value.toDF("follower", "user")
    val duplicate = broadcast.value.toDF("follower1", "user1")


    val twoPath = table.join(duplicate, table("user") <=> duplicate("follower1"))
    //twoPath.show()

    val newTable = table.toDF("follower2", "user2")
    val threePath = twoPath.join(newTable, twoPath("user1") === newTable("follower2") && twoPath("follower") === newTable("user2"))
    //println("------>>>  "+threePath.count()/3)

    //threePath.coalesce(1).write.csv(args(1))


    //This is to count the triangle and save the file.
    val countT = threePath.rdd.map(e => {
      ("The Triangle count in ReplicatedJoinDataFrame: ", 1)
    }).reduceByKey(_ + _).map(res => {
      (res._1, res._2.toInt / 3)
    })

    countT.coalesce(1).saveAsTextFile(args(1))

  }


}
