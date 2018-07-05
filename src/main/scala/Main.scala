import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

import IM.IMFramework._

object Main extends App {

  val conf = new SparkConf()
    .setAppName("InductiveMIner")
    .setMaster("local[1]")
  val sc = new SparkContext(conf)
  val rootLogger = Logger
    .getRootLogger()
    .setLevel(Level.ERROR)

  println("\n")
  println("*** Inductive Miner on Apache Spark ***")
  println("** Gianluca Bonifazi - Gianpio Sozzo **")
  println("** TEST **)
  println("\n")

  // Example: Read log from Hadoop hosted file
  // val log = sc.textFile("hdfs://....")

  val log = List(
    List("a","b","d","f"),
    List("a","c","e","f"),
    List("a","c","d","f"),
    List("a","c","d","f")
  )

  IMFramework(log, sc)

  sc.stop

}
