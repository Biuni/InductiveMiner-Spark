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
  println("* Università Politecnica delle Marche *")
  println("\n")

  // Example: Read log from Hadoop hosted file
  // val log = sc.textFile("hdfs://....")

  val log = List(
    List("a","b","c"),
    List("a","c","b","h","c"),
    List("a","d","e"),
    List("a","d","e","f","d","e")
  )
  /*val log = List(
    List("b","c"),
    List("c","b","h","c"),
    List("d","e"),
    List("d","e","f","d","e")
  )*/

  IMFramework(log, sc)

  sc.stop

}
