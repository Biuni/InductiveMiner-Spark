import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

import IM.IMFramework._
import IM.Utilities._

object Main extends App {

  val conf = new SparkConf()
    .setAppName("InductiveMIner")
    .setMaster("local[1]")
  val sc = new SparkContext(conf)
  val rootLogger = Logger
    .getRootLogger()
    .setLevel(Level.ERROR)

  println("\n")
  printColor("yellow", "*** Inductive Miner on Apache Spark ***")
  printColor("yellow", "** Gianluca Bonifazi - Gianpio Sozzo **")
  printColor("yellow", "* Università Politecnica delle Marche *")
  println("\n")

  // Example: Read log from Hadoop hosted file
  // val log = sc.textFile("hdfs://....")

  val log = List(
    List("a","b","c"),
    List("a","c","b","h","c"),
    List("d","e"),
    List("d","e","f","d","e")
  )

  val IMtype = chooseIM()
  
  IMFramework(log, sc, IMtype._1, IMtype._2)

  sc.stop

}
