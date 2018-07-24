import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable.ListBuffer

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

  /*val log = List(
    List("a","b","c"),
    List("a","c","b","h","c"),
    List("a","d","e"),
    List("a","d","e","f","d","e")
  )*/

  val log = List(
    List("a","z","b","c"),
    List("a","z","c","b","h","c"),
    List("a","z","d","e"),
    List("a","z","d","e","f","d","e"),
    List("a","z","r","t"),
    List("a","z","t","t","r","g","r")
  )

  /*val log = List(
    List("b","c"),
    List("c","b","h","c"),
    List("d","e"),
    List("d","e","f","d","e")
  )*/

  /*val log = List(
    List("z","b","c"),
    List("z","c","b","h","c"),
    List("z","d","e"),
    List("z","d","e","f","d","e")
  )*/

  /*val log = List(
    List("b","c","a"),
    List("c","b","h","c","a"),
    List("d","e","a"),
    List("d","e","f","d","e","a")
  )*/

  /*val log = List(
    List("b","c","z"),
    List("c","b","h","c","z"),
    List("d","e","z"),
    List("d","e","f","d","e","z")
  )*/

  IMFramework(log, sc)

  sc.stop

}
