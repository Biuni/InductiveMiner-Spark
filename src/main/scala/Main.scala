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

  val log = sc.textFile("/home/biuni/Scrivania/InductiveMiner-Spark/src/main/scala/logExample/log2.txt")
            .map(line => line.split(",").toList)
            .collect.toList

  val IMtype = chooseIM()
  var graph = createDFG(log, IMtype._1, sc)

  printDFG(graph, false)

  IMFramework(graph, IMtype._1, IMtype._2)

  sc.stop

}
