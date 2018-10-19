import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

import IM.IMFramework._
import IM.Utilities._

object Main extends App {

  // Start Time
  val t1 = System.nanoTime()

  val conf = new SparkConf()
    .setAppName("InductiveMIner")
    .setMaster("local[4]")
  val sc = new SparkContext(conf)
  val rootLogger = Logger
    .getRootLogger()
    .setLevel(Level.ERROR)

  println("\n")
  printColor("yellow", "*** Inductive Miner on Apache Spark ***")
  printColor("yellow", "** Gianluca Bonifazi - Gianpio Sozzo **")
  printColor("yellow", "* Università Politecnica delle Marche *")
  println("\n")

  val params = readParams(args, sc)

  // Create a list of list from the Log File
  val log = sc.textFile(params._1)
            .map(line => line.split(",").toList)
            .collect.toList

  // Create the DFG
  var graph = createDFG(log, params._2, sc)

  graph.cache()

  // Print to terminal the DFG
  printDFG(graph, false)

  // Run the inductive miner core
  IMFramework(graph, params._2, params._3)

  // Execution Time
  val duration = (System.nanoTime()-t1)/1e9d
  println("EXECUTION TIME: " +duration+ " seconds.")

  sc.stop

}
