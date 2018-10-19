package IM

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd._

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.util.MurmurHash

object Utilities {

  /**
  * readParams
  * 
  * Read the parameters
  */
  def readParams(args: Array[String], sc: SparkContext) : (String, Boolean, Float) = {
    
    var logFile: String = ""
    var IMType: Boolean = true
    var Threshold: Float = -1

    if(args.length > 1) {
      if(new java.io.File(args(0)).exists) {
        logFile = args(0)
      } else {
        printColor("red", "Attenzione! Percorso del file errato. (PS: è richiesto il percorso assoluto)")
        sc.stop
        sys.exit(0)
      }
      if(args(1) == "IM") {
        // IM standard
        IMType = false
      } else {
        if(args.length == 3) {
          Threshold = args(2).toFloat
        } else {
          printColor("red", "Attenzione! Soglia non impostata.")
          sc.stop
          sys.exit(0)
        }
      }
    } else {
      printColor("red", "Attenzione! Parametri mancanti.")
      sc.stop
      sys.exit(0)
    }

    (logFile, IMType, Threshold)
  }

  /**
  * createDFG
  *
  * Create the Direct Follow Graph using
  * the Graphx library.
  *
  */
  def createDFG(log: List[List[String]], imf: Boolean, sc: SparkContext) : Graph[String,String] = {

    val vertices = log.flatMap(x => x).toSet.toSeq
    val vertexMap = (0 until vertices.size)
      .map(i => vertices(i) -> i.toLong)
      .toMap
    val vertexNames = sc.parallelize(vertexMap.toSeq.map(_.swap))

    // Create the DFG without counts frequencies
    if(!imf) {
      val edgeSet = log
        .filter(_.size > 1)
        .flatMap(list => list.indices.tail.map(i => list(i-1) -> list(i)))
        .map(x => Edge(vertexMap(x._1), vertexMap(x._2), "1"))
        .toSet
      var edges = sc.parallelize(edgeSet.toSeq)
      
      Graph(vertexNames, edges)

    // Create the DFG with frequencies stored into edges weight
    } else {
      val edgeSet2 = log
        .filter(_.size > 1)
        .flatMap(list => list.indices.tail.map(i => list(i-1) -> list(i))).toList
      val edgeSet = edgeSet2
        .map(x => Edge(vertexMap(x._1), vertexMap(x._2), edgeSet2.count(_ == x).toString))
        .toSet
      var edges = sc.parallelize(edgeSet.toSeq)
      var edges1 = edges.sortBy(e => e.srcId)

      Graph(vertexNames, edges1)
    }
  }

  /**
  * countCC
  *
  * Count the connected 
  * components of the graph.
  */
  def countCC(graph: Graph[String,String]) : (Long, Array[Long]) = {

    val components = graph.connectedComponents().vertices.cache()
    val countCC = components.map{ case(_,cc) => cc }.distinct.count()
    val getCC = components.map{ case(_,cc) => cc }.distinct.collect()
    
    (countCC, getCC)
  }

  /**
  * printDFG
  * 
  * Print the Direct Follow Graph
  */
  def printDFG(graph: Graph[String,String], filtered: Boolean) : Unit = {

    printColor("purple", "\n*********************************")
    printColor("purple", "*** DFG - Direct Follow Graph ***")
    if(filtered) {
      printColor("purple", "*********** FILTERED ************")
    }
    printColor("purple", "*********************************")
    printColor("blue", "------------- Edges -------------")
    graph.edges.foreach(println)
    printColor("blue", "------------ Vertices -----------")
    graph.vertices.foreach(println)
    printColor("purple", "*********************************")
    println("\n")

  }

  /**
  * printColor
  * 
  * Print colored text into console
  */
  def printColor(color: String, text: String) : Unit = {

    val colorCode = color match {
      case "black"  => "30"
      case "red"    => "31"
      case "green"  => "32"
      case "yellow" => "33"
      case "blue"   => "34"
      case "purple" => "35"
      case "cyan"   => "36"
      case "white"  => "37"
    }
 
    println("\u001b[" + colorCode + "m" + text + "\u001b[0m")

  }

  /**
  * printCut
  * 
  * Print the found cut
  */
  def printCut(cut: List[List[String]]) : Unit = {

    val typeOfCut = cut(0)(0) match {
      case "X"  => "- xorCut: "
      case "-->" => "- seqCut: "
      case "||" => "- concurrentCut: "
      case "*"  => "- loopCut: "
    }

    printColor("green", typeOfCut + cut + "\n")

  }

}
