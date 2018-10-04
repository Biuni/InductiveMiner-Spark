package IM

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd._

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.util.MurmurHash

object Utilities {

  /**
  * chooseLog
  * 
  * Choose the log file
  */
  def chooseLog() : (String) = {

    printColor("cyan", "List of available log files.\n")

    val dirName = System.getProperty("user.dir") + "/src/main/scala/logExample"
    val listOfLog = new java.io.File(dirName).listFiles.filter(_.isFile)
      .filter(_.getName.endsWith(".txt"))
      .map(_.getPath).toList

    listOfLog.zipWithIndex.foreach {
      case(file, count) => println(s"$count) $file")
    }

    val maxNumber = listOfLog.length - 1
    print("\n~ Digit (from 0 to "+ maxNumber +"): ")
    var choose: Int = scala.io.StdIn.readInt()

    while(choose < 0 || choose > maxNumber) {
      printColor("red", "* File not valid! *")
      print("\n~ Digit (from 0 to "+ maxNumber +"): ")
      choose = scala.io.StdIn.readInt()
    }

    println()

    (listOfLog(choose))
  }

  /**
  * chooseIM
  * 
  * Choose the type of Inductive Miner
  */
  def chooseIM() : (Boolean, Float) = {

    printColor("cyan", "Choose a variant of Inductive Miner:\n")
    printColor("cyan", "1 - Inductive Miner (IM)")
    printColor("cyan", "2 - Inductive Miner - infrequent (IMf)\n")

    print("~ Digit (1 or 2): ")

    var IMf: Boolean = false
    var threshold: Float = -1
    var choose: Int = scala.io.StdIn.readInt()

    if(choose == 2) {
      IMf = true
      print("~ Set a noise threshold (0 to 1): ")
      threshold = scala.io.StdIn.readFloat()
      while(threshold < 0 || threshold > 1) {
        printColor("red", "* Threshold not valid! *")
        print("~ Set a noise threshold (0 to 1): ")
        threshold = scala.io.StdIn.readFloat()
      }
    }

    (IMf, threshold)
  }

  /**
  * createDFG
  *
  * Create the Direct Follow Graph using
  * the graphx library.
  *
  */
  def createDFG(log: List[List[String]], imf: Boolean, sc: SparkContext) : Graph[String,String] = {

    val vertices = log.flatMap(x => x).toSet.toSeq
    val vertexMap = (0 until vertices.size)
      .map(i => vertices(i) -> i.toLong)
      .toMap
    val vertexNames = sc.parallelize(vertexMap.toSeq.map(_.swap))

    // Create the DFG without count frequencies
    if(!imf) {
      val edgeSet = log
        .filter(_.size > 1)
        .flatMap(list => list.indices.tail.map(i => list(i-1) -> list(i)))
        .map(x => Edge(vertexMap(x._1), vertexMap(x._2), "1"))
        .toSet
      val edges = sc.parallelize(edgeSet.toSeq)
      
      Graph(vertexNames, edges)

    // Create the DFG with frequencies stored into edges weight
    } else {
      val edgeSet2 = log
        .filter(_.size > 1)
        .flatMap(list => list.indices.tail.map(i => list(i-1) -> list(i))).toList
      val edgeSet = edgeSet2
        .map(x => Edge(vertexMap(x._1), vertexMap(x._2), edgeSet2.count(_ == x).toString))
        .toSet
      val edges = sc.parallelize(edgeSet.toSeq)

      Graph(vertexNames, edges)
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
