package IM

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd._

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.util.MurmurHash

object Utilities {

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

  // Get all activities in the log
  def checkActivities(log: List[List[String]], sc: SparkContext) : List[String] = {
    val rddLog = sc.parallelize(log)
    val activities = rddLog.distinct().collect().toList.flatten.distinct.sorted
    activities
  }
  def printDFG(mAtr: Array[Array[Int]], sigma: List[String]) = {

    val arr = sigma.toArray.sorted
    print("       ")
    for(a<-0 until sigma.length){
      print(arr(a)+" ")
    }
    println("\n")
    for(a<-0 until sigma.length){
      print("   "+arr(a)+"   ")
      for(b<-0 until sigma.length){
        print(mAtr(a)(b)+" ")
      }
      println()
    }
    println("\n")

  }

  def OLDcreateDFG(log: List[List[String]], sc: SparkContext, imf: Boolean) : (Array[Array[Int]], List[String], Array[Array[Int]]) = {
    //val rddLog = sc.parallelize(log)
    //val activities = rddLog.distinct().collect().toList.flatten.distinct.sorted

    val activities = checkActivities(log,sc)

    val rddEdges = sc.parallelize(log)
    var edges = rddEdges.map(list =>{
      var aux = new ListBuffer[(String,String)]()
      for(a <- 0 until list.length-1){
        aux.+=((list(a),list(a+1)))
      }
      aux.toList
    }).collect().toList.flatten

    var matrix = new Array[Array[Int]](activities.length)
    var matrixFreq = new Array[Array[Int]](activities.length)
    var rddActivities = sc.parallelize(activities)

    var arrT = rddActivities.map(list => {
      var riga = activities.indexOf(list)
      var arr =  new Array[Int](activities.length)
      for(a<-0 until activities.length){
        if(edges.contains((list,activities(a)))){
          arr.update(a, 1)
        }
      }
      (riga,arr)
    }).sortByKey(true).collect()

    if(imf) {
      var arrFreq = rddActivities.map(list => {
        var riga = activities.indexOf(list)
        var arrFreq =  new Array[Int](activities.length)
        for(a<-0 until activities.length) {
	    if(edges.contains((list,activities(a)))) {
     	      arrFreq.update(a, edges.count(_ == (list,activities(a))))
              }
	  }
        (riga,arrFreq)
    }).sortByKey(true).collect()

    for(a<-0 until arrFreq.length){
      matrixFreq(a)=arrFreq(a)._2
    }

    }

    for(a<-0 until arrT.length){
      matrix(a)=arrT(a)._2
    }
    
    // Debug Only
    printColor("purple","   Simple DFG\n")
    printDFG(matrix, activities)
    if(imf) {
      printColor("purple","   DFG with frequencies\n")
      printDFG(matrixFreq, activities)
    }

    (matrix, activities, matrixFreq)
  }


  /**
  * createDFG
  * 
  * Create the Direct Follow Graph
  * using the graphx library.
  */
  def createDFG(log: List[List[String]], sc: SparkContext, imf: Boolean) : Graph[String, String] = {

    var vertex = new ArrayBuffer[(Long,String)]()
    val rddVertex = sc.parallelize(log)
    val activities = rddVertex.distinct.collect.toList.flatten.distinct.sorted.zipWithIndex.map{ case (el, index) => 
      vertex += ((index.toLong, el))
    }

    val vertexList = vertex.toList
    var edges = new ArrayBuffer[Edge[String]]()
    val rddEdge = sc.parallelize(log)
    val createEdges = rddEdge.map{list =>
      for(el <- list) {
      
      }
      edges += Edge(1L, 2L, "1")
    }.collect

    // Create an Array for edges
    //var edgeArray = Array(Edge(1L, 2L, "1"), Edge(2L, 3L, "1"), Edge(1L, 3L, "1"), Edge(2L, 7L, "1"), Edge(3L, 2L, "1"), Edge(4L, 5L, "1"), Edge(5L, 6L, "1"), Edge(6L, 4L, "1"), Edge(7L, 3L, "1"))

    // Create an RDD for vertex
    val vertexName: RDD[(VertexId, (String))] = sc.parallelize(vertex)
    // Create an RDD for edges
    val edgeName: RDD[Edge[String]] = sc.parallelize(edges)

    Graph(vertexName, edgeName)

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
      case "->" => "- seqCut: "
      case "||" => "- concurrentCut: "
      case "*"  => "- loopCut: "
    }

    printColor("green", typeOfCut + cut + "\n")

  }

}
