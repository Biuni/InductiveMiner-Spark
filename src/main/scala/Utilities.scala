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

  /**
  * countCC
  *
  * Count the connected 
  * components of the graph.
  */
  def countCC(graph: Graph[String,String]) : Long = {

    val components = graph.connectedComponents().vertices.cache()
    val countCC = components.map{ case(_,cc) => cc }.distinct.count()
    
    countCC
  }

  /**
  * getElemCC
  *
  * Get the elements of
  * the connected components.
  */
  def getElemCC(graph: Graph[String,String]) : List[List[Long]] = {

    var listOfCC = ArrayBuffer[List[Long]]()
    val components = graph.connectedComponents().vertices.cache()
    val getCC = components.map{ case(_,cc) => cc }.distinct.collect()

    for(vertex <- getCC) {
      var listOfElem = components.filter {
        case (id, component) => component == vertex
      }.map(_._1).collect.toList

      listOfCC += listOfElem.toList
    }

    listOfCC.toList
  }

  /**
  * createDFG
  * 
  * Create the Direct Follow Graph
  * using the graphx library.
  */
  def createDFG(log: List[List[String]], imf: Boolean, sc: SparkContext) : (Graph[String, String], List[(String,Long)]) = {

    val vertices = log.flatMap(x => x).toSet.toSeq
    val vertexMap = (0 until vertices.size)
        .map(i => vertices(i) -> i.toLong)
        .toMap

    val edgeSet = log
        .filter(_.size > 1)
        .flatMap(list => list.indices.tail.map(i => list(i-1) -> list(i)))
        .map(x => Edge(vertexMap(x._1), vertexMap(x._2), "1"))
        .toSet

    val edges = sc.parallelize(edgeSet.toSeq)
    val vertexNames = sc.parallelize(vertexMap.toSeq.map(_.swap))
    
    (Graph(vertexNames, edges), vertexMap.toList)

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




/* ------------------------------- */
  def cc(graph: Graph[String,String]) : (Long, Array[Long]) = {
	
	val components: VertexRDD[VertexId] = graph.connectedComponents().vertices.cache()

	// Conta il numero di componenti connesse
	val countCC = components.map{ case(_,cc) => cc }.distinct.count()
	val getCC = components.map{ case(_,cc) => cc }.distinct.collect()

	/*var CC = new ListBuffer[List[Long]]()
	//var newGraphs = new ListBuffer[Graph[String,String]]()
	// Print the vertices in that component
	for(vertex <- getCC) {
		var test = components.filter {
		  case (id, component) => component == vertex
		}.map(_._1).collect.toList
		CC += test
		var newGraph1 = graph.subgraph(vpred = (id,att) => test.contains(id))
		newGraphs += newGraph1
		//println("CC: " +test.toList)
	}*/

	//(countCC, newGraphs.toList.distinct)

	(countCC, getCC)
}
/* ------------------------------- */



  /**
  * checkActivities
  *
  * Cerca tutte le attività presenti nel log
  *
  */
  def checkActivities(log: List[List[String]], sc: SparkContext) : List[String] = {
    val rddLog = sc.parallelize(log)
    val activities = rddLog.distinct().collect().toList.flatten.distinct.sorted
    activities
  }

  /**
  * OLDcreateDFG
  *
  * Crea la matrice di adiacenza nodi-nodi del DFG tenendo conto, eventualmente, delle frequenze
  *
  */
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

    /**
    * Crea la matrice di adiacenza nodi-nodi contando la frequenza con la quale un arco compare
    */
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
  * printDFG
  *
  * Stampa la matrice di adiacenza nodi-nodi del DFG
  *
  */
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

}
