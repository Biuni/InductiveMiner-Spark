package IM

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer

object Utilities {

  def checkActivities(log: List[List[String]], sc: SparkContext) : List[String] = {
    val rddLog = sc.parallelize(log)
    val activities = rddLog.distinct().collect().toList.flatten.distinct.sorted
    activities
  }

  def createDFG(log: List[List[String]], sc: SparkContext, imf: Boolean) : (Array[Array[Int]], List[String], Array[Array[Int]]) = {
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
    println("Simple DFG\n")
    printDFG(matrix, activities)
    if(imf) {
      println("DFG with frequencies\n")
      printDFG(matrixFreq, activities)
    }

    (matrix, activities, matrixFreq)
  }


  /**
  * printDFG
  * 
  * Print the Direct Follow Graph into console
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
