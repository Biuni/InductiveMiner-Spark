package IM

import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

import SplitLog._
import Utilities._

object FindCut {

  /**
  * FindCut
  * 
  * The IM searches for several cuts using the cut footprints.
  * It attempts to find cuts in the order: XOR, SEQUENCE, CONCURRENT, LOOP.
  */
  def checkFindCut(graph: Graph[String, String]) : (Boolean, ListBuffer[List[String]], Long, Array[Long], List[Long]) = {

    // CODE: https://s22.postimg.cc/esj1sl17l/Find_Cut.jpg

    var cutFound : Boolean = false
    var result : ListBuffer[List[String]] = null
    var countCC : Long = -1
    var getCC : Array[Long] = null
    var lst : List[Long] = null

    // Check if exist a xorCut
    val xor = xorCut(graph)
    if(xor._1) {
      // xorCut FOUNDED
      cutFound = true
      result = xor._2
      countCC = xor._3
      getCC = xor._4
    } else {

      // Check if exist a sequenceCut
      val seq = seqCut(graph)
      if(seq._1) {
        // sequenceCut FOUNDED
	cutFound = true
        result = seq._2
        countCC = seq._3
        getCC = seq._4
	lst = seq._5
      } else {

       // Check if exist a concurrentCut
       val concurrent = concurrentCut(graph)
        if(concurrent) {
          // concurrentCut FOUNDED
        } else {

          // Check if exist a loopCut
          val loop = loopCut(graph)
          if(loop) {
            // loopCut FOUNDED
          } else {
	    // NO Cut FOUND
            cutFound = false
          }
        }
      }
    }

    (cutFound, result, countCC, getCC, lst)
  }
  
  ///////////////////////////////////////////////////////////////////////////
  // Operations
  ///////////////////////////////////////////////////////////////////////////

  /**
  * Xor Cut
  * 
  * Check if exist a xorCut.
  */
  def xorCut(graph: Graph[String, String]) : (Boolean, ListBuffer[List[String]], Long, Array[Long]) = {

    var result = ListBuffer[List[String]]()
    var isXor : Boolean = false

    // Count the Connected Components of the DFG.
    var ccRes = countCC(graph)

    // If the number of the CC is bigger than 1 is a xorCut.
    if(ccRes._1 > 1) {
      result += List("X")
      isXor = true
    }

    (isXor, result, ccRes._1, ccRes._2)
  }

  /**
  * Seq Cut
  * 
  * Check if exist a seqCut.
  */
  def seqCut(graph: Graph[String, String]) : (Boolean, ListBuffer[List[String]], Long, Array[Long], List[Long]) = {

    var result = new ListBuffer[List[String]]()
    var isSeq : Boolean = false
    var lst : List[Long] = null

    // Count the Connected Components of the DFG.
    var ccRes = countCC(graph)

    // If there's only 1 Connected Component in the DFG
    if(ccRes._1 == 1) {
      // Collect all incoming vertices
      var in = graph.collectNeighborIds(EdgeDirection.In).map{_._2.toList}.collect().toList
      // Collect all outgoing vertices
      var out = graph.collectNeighborIds(EdgeDirection.Out).map{_._2.toList}.collect().toList

      if(in.indexOf(List()) != -1) {
       	// List of vertices with incoming edges
	lst = graph.inDegrees.map(x => x._1).collect.toList
      } else {
          if(out.indexOf(List()) != -1) {
            // List of vertices with outgoing edges
	    lst = graph.outDegrees.map(x => x._1).collect.toList
	    if(!in.contains(lst)) {
	      lst = lst.filter(_ < 0)
	    }
	  }
	}
        
	if( ((in.contains(List())) || (out.contains(List()))) && (!lst.isEmpty) ) {
	  result += List("-->")
          isSeq = true
	}
    }

    (isSeq, result, ccRes._1, ccRes._2, lst)
  }

  /**
  * Concurrent Cut
  * TODO...
  */
  def concurrentCut(graph: Graph[String, String]) : Boolean = {
	
    false
  }

  /**
  * Loop Cut
  * TODO...
  */
  def loopCut(graph: Graph[String, String]) : Boolean = {

    false
  }
  
}
