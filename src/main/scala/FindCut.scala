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
  def checkFindCut(graph: Graph[String, String]) : (Boolean, ListBuffer[List[String]], Long, Array[Long]) = {

    // CODE: https://s22.postimg.cc/esj1sl17l/Find_Cut.jpg

    var cutFound : Boolean = false
    var result : ListBuffer[List[String]] = null
    var countCC : Long = -1
    var getCC : Array[Long] = null

    val xor = xorCut(graph)
    if(xor._1) {
      // xorCut found
      cutFound = true
      result = xor._2
      countCC = xor._3
      getCC = xor._4
    } else {

      val seq = seqCut(graph)
      if(seq._1) {
        // sequenceCut found
	cutFound = true
        result = seq._2
        countCC = seq._3
        getCC = seq._4
      } else {

       val concurrent = concurrentCut(graph)
        if(concurrent) {
          // concurrentCut found
        } else {

          val loop = loopCut(graph)
          if(loop) {
            // loopCut found
          } else {
	    // NO Cut found
            cutFound = false
          }
        }
      }
    }

    (cutFound, result, countCC, getCC)
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
  def seqCut(graph: Graph[String, String]) : (Boolean, ListBuffer[List[String]], Long, Array[Long]) = {

    var result = new ListBuffer[List[String]]()
    var isSeq : Boolean = false

    // Count the Connected Components of the DFG.
    var ccRes = countCC(graph)

    // If there's only 1 Connected Component in the DFG
    if(ccRes._1 == 1) {
      var in = graph.collectNeighborIds(EdgeDirection.In).map{_._2.toList}.collect().toList
      // If exist a vertex with all incoming or outgoing edges is a seqCut.
      if (in.contains(List())) {
        result += List("-->")
        isSeq = true
      }
    }

    (isSeq, result, ccRes._1, ccRes._2)
  }

  /**
  * Concurrent Cut
  * ToDo...
  */
  def concurrentCut(graph: Graph[String, String]) : Boolean = {
	
    false
  }

  /**
  * Loop Cut
  * ToDo...
  */
  def loopCut(graph: Graph[String, String]) : Boolean = {

    false
  }
  
}
