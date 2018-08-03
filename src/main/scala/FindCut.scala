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
    var newLogs: List[Graph[String,String]] = null
    var countCC : Long = -1
    var getCC : Array[Long] = null

    val xor = xorCut(graph)
    if(xor._1) {
      // xorCut found
      cutFound = true
      result = xor._2
      //newLogs = xor._3
      countCC = xor._3
      getCC = xor._4
    } else {

      val seq = seqCut(graph)
      if(seq._1) {
	cutFound = true
        // sequenceCut found
        result = seq._2
        //newLogs = seq._3
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

    //(cutFound, result, newLogs)
    (cutFound, result, countCC, getCC)
  }
  
  ///////////////////////////////////////////////////////////////////////////
  // Operations
  ///////////////////////////////////////////////////////////////////////////

  /**
  * Xor Cut
  * 
  * Return a List of List of String if the Xor Cut is found in the log.
  * Otherwhise return an empty list.
  * Example : List(List("X"), List("b","c","h"), List("d","e","f"))
  * @return (Boolean, List[List[String]])
  */

  def xorCut(graph: Graph[String, String]) : (Boolean, ListBuffer[List[String]], Long, Array[Long]) = {

	var result = ListBuffer[List[String]]()
	var isXor : Boolean = false
	var newLogs: List[Graph[String,String]] = null

	// calcola il numero di componenti connesse
	var ccRes = cc(graph)

	// Se il numero di componenti connesse è maggiore di 1 allora è uno xorCut
	if(ccRes._1 > 1) {
          result += List("X")
	  isXor = true
	  // xorSplit
	  //newLogs = xorSplit(graph, ccRes._1, ccRes._2)
	  // Aggiunge al risultato le liste delle attività del cut
	  /*for(g <- newLogs) {
	    var v = g.vertices.map(_._2).collect().toList
	    result += v
	  }*/
	}

	//(isXor, result.toList, newLogs)
	(isXor, result, ccRes._1, ccRes._2)
  }

  def seqCut(graph: Graph[String, String]) : (Boolean, ListBuffer[List[String]], Long, Array[Long]) = {

	var result = new ListBuffer[List[String]]()
	var isSeq : Boolean = false
	var newLogs : List[Graph[String,String]] = null

	// calcola il numero di componenti connesse
	var ccRes = cc(graph)

	if(ccRes._1 == 1) {
	  var in = graph.collectNeighborIds(EdgeDirection.In).map{_._2.toList}.collect().toList
	  if (in.contains(List())) {
	    // Se il numero di componenti connesse è 1 ed esiste un vertice che non ha archi entranti allora è un sequenceCut
	    isSeq = true
	    result += List("-->")
	    // sequenceSplit
	    //newLogs = sequenceSplit(graph)

	    // Aggiunge al risultato le liste delle attività del cut
	    /*for(g <- newLogs) {
	      var v = g.vertices.map(_._2).collect().toList
	      result += v
	    }*/
	  }
	}

	//(isSeq, result.toList, newLogs)
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
