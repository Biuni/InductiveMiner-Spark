package IM

import scala.collection.mutable.ListBuffer
import org.apache.spark.{ SparkConf, SparkContext }
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
  def checkFindCut(graph: Graph[String, String]): (Boolean, ListBuffer[List[String]], Array[Long], List[Long], VertexRDD[Long]) = {

    // CODE: https://s22.postimg.cc/esj1sl17l/Find_Cut.jpg

    var cutFound: Boolean = false
    var result: ListBuffer[List[String]] = null
    var getCC: Array[Long] = null
    var lst: List[Long] = null

    // Count connected components of graph
    var components = graph.connectedComponents.vertices.cache()

    val countCC = components.map { case (_, cc) => cc }.distinct.count()

    // If number of connected components are greater than 1 then there is a XorCut
    if (countCC > 1) {
      // xorCut FOUNDED
      cutFound = true
      val xor = xorCut(graph, components)
      result = xor._1
      getCC = xor._2
    } else {

      // Check if exist a sequenceCut
      val seq = seqCut(graph)
      if (seq._1) {
        // sequenceCut FOUNDED
        cutFound = true
        result = seq._2
        lst = seq._3
      } else {

        // Check if exist a concurrentCut
        val concurrent = concurrentCut(graph)
        if (concurrent) {
          // concurrentCut FOUNDED
        } else {

          // Check if exist a loopCut
          val loop = loopCut(graph)
          if (loop) {
            // loopCut FOUNDED
          } else {
            // NO Cut FOUND
            cutFound = false
          }
        }
      }
    }

    (cutFound, result, getCC, lst, components)
  }

  ///////////////////////////////////////////////////////////////////////////
  // Operations
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Xor Cut
   *
   * Check if exist a xorCut.
   */
  def xorCut(graph: Graph[String, String], components: VertexRDD[Long]): (ListBuffer[List[String]], Array[Long]) = {

    var result = ListBuffer[List[String]]()

    // Get connected components
    var getCC = components.map { case (_, cc) => cc }.distinct.collect()
    
    result += List("X")

    (result, getCC)
  }

  /**
   * Seq Cut
   *
   * Check if exist a seqCut.
   */
  def seqCut(graph: Graph[String, String]): (Boolean, ListBuffer[List[String]], List[Long]) = {

    var result = new ListBuffer[List[String]]()
    var isSeq: Boolean = false
    var lst: List[Long] = null

    // Collect all incoming vertices
    var in = graph.collectNeighborIds(EdgeDirection.In).map { _._2.toList }.collect().toList
    // Collect all outgoing vertices
    var out = graph.collectNeighborIds(EdgeDirection.Out).map { _._2.toList }.collect().toList

    if (in.indexOf(List()) != -1) {
      // List of vertices with incoming edges
      lst = graph.inDegrees.map(x => x._1).collect.toList
    } else {
      if (out.indexOf(List()) != -1) {
        // List of vertices with outgoing edges
        lst = graph.outDegrees.map(x => x._1).collect.toList
        if (!in.contains(lst)) {
          lst = lst.filter(_ < 0)
        }
      }
    }

    if (((in.contains(List())) || (out.contains(List()))) && (!lst.isEmpty)) {
      result += List("-->")
      isSeq = true
    }

    (isSeq, result, lst)
  }

  /**
   * Concurrent Cut
   * TODO...
   */
  def concurrentCut(graph: Graph[String, String]): Boolean = {

    false
  }

  /**
   * Loop Cut
   * TODO...
   */
  def loopCut(graph: Graph[String, String]): Boolean = {

    false
  }

}
