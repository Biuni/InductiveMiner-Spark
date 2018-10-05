package IM

import scala.collection.mutable.ListBuffer
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.graphx._

object SplitLog {

  /**
   * SplitLog
   *
   * After finding a cut, the IM framework splits the log into several sub-logs,
   * on which recursion continues.
   */
  def checkSplitLog(graph: Graph[String, String], result: ListBuffer[List[String]], countCC: Long, getCC: Array[Long], lst: List[Long]): (List[Graph[String, String]], List[List[String]]) = {

    // CODE: https://s22.postimg.cc/c684p44g1/splitlof.jpg

    var splitResult: (List[Graph[String, String]], List[List[String]]) = (null, null)
    splitResult = result(0)(0) match {
      case "X"   => xorSplit(graph, result, countCC, getCC)
      case "-->" => sequenceSplit(graph, result, lst)
      case "||"  => concurrentSplit(graph, result) // TODO
      case "*"   => loopSplit(graph, result) // TODO
    }

    (splitResult._1, splitResult._2)
  }

  ///////////////////////////////////////////////////////////////////////////
  // Operations
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Xor Split
   *
   * Split the DFG using the Xor Cut found.
   */
  def xorSplit(graph: Graph[String, String], result: ListBuffer[List[String]], countCC: Long, getCC: Array[Long]): (List[Graph[String, String]], List[List[String]]) = {

    val components = graph.connectedComponents().vertices.cache()
    var newLogs = new ListBuffer[Graph[String, String]]()

    // Split DFG into sub-DFGs using connected components
    for (vertex <- getCC) {
      var vertices = components.filter {
        case (id, component) => component == vertex
      }.map(_._1).collect.toList
      var newLog = graph.subgraph(vpred = (id, att) => vertices.contains(id))
      newLogs += newLog
    }

    // Add DFGs vertices to 'result'
    for (g <- newLogs) {
      var v = g.vertices.map(_._2).collect().toList
      result += v
    }

    (newLogs.toList.distinct, result.toList)
  }

  /**
   * Sequence Split
   *
   * Split the DFG using the Seq Cut found.
   */
  def sequenceSplit(graph: Graph[String, String], result: ListBuffer[List[String]], lst: List[Long]): (List[Graph[String, String]], List[List[String]]) = {

    var newLogs = new ListBuffer[Graph[String, String]]()

    // Add to 'newLogs' the graph with vertices that don't have incoming edges
    newLogs += graph.subgraph(vpred = (id, att) => !lst.contains(id))
    // Add to 'newLogs' the graph with vertices that have only incoming edges
    newLogs += graph.subgraph(vpred = (id, att) => lst.contains(id))

    // Add to result the lists of cut's activities
    for (g <- newLogs) {
      var v = g.vertices.map(_._2).collect().toList
      result += v
    }

    (newLogs.toList.distinct, result.toList)
  }

  /**
   * Concurrent Split
   * TODO...
   */
  def concurrentSplit(graph: Graph[String, String], result: ListBuffer[List[String]]): (List[Graph[String, String]], List[List[String]]) = {

    (List(graph), List(List()))
  }

  /**
   * Loop Split
   * TODO...
   */
  def loopSplit(graph: Graph[String, String], result: ListBuffer[List[String]]): (List[Graph[String, String]], List[List[String]]) = {

    (List(graph), List(List()))
  }

}
