package IM

import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

object SplitLog {

  /**
  * SplitLog
  * 
  * After finding a cut, the IM framework splits the log into several sub-logs,
  * on which recursion continues.
  */
  def checkSplitLog(log: Graph[String,String], result: ListBuffer[List[String]], countCC: Long, getCC: Array[Long]) : (List[Graph[String,String]], List[List[String]]) = {

    // CODE: https://s22.postimg.cc/c684p44g1/splitlof.jpg

    var splitResult : (List[Graph[String,String]], List[List[String]]) = (null, null)
    splitResult = result(0)(0) match {
      case "X" => xorSplit(log, result, countCC, getCC)
      case "-->" => sequenceSplit(log, result)
      //case "||" => concurrentSplit(log, result, countCC, getCC)
      //case "*" => loopSplit(log, result, countCC, getCC)
    }

    (splitResult._1, splitResult._2)
  }

  ///////////////////////////////////////////////////////////////////////////
  // Operations
  ///////////////////////////////////////////////////////////////////////////

  /**
  * Xor Split
  * 
  * Return a List of List of List of String if the Cut founded is a XOR.
  * Otherwhise return an empty list.
  * Example: List(List(List(b,c),List(c,b,h,c)), List(List(d,e),List(d,e,f,d,e))
  * @return List[List[List[String]]]
  */
  def xorSplit(graph: Graph[String,String], result: ListBuffer[List[String]], countCC: Long, getCC: Array[Long]) : (List[Graph[String,String]], List[List[String]]) = {
    
    val components: VertexRDD[VertexId] = graph.connectedComponents().vertices.cache()
    var CC = new ListBuffer[List[Long]]()
    var newLogs = new ListBuffer[Graph[String,String]]()

    for(vertex <- getCC) {
      var test = components.filter {
	case (id, component) => component == vertex
	}.map(_._1).collect.toList
     CC += test
     var newLog = graph.subgraph(vpred = (id,att) => test.contains(id))
     newLogs += newLog
    }

    for(g <- newLogs) {
      var v = g.vertices.map(_._2).collect().toList
      result += v
    }

    (newLogs.toList.distinct, result.toList)
  }

  /**
  * Sequence Split
  * 
  * Return a List of List of List of String if the Cut founded is a SEQUENCE.
  * Otherwhise return an empty list.
  * Example: List(List(List(a)), List(List(b,c),List(c,b,h,c),List(d,e),List(d,e,f,d,e))
  * @return List[List[List[String]]]
  */

  def sequenceSplit(graph: Graph[String,String], result: ListBuffer[List[String]]) : (List[Graph[String,String]], List[List[String]]) = {

	//var result = new ListBuffer[List[String]]()
	var newLogs = new ListBuffer[Graph[String,String]]()
	var in = graph.collectNeighborIds(EdgeDirection.In).map{_._2.toList}.collect().toList
	var i = in.indexOf(List())
	// lista di vertici con archi entranti
	val lst = graph.inDegrees.map(x => x._1).collect.toList
	// Crea grafo con i vertici che non hanno archi entranti e relativi archi
	newLogs += graph.subgraph(vpred = (id,att) => !lst.contains(id))
	// Crea grago con i vertici che hanno archi entranti e relativi archi
	newLogs += graph.subgraph(vpred = (id,att) => lst.contains(id))

	// Aggiunge al risultato le liste delle attività del cut
	for(g <- newLogs) {
	  var v = g.vertices.map(_._2).collect().toList
	  result += v
	}

	(newLogs.toList.distinct, result.toList)
  }

  /**
  * Concurrent Split
  * ToDo...
  */
  def concurrentSplit(graph: Graph[String,String], result: ListBuffer[List[String]], countCC: Long, getCC: Array[Long]) : Unit = {

  }

  /**
  * Loop Split
  * ToDo...
  */
  def loopSplit(graph: Graph[String,String], result: ListBuffer[List[String]], countCC: Long, getCC: Array[Long]) : Unit = {

  }
    
}
