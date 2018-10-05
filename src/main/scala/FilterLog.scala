package IM

import scala.collection.mutable.ListBuffer
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.graphx._
import org.apache.spark.rdd._

object FilterLog {

  /**
   * Filter Log
   *
   * Filter the DFG using the threshold.
   */
  def filterLog(graph: Graph[String, String], threshold: Float): (Graph[String, String]) = {

    var src = new ListBuffer[Long]()
    var dst = new ListBuffer[Long]()
    var weights = new ListBuffer[String]()
    var i = 0
    var isUnd: (Boolean, Int) = null // to establish if there's an edge that it under-threshold and its position
    var isUnder: Boolean = false
    var pos: Int = -1
    var edgeCount = graph.edges.count()
    var edgeDel: Int = -1
    var srcDel = new ListBuffer[Long]()
    var dstDel = new ListBuffer[Long]()

    /** Create array of source vertices and destination vertices to delete **/
    var toDelete: Array[(List[Long], List[Long])] = graph.triplets.map(triplet => {
      // Add to 'src' and 'dst' source IDs and destination IDs of all edges
      src += triplet.srcId
      dst += triplet.dstId

      // Foreach edge add its weight
      weights += triplet.attr

      // If it is the first edge then analyze the second
      if (src.length > 1) {
        /* If edge-i hasn't the same source vertex than previous edge then
         * evaluate frequencies of all edges that have the same source vertex
         */
        if (src(i) != src(i - 1)) {
          // Delete weight of current edge that has different vertex source
          weights -= triplet.attr
          // Check if exists an edge that is under-threshold and find its position
          isUnd = checkFreq(weights, threshold)
          isUnder = isUnd._1
          pos = isUnd._2
          // If it exists evaluate position of edge to delete from DFG and record vertices to delete
          if (isUnd._1) {
            edgeDel = i - pos - 1
            srcDel += src(edgeDel)
            dstDel += dst(edgeDel)
            pos = 0
          }
          /* If no edge under-threshold is found then clear lists of edge weights that start from the same vertex
          * and add weight of current edge that have source vertex different than the previous one
	        */
          weights.clear()
          weights += triplet.attr
        }
      }

      // If it is the final edge than evaluate frequencies of any edges with same vertex
      if (i == edgeCount - 1) {
        isUnd = checkFreq(weights, threshold)
        // If edge under-threshold is found than evaluate position of edge to delete
        if (isUnd._1) {
          edgeDel = i - pos - 1
          srcDel += src(edgeDel)
          dstDel += dst(edgeDel)
        }
      }

      i = i + 1

      (srcDel.toList, dstDel.toList)

    }).collect()

    // Create lists of source edges and destination edges to delete
    var sourceDel = new ListBuffer[Long]()
    var destDel = new ListBuffer[Long]()

    // Example: toDelete = List( (List(),List()), List(List(1),List(4)), List(List(1,3),List(4,1)) )
    if (toDelete.length > 1) {
      var l = toDelete(toDelete.length - 1)
      for (i <- l._1) {
        // Example: sourceDel = List(1,3)
        sourceDel += i
      }
      for (i <- l._2) {
        // Example: destDel = List(4,1)
        destDel += i
      }
    }

    var graph2: Graph[String, String] = graph

    // Filtering DFG foreach element in 'sourceDel' and 'destDel'
    for (j <- 0 until sourceDel.length) {
      graph2 = filtering(graph2, sourceDel(j), destDel(j))
    }

    graph2
  }

  /** Create sub-DFG without edges under-threshold and without vertices with no incoming and outcoming edges **/
  def filtering(graph: Graph[String, String], sdel: Long, ddel: Long): Graph[String, String] = {

    //Edges filtering
    var graphFiltered = graph.subgraph(epred = (triplet) =>

      ((triplet.srcId != sdel) || (triplet.dstId != ddel)))

    // Filter vertices with no incoming and outcoming edges
    var lst = graphFiltered.outDegrees.map(x => x._1).collect()
    var lst2 = graphFiltered.inDegrees.map(x => x._1).collect()
    var union = lst.toList.union(lst2.toList)

    var graphFiltered2 = graphFiltered.subgraph(vpred = (id, att) =>

      union.contains(id))

    graphFiltered2

  }

  /** Evaluate if edges' frequencies that starting from the same node are under-threshold **/
  def checkFreq(weights: ListBuffer[String], threshold: Float): (Boolean, Int) = {

    // CODE: https://s15.postimg.cc/9no9nrs0r/filter_Log.jpg

    var max: Int = -1
    var weights2 = new ListBuffer[String]()
    var isUnd: Boolean = false
    var w1Length: Int = weights.length - 1
    var i = 0
    var pos: Int = -1

    // Evaluate foreach weight if it is less than: threshold * max, where 'max' is the maximum weight of other edges with same starting vertex
    for (weight <- weights) {
      w1Length = w1Length - 1
      weights2 = weights.drop(weight.toInt)
      for (w_weight <- weights2) {
        if (w_weight.toInt > max) {
          max = w_weight.toInt
        }
      }

      // If weight is under-threshold than record position in which it is present in list of weights that is being analyzed
      if (weight.toInt < threshold * max) {
        isUnd = true
        if (i == 0) { pos = weights.length - 1 }
        else pos = w1Length
      }

      i = i + 1

    }

    (isUnd, pos)

  }

}
