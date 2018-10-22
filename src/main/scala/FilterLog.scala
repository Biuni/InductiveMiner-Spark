package IM

import scala.collection.mutable.ListBuffer
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import scala.collection.mutable.ArrayBuffer

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
    var edges = graph.edges
    var edgeCount = edges.count()
    var edgeDel: Int = -1
    var srcDel = new ListBuffer[Long]()
    var dstDel = new ListBuffer[Long]()
    var toDeleteSrc = new ListBuffer[(List[Long])]()
    var toDeleteDst = new ListBuffer[(List[Long])]()
    var e : Edge[String] = null
    var g2 = graph
    var j : Int = -1
    var g : Int = -1

    /** Create array of source vertices and destination vertices to delete **/
    for(k <- 0 until edgeCount.toInt) {
      g = g + 1
      j = j + 1
      // Add to 'src' and 'dst' source IDs and destination IDs of all edges
      e = g2.edges.first()
      src += e.srcId
      dst += e.dstId

      // Foreach edge add its weight
      weights += e.attr

      // If it is the first edge then analyze the second
      if (src.length > 1) {
        /* If edge-i hasn't the same source vertex than previous edge then
         * evaluate frequencies of all edges that have the same source vertex
         */
        if (src(g) != src(g - 1)) {
          // Delete weight of current edge that has different vertex source
          weights -= e.attr
          // Check if exists an edge that is under-threshold and find its position
          isUnd = checkFreq(weights, threshold)
          isUnder = isUnd._1
          pos = isUnd._2
          // If it exists evaluate position of edge to delete from DFG and record vertices to delete
          if (isUnd._1) {
            edgeDel = g - pos - 1
            srcDel += src(edgeDel)
            dstDel += dst(edgeDel)
            pos = 0
          }
          /* If no edge under-threshold is found then clear lists of edge weights that start from the same vertex
          * and add weight of current edge that have source vertex different than the previous one
	  */
          weights.clear()
          weights += e.attr

            while(src.length > 1) {
	      src.remove(0)
	      dst.remove(0)
	    }

   	  j = 0
	  g = 0
        }
      }

      // If it is the final edge than evaluate frequencies of any edges with same vertex
      if (k == edgeCount - 1) {
        isUnd = checkFreq(weights, threshold)
        // If edge under-threshold is found than evaluate position of edge to delete
        if (isUnd._1) {
          edgeDel = g - pos - 1
          srcDel += src(edgeDel)
          dstDel += dst(edgeDel)
        }
      }

      if(!srcDel.isEmpty) {
	toDeleteSrc += srcDel.toList
	toDeleteDst += dstDel.toList
		
	srcDel.clear()
	dstDel.clear()
      }

      var sG = src(j)
      var dG = dst(j)
      g2 = g2.subgraph(epred = e => ((e.srcId != sG) || (e.dstId != dG)))
  }

  // Create lists of source edges and destination edges to delete
  var sourceDel = new ListBuffer[Long]()
  var destDel = new ListBuffer[Long]()

  // Example: toDelete = List( (List(),List()), List(List(1),List(4)), List(List(1,3),List(4,1)) )
  if (toDeleteSrc.length >= 1) {
    var lS = toDeleteSrc.flatten
    var lD = toDeleteDst.flatten
    for (i <- 0 until lS.length) {
      // Example: sourceDel = List(1,3)
      sourceDel += lS(i)
    }
    for (i <- 0 until lD.length) {
      // Example: destDel = List(4,1)
      destDel += lD(i)
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

    var max: Float = -1
    var weights2 = new ListBuffer[String]()
    var isUnd: Boolean = false
    var w1Length: Int = weights.length - 1
    var i = 0
    var pos: Int = -1
    // Evaluate foreach weight if it is less than: threshold * max, where 'max' is the maximum weight of other edges with same starting vertex
    for (weight <- weights) {
      w1Length = w1Length - 1
      weights2 = weights
      weights2.drop(weight.toInt)
      for (w_weight <- weights2) {
        if (w_weight.toFloat > max) {
          max = w_weight.toFloat
        }
      }

      // If weight is under-threshold than record position in which it is present in list of weights that is being analyzed
      if (weight.toFloat < threshold * max) {
        isUnd = true
        if (i == 0) { pos = weights.length - 1 }
        else pos = w1Length
      }

      i = i + 1

    }

    (isUnd, pos)

  }

}
