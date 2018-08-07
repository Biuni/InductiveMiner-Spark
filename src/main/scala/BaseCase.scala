package IM

import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

object BaseCase {

  /**
  * BaseCase
  * 
  * The base cases for the IM algorithm provide an end to the recursion.
  * IM implements the BaseCase function of the IM framework using several steps:
  * it tries several base cases and returns the first matching one.
  * As the base cases are mutually exclusive, so their order is irrelevant.
  */
  def checkBaseCase(graph: Graph[String, String]) : List[String] = {

    // CODE: https://s22.postimg.cc/cucdu8t6p/Base_Case.jpg

    var result = ListBuffer[String]()

    if(graph.vertices.isEmpty) {
      result += "tau"
    } else if (graph.vertices.distinct.count() == 1) {
      var act = graph.vertices.map{ case(_,cc) => cc }.collect().distinct.toList
      result += act.map{ _.toString}.toString
    } else {
      result
    }

    result.toList
  }
}
