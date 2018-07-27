package IM

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd._

import Utilities._
import BaseCase._
import FindCut._
import SplitLog._
import FallThrough._
import FilterLog._

object IMFramework {

  def IMFramework(log: List[List[String]], sc: SparkContext, imf: Boolean, threshold: Float) : Unit = { 
    // Viene creato il Directly Follows Graph del log
    val DFG = createDFG(log, sc, imf)
	val components: VertexRDD[VertexId] = DFG.connectedComponents().vertices.cache()

	// Conta il numero di componenti connesse
	val countCC = components.map{ case(_,cc) => cc }.distinct.count()
	val getCC = components.map{ case(_,cc) => cc }.distinct.collect()

	// Print the vertices in that component
	for(vertex <- getCC) {
		var test = components.filter {
		  case (id, component) => component == vertex
		}.map(_._1).collect
		println(test.toList)
	}

	println(countCC)

  }
}
