package IM

import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkConf, SparkContext}

import Utilities._
import BaseCase._
import FindCut._
import SplitLog._
import FallThrough._

object FilterLog {

  def filterLog(log: List[List[String]], sc: SparkContext, DFG: (Array[Array[Int]], List[String], Array[Array[Int]]), threshold: Float): Unit = {
    for(i <- 0 to DFG._3.length - 1) {
      for(elem <- DFG._3(i)) {
	var riga = DFG._3(i)
	var max = riga.filterNot(el => el == elem).max
        if(elem < threshold * max) {
	  var j = riga.indexOf(elem)
	  DFG._1(i)(j) = 0
	}
      }
    }
    printColor("purple","   Simple DFG after filtering\n")
    printDFG(DFG._1, checkActivities(log,sc))
  }
}
