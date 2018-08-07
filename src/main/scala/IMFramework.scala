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

  /**
  * IMFramework
  * 
  * The core of Inductive Miner.
  */
  def IMFramework(graph: Graph[String, String], imf: Boolean, threshold: Float) : Unit = { 

    // Check if the DFG is a BaseCase
    var bc = checkBaseCase(graph)
    // If the DFG is BaseCase
    if(!bc.isEmpty) {
      // Print the BaseCase
      printColor("green", "- baseCase: "+ bc +"\n")
    } else {
      // If isn't a BaseCase check if exist a Cut
      var cut = checkFindCut(graph)

      // If the Cut was found
      if(cut._1) {
	// Split the DFG using the Cut
        var newLogs = checkSplitLog(graph, cut._2, cut._3, cut._4)
        // Print the Cut
	printCut(cut._2.toList)
        // Start recursion using the splitted DFG
	for(i <- 0 until newLogs._1.length) {
          IMFramework(newLogs._1(i), imf, threshold)
	}
      } else {
        if(imf) {
          // If the IM type is Infrequent Behaviour
          // Filter the DFG using the threshold
          var graphFiltered = filterLog(graph, threshold)
          // Print the filtered DFG
          printDFG(graphFiltered, true)
          // If the filtered DFG was different by initial DFG execute IM with the filtered DFG
          if (graphFiltered != graph) {
            IMFramework(graphFiltered, imf, threshold)
          } else {
            // If the cut don't exist and the filtered DFG
            // is equal to initial DFG execute the FallThrogugh.
            // ##### FallThrough(log)
            printColor("red", "- FallThrough\n")
          }
        } else {
          // If the cut don't exist. Do the FallThrough.
          // ##### FallThrough(log)
          printColor("red", "- FallThrough\n")
        }

      } // cut

    } // baseCase

  } // def

}
