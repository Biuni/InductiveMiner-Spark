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

    // IM Framework
    def IMFramework(graph: Graph[String, String]) : Unit = { 

    // Controlla se il log è un BaseCase
    var bc = checkBaseCase(graph)
    // Se esiste un basecase (e quindi la lista non è vuota)
    if(!bc.isEmpty) {
      // Inserisco il BaseCase nella lista dell'albero
      printColor("green", "- baseCase: "+ bc +"\n")
      //println("- baseCase: "+ bc +"\n")
    } else {
      // Se non è un BaseCase si controlla l'esistenza di un cut e si splitta il log in base al cut trovato
      // Example : List(List(->), List(a), List(b, c, d, e, f, h))
      var cut = checkFindCut(graph)

      // Se esiste un cut (e quindi la lista non è vuota)
      if(cut._1) {
        // Stampo il cut
        //printCut(cut._2)
	// Faccio lo split in base al cut
        // Example: List(List(List(a)), List(List(b,c),List(c,b,h,c),List(d,e),List(d,e,f,d,e))
        var newLogs = checkSplitLog(graph, cut._2, cut._3, cut._4)
	// Stampa il cut
	printCut(cut._2.toList)	
	//println(cut._2)
        // Avvia la ricorsione con i log splittati
        // (le due ricorsioni vanno eseguite in parallelo)
        IMFramework(newLogs._1(0))
        IMFramework(newLogs._1(1))
      } else {
        // Se non esiste nessun cut si esegue il FallThrough
        // Next...
        // ##### FallThrough(log)
      }
    }
  }

  // IM Framework: Handling Deviating & Infrequent Behaviour
  def IMFrameworkInf(graph: Graph[String, String], threshold: Float) : Unit = { 

    // Controlla se il log è un BaseCase
    var bc = checkBaseCase(graph)
    // Se esiste un basecase (e quindi la lista non è vuota)
    if(!bc.isEmpty) {
      // Inserisco il BaseCase nella lista dell'albero
      printColor("green", "- baseCase: "+ bc +"\n")
      //println("- baseCase: "+ bc +"\n")
    } else {
      // Se non è un BaseCase si controlla l'esistenza di un cut e si splitta il log in base al cut trovato
      // Example : List(List(->), List(a), List(b, c, d, e, f, h))
      var cut = checkFindCut(graph)

      // Se esiste un cut (e quindi la lista non è vuota)
      if(cut._1) {
        // Stampo il cut
        //printCut(cut._2)
	// Faccio lo split in base al cut
        // Example: List(List(List(a)), List(List(b,c),List(c,b,h,c),List(d,e),List(d,e,f,d,e))
        var newLogs = checkSplitLog(graph, cut._2, cut._3, cut._4)
	// Stampa il cut
	printCut(cut._2.toList)	
	//println(cut._2)
        // Avvia la ricorsione con i log splittati
        // (le due ricorsioni vanno eseguite in parallelo)
        IMFramework(newLogs._1(0))
        IMFramework(newLogs._1(1))
      } else {
	  // Se non esiste un cut si filtra il log in base al threshold in input
   	  var graphFiltered = filterLog(graph,threshold)
	  println("*** GRAPH FILTERED ***\n")
	  println("* EDGES *\n")
  	  graphFiltered.edges.foreach(println)
	  println("\n* VERTICES *\n")
	  graphFiltered.vertices.foreach(println)
	  println("\n")
	  // Se il grafo filtrato è diverso dal grafo iniziale allora si esegue IMf col grafo filtrato
	  if (graphFiltered != graph) {
	    IMFrameworkInf(graphFiltered,threshold)
	  } else {
	      // Se non esiste nessun cut e il grafo filtrato è uguale al grafo iniziale si esegue il FallThrough
              // Next...
              // ##### FallThrough(log)
      }
	}
     }

  }

}
