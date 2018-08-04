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

    val DFG = createDFG(log, imf, sc)

/*println("Num di CC: " + countCC(DFG._1))
println("Elementi CC: " + getElemCC(DFG._1))
println("ID - Vertici: " + DFG._2)*/

    // Controlla se il log è un BaseCase
    var bc = checkBaseCase(DFG._1)
    // Se esiste un basecase (e quindi la lista non è vuota)
    if(!bc.isEmpty) {
      // Inserisco il BaseCase nella lista dell'albero
      printColor("green", "- baseCase: "+ bc +"\n")
      //println("- baseCase: "+ bc +"\n")
    } else {
      // Se non è un BaseCase si controlla l'esistenza di un cut e si splitta il log in base al cut trovato
      // Example : List(List(->), List(a), List(b, c, d, e, f, h))
      var cut = checkFindCut(DFG._1)

      // Se esiste un cut (e quindi la lista non è vuota)
      if(cut._1) {
        // Stampo il cut
        //printCut(cut._2)
	// Faccio lo split in base al cut
        // Example: List(List(List(a)), List(List(b,c),List(c,b,h,c),List(d,e),List(d,e,f,d,e))
        var newLogs = checkSplitLog(DFG._1, cut._2, cut._3, cut._4)
	// Stampa il cut
	printCut(cut._2.toList)	
	//println(cut._2)
        // Avvia la ricorsione con i log splittati
        // (le due ricorsioni vanno eseguite in parallelo)
        IMFramework(newLogs._1(0), sc, imf, threshold)
        IMFramework(newLogs._1(1), sc, imf, threshold)
      } else {
        // Se non esiste nessun cut si esegue il FallThrough
        // Next...
        // ##### FallThrough(log)
      }
    }

  }
}
