package IM

import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkConf, SparkContext}

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

    // Controlla se il log è un BaseCase
    var bc = checkBaseCase(log, sc)
    // Se esiste un basecase (e quindi la lista non è vuota)
    if(!bc.isEmpty) {
      // Inserisco il BaseCase nella lista dell'albero
      printColor("green", "- baseCase: "+ bc +"\n")
    } else {
      // Se non è un BaseCase si controlla l'esistenza di un cut
      // Example : List(List(->), List(a), List(b, c, d, e, f, h))
      var cut = checkFindCut(log, sc, DFG)

      if(!cut._1 && imf) {
	printColor("red", "CUT not detected --> Filter log\n")
	filterLog(log,sc,DFG,threshold)
	cut = checkFindCut(log,sc,DFG)
      }

      // Se esiste un cut (e quindi la lista non è vuota)
      if(cut._1) {
        // Stampo il cut
        printCut(cut._2)
        // Faccio lo split in base al cut
        // Example: List(List(List(a)), List(List(b,c),List(c,b,h,c),List(d,e),List(d,e,f,d,e))
        var newLogs = checkSplitLog(log, cut._2, sc)
        // Avvia la ricorsione con i log splittati
        // (le due ricorsioni vanno eseguite in parallelo)
        IMFramework(newLogs(0), sc, imf, threshold)
        IMFramework(newLogs(1), sc, imf, threshold)
      } else {
        // Se non esiste nessun cut si esegue il FallThrough
        // Next...
        // ##### FallThrough(log)
      }
    }

  }
}
