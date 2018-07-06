package IM

import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkConf, SparkContext}

import Utilities._
import BaseCase._
import FindCut._
import SplitLog._
import FallThrough._

object IMFramework {

  def IMFramework(log: List[List[String]], sc: SparkContext) : Unit = { 
    // Viene creato il Directly Follows Graph del log
    createDFG(log, sc)

    // Controlla se il log è un BaseCase
    var bc = checkBaseCase(log, sc)
    // Se esiste un basecase (e quindi la lista non è vuota)
    if(!bc.isEmpty) {
      // Inserisco il BaseCase nella lista dell'albero
      bc
    } else {
      // Se non è un BaseCase si controlla l'esistenza di un cut
      // example : List(List("X"), List("a","b"))
      var cut = FindCut(log)
      // Se esiste un cut (e quindi la lista non è vuota)
      if(!cut.isEmpty) {
        // Faccio lo split in base al cut
        // example: List(List(List("a")), List(List("b","d","f"), List("c","e","f"), List("c","d","f"))
        var newLogs = SplitLog(log, cut)
        // Avvia la ricorsione con i log splittati
        // (le due ricorsioni vanno eseguite in parallelo)
        IMFramework(newLogs(0))
        IMFramework(newLogs(1))
      } else {
        // Se non esiste nessun cut si esegue il FallThrough
        // Next...
        FallThrough(log)
      }
    }

  }

}
