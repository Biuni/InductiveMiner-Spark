package IM

import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkConf, SparkContext}

object FindCut {

  /**
  * FindCut
  * 
  * The IM searches for several cuts using the cut footprints.
  * It attempts to find cuts in the order: XOR, SEQUENCE, CONCURRENT, LOOP.
  */
  def checkFindCut(log: List[List[String]], sc: SparkContext, DFG: (Array[Array[Int]], List[String])) : (Boolean, List[List[String]]) = {

    // CODE: https://s22.postimg.cc/esj1sl17l/Find_Cut.jpg
    var cutFound : Boolean = true
    var result : List[List[String]] = null

    val xor = xorCut(log, sc, DFG)
    if(xor._1) {
      // xorCut found
      println("xorCut: " + xor._2)
      result = xor._2
    } else {

      val seq = sequenceCut(log, sc, DFG)
      if(seq._1) {
        // sequenceCut found
        println("seqCut: " + seq._2)
        result = seq._2
      } else {

        val concurrent = concurrentCut(log, sc, DFG)
        if(concurrent._1) {
          // concurrentCut found
        } else {

          val loop = loopCut(log, sc, DFG)
          if(loop._1) {
            // loopCut found
          } else {
	    // NO Cut found
            cutFound = false
          }
        }
      }
    }

    (cutFound, result)
  }
  
  ///////////////////////////////////////////////////////////////////////////
  // Operations
  ///////////////////////////////////////////////////////////////////////////

  /**
  * Xor Cut
  * 
  * Return a List of List of String if the Xor Cut is found in the log.
  * Otherwhise return an empty list.
  * Example : List(List("X"), List("b","c","h"), List("d","e","f"))
  * @return (Boolean, List[List[String]])
  */
  def xorCut(log: List[List[String]], sc: SparkContext, DFG: (Array[Array[Int]], List[String])) : (Boolean, List[List[String]]) = {

    // Controllo se riga o colonna del DFG sono tutti 0
    // perchè se così fosse allora si può skippare al seqCut
    var isXor : Boolean = false
    // Scorro la RIGA della prima attività
    for(elem <- DFG._1(0)) {
      if(elem == 1) {
	isXor = true
      }
    }
    // Se la riga non ha tutti 0 provo con la COLONNA
    if(isXor) {
      isXor = false
      for(elem <- DFG._1) {
        if(elem(0) == 1) {
	  isXor = true
        }
      }
    }

    var result = ListBuffer[List[String]]()
    // Se riga o colonna della prima attività 
    // NON sono uguali a 0 allora è uno xorCut.
    if(isXor) {
      result += List("X")
      // Per ogni attività si deve controllare
      // i collegamenti, quindi dove vale 1
      for((elem, index) <- DFG._2.zipWithIndex) {
        var links = ListBuffer[String]()
        links += elem
        // Scorro la RIGA
        for((value, index2) <- DFG._1(index).zipWithIndex) {
          if(value == 1) {
            // Se vale 1 allora è un collegamento
            // e lo inserisco nella lista dei collegati
            links += DFG._2(index2)
          }
        }
        // Scorro la COLONNA
        for((value2, index3) <- DFG._1.zipWithIndex) {
          if(value2(index) == 1) {
            // Se vale 1 allora è un collegamento
            // e lo inserisco nella lista dei collegati
            links += DFG._2(index3)
          }
        }
        // A questo punto faccio il distinct delle attività
        // in modo da non avere ripetizioni tra i collegamenti
        // e le inserisco nella lista dei risultati
        result += links.toList.distinct.sorted
      }
    }

    // La lista dei risultati ha di nuovo in distinct perchè
    // così facendo elimino le liste uguali, cioè quelle che
    // formano lo XOR lasciando solo le attività separate
    (isXor, result.distinct.toList)
  }

  /**
  * Sequence Cut
  * 
  * Return a List of List of String if the Sequence Cut is found in the log.
  * Otherwhise return an empty list.
  * Example : List(List(->), List(a), List(b, c, d, e, f, h))
  * @return (Boolean, List[List[String]])
  */
  def sequenceCut(log: List[List[String]], sc: SparkContext, DFG: (Array[Array[Int]], List[String])) : (Boolean, List[List[String]]) = {

    // Controllo se riga o colonna della
    // prima attività del DFG sono tutti 0
    var allZero : Boolean = true
    // Scorro la RIGA della prima attività
    for(elem <- DFG._1(0)) {
      if(elem == 1) {
	allZero = false
      }
    }
    // Se la riga non ha tutti 0 provo con la COLONNA
    if(!allZero) {
      allZero = true
      for(elem <- DFG._1) {
        if(elem(0) == 1) {
	  allZero = false
        }
      }
    }

    var result = ListBuffer[List[String]]()
    // Se riga o colonna sono uguali a 0 allora
    // è un sequenceCut.
    if(allZero) {
      result += List("->")
      result += List(DFG._2.head)
      result += DFG._2.tail
    }

    (allZero, result.toList)
  }

  /**
  * Concurrent Cut
  * ToDo...
  */
  def concurrentCut(log: List[List[String]], sc: SparkContext, DFG: (Array[Array[Int]], List[String])) : (Boolean, List[List[String]]) = {

    (false, List(List()))
  }

  /**
  * Loop Cut
  * ToDo...
  */
  def loopCut(log: List[List[String]], sc: SparkContext, DFG: (Array[Array[Int]], List[String])) : (Boolean, List[List[String]]) = {

    (false, List(List()))
  }
    
}
