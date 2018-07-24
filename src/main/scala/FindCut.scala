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
  def checkFindCut(log: List[List[String]], sc: SparkContext, DFG: (Array[Array[Int]], List[String], Array[Array[Int]])) : (Boolean, List[List[String]]) = {

    // CODE: https://s22.postimg.cc/esj1sl17l/Find_Cut.jpg
    var cutFound : Boolean = true
    var result : List[List[String]] = null

    val xor = xorCut(log, sc, DFG)
    if(xor._1) {
      // xorCut found
      result = xor._2
    } else {

      val seq = sequenceCut(log, sc, DFG)
      if(seq._1) {
        // sequenceCut found
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
  def xorCut(log: List[List[String]], sc: SparkContext, DFG: (Array[Array[Int]], List[String], Array[Array[Int]])) : (Boolean, List[List[String]]) = {

    // Controllo se esiste una riga o una colonna del DFG 
    // dove sono tutti 0 perchè se così fosse allora
    // si può skippare al seqCut
    var isXor : Boolean = true
    for(i <- 0 to DFG._2.length - 1) {

      var checkXorRow : Int = 0
      var checkXorCol : Int = 0
      if(isXor == true) {
        // RIGA
        for(elem <- DFG._1(i)) {
          if(elem == 1) {
	    // NON E' UN SEQ
            checkXorRow += 1
          }
        }
        // COLONNA
        for(elem <- DFG._1) {
          if(elem(i) == 1) {
	    // NON E' UN SEQ
            checkXorCol += 1
          }
        }

        if((checkXorRow == 0 || checkXorCol == 0) && (i<1)){
          isXor = false
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

    // Trovo la lista con lunghezza maggiore
    var maxLength : Int = 0
    for(elem <- result) {
      if(maxLength < elem.length) {
        maxLength = elem.length
      }
    }

    var result2 = new ListBuffer[List[String]]()
    // Controllo tutte le liste con lunghezza
    // minore di quella massima per vedere
    // se è contenuta in una delle altre liste
    for(elem <- result) {
      if(elem.length < maxLength) {
        for(a <- 0 until result.length) {
          // Se è una sotto lista, va rimossa
          if(elem.forall(result(a).contains) && (elem != result(a))) {
            result2.+=(elem)
          }
        }
      }
    }

    result2.toList

    var res3 = result.diff(result2)
    // La lista dei risultati ha di nuovo in distinct perchè
    // così facendo elimino le liste uguali, cioè quelle che
    // formano lo XOR lasciando solo le attività separate
    val res = res3.toList.distinct.toList

    // Il cut va fatto tra due liste di attività quindi
    // se ne ho meno di 3 (perchè comprende anche
    // il segno "X") allora non si tratta di uno xorCut.
    if(res.length < 3) {
      isXor = false
    }

    (isXor, res)
  }

  /**
  * Sequence Cut
  * 
  * Return a List of List of String if the Sequence Cut is found in the log.
  * Otherwhise return an empty list.
  * Example : List(List(->), List(a), List(b, c, d, e, f, h))
  * @return (Boolean, List[List[String]])
  */
  def sequenceCut(log: List[List[String]], sc: SparkContext, DFG: (Array[Array[Int]], List[String], Array[Array[Int]])) : (Boolean, List[List[String]]) = {

    // Controllo se riga o colonna delle
    // attività del DFG sono tutti 0
    var allZero : Boolean = false
    var seqActivity : Int = 0
    for(i <- 0 to DFG._2.length - 1) {

      var checkSeqRow : Int = 0
      var checkSeqCol : Int = 0
      if(allZero == false) {
        // RIGA
        for(elem <- DFG._1(i)) {
          if(elem == 1) {
	    // NON E' UN SEQ
            checkSeqRow += 1
          }
        }
        // COLONNA
        for(elem <- DFG._1) {
          if(elem(i) == 1) {
	    // NON E' UN SEQ
            checkSeqCol += 1
          }
        }

        if(checkSeqRow == 0 || checkSeqCol == 0){
          allZero = true
          seqActivity = i
        }
      }

    }

    var result = ListBuffer[List[String]]()
    // Se riga o colonna sono uguali a 0 allora
    // è un sequenceCut.
    if(allZero) {
      result += List("->")
      result += List(DFG._2(seqActivity))
      result += DFG._2.filter(_ != DFG._2(seqActivity))
    }

    val res = result.toList

    // Il cut va fatto tra due liste di attività quindi
    // se ne ho meno di 3 (perchè comprende anche il 
    // segno "->") allora non si tratta di uno sequenceCut.
    if (res.length < 3) {
      allZero = false
    }

    (allZero, res)
  }

  /**
  * Concurrent Cut
  * ToDo...
  */
  def concurrentCut(log: List[List[String]], sc: SparkContext, DFG: (Array[Array[Int]], List[String], Array[Array[Int]])) : (Boolean, List[List[String]]) = {

    (false, List(List()))
  }

  /**
  * Loop Cut
  * ToDo...
  */
  def loopCut(log: List[List[String]], sc: SparkContext, DFG: (Array[Array[Int]], List[String], Array[Array[Int]])) : (Boolean, List[List[String]]) = {

    (false, List(List()))
  }
    
}
