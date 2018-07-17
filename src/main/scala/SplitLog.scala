package IM

import org.apache.spark.{SparkConf, SparkContext}

object SplitLog {

  /**
  * SplitLog
  * 
  * After finding a cut, the IM framework splits the log into several sub-logs,
  * on which recursion continues.
  */
  def checkSplitLog(log: List[List[String]], cut: List[List[String]], sc: SparkContext) : List[List[List[String]]] = {

    // CODE: https://s22.postimg.cc/c684p44g1/splitlof.jpg

    val newLog = cut(0)(0) match {
      case "X" => xorSplit(log, cut, sc)
      case "->" => sequenceSplit(log, cut, sc)
      case "||" => concurrentSplit(log, cut, sc)
      case "*" => loopSplit(log, cut, sc)
    }

    newLog
  }
  
  ///////////////////////////////////////////////////////////////////////////
  // Operations
  ///////////////////////////////////////////////////////////////////////////

  /**
  * Xor Split
  * 
  * Return a List of List of List of String if the Cut founded is a XOR.
  * Otherwhise return an empty list.
  * Example: List(List(List(b,c),List(c,b,h,c)), List(List(d,e),List(d,e,f,d,e))
  * @return List[List[List[String]]]
  */
  def xorSplit(log: List[List[String]], cut: List[List[String]], sc: SparkContext) : List[List[List[String]]] = {

    List(List(List()))
  }

  /**
  * Sequence Split
  * 
  * Return a List of List of List of String if the Cut founded is a SEQUENCE.
  * Otherwhise return an empty list.
  * Example: List(List(List(a)), List(List(b,c),List(c,b,h,c),List(d,e),List(d,e,f,d,e))
  * @return List[List[List[String]]]
  */
  def sequenceSplit(log: List[List[String]], cut: List[List[String]], sc: SparkContext) : List[List[List[String]]] = {

    val rddSplit = sc.parallelize(log)
    var newLog = rddSplit.map(list => {
      list.filter(_ != cut(1)(0))
    }).collect().toList

    List(List(List(cut(1)(0))), newLog)
  }

  /**
  * Concurrent Split
  * ToDo...
  */
  def concurrentSplit(log: List[List[String]], cut: List[List[String]], sc: SparkContext) : List[List[List[String]]] = {

    List(List(List()))
  }

  /**
  * Loop Split
  * ToDo...
  */
  def loopSplit(log: List[List[String]], cut: List[List[String]], sc: SparkContext) : List[List[List[String]]] = {

    List(List(List()))
  }
    
}
