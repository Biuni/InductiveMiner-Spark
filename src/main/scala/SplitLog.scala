package IM

import org.apache.spark.{SparkConf, SparkContext}

object SplitLog {

  /**
  * SplitLog
  * 
  * After finding a cut, the IM framework splits the log into several sub-logs,
  * on which recursion continues.
  */
  def SplitLog(log: List[List[String]], cut: List[List[String]], sc: SparkContext) : Unit = {
    // 1. xorSplit(log)
    // 2. sequenceSplit(log)
    // 3. concurrentSplit(log)
    // 4. loopSplit(log)

    // CODE: https://s22.postimg.cc/c684p44g1/splitlof.jpg
  }
  
  ///////////////////////////////////////////////////////////////////////////
  // Operations
  ///////////////////////////////////////////////////////////////////////////

  /**
  * Xor Split
  * 
  * Return a List of List of List of String if the Cut founded is a XOR.
  * Otherwhise return an empty list.
  * example: List(List(List("a")), 
             List(List("b","d","f"), List("c","e","f"), List("c","d","f"))
  * @return List[List[List[String]]]
  */
  def xorSplit(log: List[List[String]], sc: SparkContext) : Unit = {

  }

  /**
  * Sequence Split
  * 
  * Return a List of List of List of String if the Cut founded is a SEQUENCE.
  * Otherwhise return an empty list.
  * example: List(List(List("a")), 
             List(List("b","d","f"), List("c","e","f"), List("c","d","f"))
  * @return List[List[List[String]]]
  */
  def sequenceSplit(log: List[List[String]], sc: SparkContext) : Unit = {

  }

  /**
  * Concurrent Split
  * ToDo...
  */
  def concurrentSplit(log: List[List[String]], sc: SparkContext) : Unit = {

  }

  /**
  * Loop Split
  * ToDo...
  */
  def loopSplit(log: List[List[String]], sc: SparkContext) : Unit = {

  }
    
}
