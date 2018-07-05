package IM

import org.apache.spark.{SparkConf, SparkContext}

object FindCut {

  /**
  * FindCut
  * 
  * The IM searches for several cuts using the cut footprints.
  * It attempts to find cuts in the order: XOR, SEQUENCE, CONCURRENT, LOOP.
  */
  def FindCut(log: List[List[String]], sc: SparkContext) : Unit = {
    // 1. xorCut(log)
    // 2. sequenceCut(log)
    // 3. concurrentCut(log)
    // 4. loopCut(log)

    // CODE: https://s22.postimg.cc/esj1sl17l/Find_Cut.jpg
  }
  
  ///////////////////////////////////////////////////////////////////////////
  // Operations
  ///////////////////////////////////////////////////////////////////////////

  /**
  * Xor Cut
  * 
  * Return a List of List of String if the Xor Cut is found in the log.
  * Otherwhise return an empty list.
  * Example : List(List("X"), List("a","b"))
  * @return List[List[String]]
  */
  def xorCut(log: List[List[String]], sc: SparkContext) : Unit = {

  }

  /**
  * Sequence Cut
  * 
  * Return a List of List of String if the Sequence Cut is found in the log.
  * Otherwhise return an empty list.
  * Example : List(List("->"), List("a","b"))
  * @return List[List[String]]
  */
  def sequenceCut(log: List[List[String]], sc: SparkContext) : Unit = {

  }

  /**
  * Concurrent Cut
  * ToDo...
  */
  def concurrentCut(log: List[List[String]], sc: SparkContext) : Unit = {

  }

  /**
  * Loop Cut
  * ToDo...
  */
  def loopCut(log: List[List[String]], sc: SparkContext) : Unit = {

  }
    
}
