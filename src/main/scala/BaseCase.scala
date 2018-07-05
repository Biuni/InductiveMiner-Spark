package IM

import org.apache.spark.{SparkConf, SparkContext}

object BaseCase {

  /**
  * BaseCase
  * 
  * The base cases for the IM algorithm provide an end to the recursion.
  * IM implements the BaseCase function of the IM framework using several steps:
  * it tries several base cases and returns the first matching one.
  * As the base cases are mutually exclusive, so their order is irrelevant.
  */
  def BaseCase(log: List[List[String]], sc: SparkContext) : Unit = {
    // 1. emptyLog(log)
    // 2. singleActivity(log)

    // CODE: https://s22.postimg.cc/cucdu8t6p/Base_Case.jpg
  }
  
  ///////////////////////////////////////////////////////////////////////////
  // Operations
  ///////////////////////////////////////////////////////////////////////////

  /**
  * Empty Log
  * 
  * Return true when the log contains no traces.
  * False otherwise. 
  * @return Boolean
  */
  def emptyLog(log: List[List[String]], sc: SparkContext) : Unit = {

  }

  /**
  * Single Activity
  * 
  * Return true when the log contains only traces with a single activity.
  * False otherwise. 
  * @return Boolean
  */
  def singleActivity(log: List[List[String]], sc: SparkContext) : Unit = {

  }

}
