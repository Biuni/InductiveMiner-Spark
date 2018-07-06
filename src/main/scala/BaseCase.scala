package IM

import scala.collection.mutable.ListBuffer
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
  def checkBaseCase(log: List[List[String]], sc: SparkContext) : List[String] = {

    // CODE: https://s22.postimg.cc/cucdu8t6p/Base_Case.jpg

    var result = ListBuffer[String]()

    if(emptyLog(log, sc)) {
      result += "tau"
    } else if (singleActivity(log, sc)) {
      result += log(0).head
    } else {
      result
    }

    result.toList
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
  def emptyLog(log: List[List[String]], sc: SparkContext) : Boolean = {

    var result : Boolean = false
    val rdd = sc.parallelize(log)
    val checkLog = rdd.distinct.collect().toList

    // Controllo se ho un solo tipo di tracce
    if(checkLog.length == 1) {
      // Controllo se la traccia è vuota
      // quindi è un EmptyLog
      if(checkLog(0).isEmpty) {
        result = true
      }
    }

    result
  }

  /**
  * Single Activity
  * 
  * Return true when the log contains only traces with a single activity.
  * False otherwise. 
  * @return Boolean
  */
  def singleActivity(log: List[List[String]], sc: SparkContext) : Boolean = {

    var result : Boolean = false
    val rdd = sc.parallelize(log)
    val checkLog = rdd.distinct.collect().toList

    // Controllo se ho un solo tipo di tracce
    if(checkLog.length == 1) {
      // Controllo se la traccia non è vuota
      // quindi è una SingleActivity
      if(!checkLog(0).isEmpty) {
        result = true
      }
    }

    result
  }

}
