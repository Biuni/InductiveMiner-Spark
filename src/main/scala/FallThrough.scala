package IM

import org.apache.spark.{SparkConf, SparkContext}

object FallThrough {

  /**
  * FallThrough
  * 
  * For some input event logs, no base case applies and no cut applies.
  * However, IM should return a process tree under all circumstances,
  * hence a fall through needs to be selected. As a last resort a 
  * flower model can be returned, i.e. a model that allows for any
  * behaviour over a given set of activities.
  */
  def FallThrough(log: List[List[String]], sc: SparkContext) : Unit = {
    // 1. emptyTraces(oog)
    // 2. activityOncePerTrace(log)
    // 3. activityConcurrent(log)
    // 4. strictTauLoop(log)
    // 5. tauLoop(log)
    // 6. flowerModel(log)

    // CODE: https://s22.postimg.cc/55fjihie9/fallthrough.png
  }
  
  ///////////////////////////////////////////////////////////////////////////
  // Operations
  ///////////////////////////////////////////////////////////////////////////

  /**
  * Empty Traces
  * TODO...
  */
  def emptyTraces(log: List[List[String]], sc: SparkContext) : Unit = {

  }

  /**
  * Activity Once Per Trace
  * TODO...
  */
  def activityOncePerTrace(log: List[List[String]], sc: SparkContext) : Unit = {

  }

  /**
  * Activity Concurrent
  * TODO...
  */
  def activityConcurrent(log: List[List[String]], sc: SparkContext) : Unit = {

  }

  /**
  * Strict Tau Loop
  * TODO...
  */
  def strictTauLoop(log: List[List[String]], sc: SparkContext) : Unit = {

  }

  /**
  * Tau Loop
  * TODO...
  */
  def tauLoop(log: List[List[String]], sc: SparkContext) : Unit = {

  }

  /**
  * Flower Model
  * TODO...
  */
  def flowerModel(log: List[List[String]], sc: SparkContext) : Unit = {

  }

}
