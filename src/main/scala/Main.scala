import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

import IM.IMFramework._
import IM.Utilities._

object Main extends App {

  val conf = new SparkConf()
    .setAppName("InductiveMIner")
    .setMaster("local[1]")
  val sc = new SparkContext(conf)
  val rootLogger = Logger
    .getRootLogger()
    .setLevel(Level.ERROR)

  println("\n")
  printColor("yellow", "*** Inductive Miner on Apache Spark ***")
  printColor("yellow", "** Gianluca Bonifazi - Gianpio Sozzo **")
  printColor("yellow", "* Università Politecnica delle Marche *")
  println("\n")

  // Example: Read log from Hadoop hosted file
  // val log = sc.textFile("hdfs://....")

  /** Log p.196 - (-->, {a}, {b,c,d,e}) **/
  /*val log = List(
    List("a","b","c","d","e"),
    List("a","d","b","e"),
    List("a","e","b"),
    List("a","c","b"),
    List("a","b","d","e","c")
  )*/

  /** Log p.200 - (-->, {a,b}, {c}, {d,e}) - (X, {a}, {b}) - (-->, {c}, {d,e}) - (X, {d}, {e}) **/
  /*val log = List(
    List("a","c","d"),
    List("b","c","e")
  )*/

  /** Log p.202 - (X, {a,b}, {c}) **/
  /*val log = List(
    List("a","b"),
    List("c","c","c")
  )*/

  /** Log p.203 - (-->, {a,b}, {c}) **/
  /*val log = List(
    List("a","b","c"),
    List("b","a","c")
  )*/

  /** NOOOOO Log p.216 - (X, {a,b}, {c}) - Xor splitting IMf **/
  /*val log = List(
    List("a","b"),
    List("c","c","c"),
    List("a","b","c")
  )*/

  /** NOOOOO Log p.218 - (-->, {a,b}, {c}) - Seq splitting Imf **/
  /*val log = List(
    List("a","b","c"),
    List("b","a","c"),
    List("c","a","b","c")
  )*/

  // Log p.221 (Imf) - (-->, {a}, {b,c,d,e}) **/
  val log = List(
    List("a","b","c","d","e"),
    List("a","b","c","d","e"), 
    List("a","b","c","d","e"), 
    List("a","b","c","d","e"), 
    List("a","b","c","d","e"), 
    List("a","b","c","d","e"), 
    List("a","b","c","d","e"), 
    List("a","b","c","d","e"), 
    List("a","b","c","d","e"), 
    List("a","b","c","d","e"),
    List("a","d","b","e"),
    List("a","d","b","e"), 
    List("a","d","b","e"), 
    List("a","d","b","e"), 
    List("a","d","b","e"), 
    List("a","d","b","e"), 
    List("a","d","b","e"), 
    List("a","d","b","e"), 
    List("a","d","b","e"), 
    List("a","d","b","e"), 
    List("a","e","b"), 
    List("a","e","b"), 
    List("a","e","b"), 
    List("a","e","b"), 
    List("a","e","b"), 
    List("a","e","b"), 
    List("a","e","b"), 
    List("a","e","b"), 
    List("a","e","b"), 
    List("a","e","b"), 
    List("a","c","b"),  
    List("a","c","b"),  
    List("a","c","b"),  
    List("a","c","b"),  
    List("a","c","b"),  
    List("a","c","b"),  
    List("a","c","b"),  
    List("a","c","b"),  
    List("a","c","b"),  
    List("a","c","b"), 
    List("a","b","d","e","c"),
    List("a","b","d","e","c"),
    List("a","b","d","e","c"),
    List("a","b","d","e","c"),
    List("a","b","d","e","c"),
    List("a","b","d","e","c"),
    List("a","b","d","e","c"),
    List("a","b","d","e","c"),
    List("a","b","d","e","c"),
    List("a","b","d","e","c"),
    List("c","a","b"),
    List("a","h")
  )

  // (-->, {a}, {b,c,d,e,f,h}) - (X, {b,c,h}, {d,e,f})
  /*val log = List(
    List("a","b","c"),
    List("a","c","b","h","c"),
    List("a","d","e"),
    List("a","d","e","f","d","e")
  )*/

  // (X, {a,b,c}, {d,e,f}, {g,h,i}) - (-->)x6
  /*val log = List(
    List("a","b","c"),
    List("d","e","f"),
    List("g","h","i")
  )*/

  /*val log = List(
    List("a","b","c"),
    List("b","e","f"),
    List("g","h","i")
  )*/

  val IMtype = chooseIM()
  var graph = createDFG(log, IMtype._1, sc)

  printDFG(graph, false)

  IMFramework(graph, IMtype._1, IMtype._2)

  sc.stop

}
