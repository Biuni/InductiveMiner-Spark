import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable.ListBuffer

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

  /*val log = List(
    List("a","b","c"),
    List("a","c","b","h","c"),
    List("a","d","e"),
    List("a","d","e","f","d","e")
  )*/

  /*val log = List(
    List("a","z","b","c"),
    List("a","z","c","b","h","c"),
    List("a","z","d","e"),
    List("a","z","d","e","f","d","e"),
    List("a","z","r","t"),
    List("a","z","t","t","r","g","r")
  )*/

  /*val log = List(
    List("b","c"),
    List("c","b","h","c"),
    List("d","e"),
    List("d","e","f","d","e")
  )*/

  /*val log = List(
    List("z","b","c"),
    List("z","c","b","h","c"),
    List("z","d","e"),
    List("z","d","e","f","d","e")
  )*/

  /*val log = List(
    List("b","c","a"),
    List("c","b","h","c","a"),
    List("d","e","a"),
    List("d","e","f","d","e","a")
  )*/

  /*val log = List(
    List("b","c","z"),
    List("c","b","h","c","z"),
    List("d","e","z"),
    List("d","e","f","d","e","z")
  )*/

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
    List("c","a","b")
  )

  printColor("green", "Choose a variant of Inductive Miner:\n")
  printColor("green", "1 - Inductive Miner (IM)\n")
  printColor("green", "2 - Inductive Miner - infrequent (IMf)\n")

  var imf: Boolean = false
  var threshold: Float = -1
  var c: Int = scala.io.StdIn.readInt()

  if(c == 2) {
    imf = true
    printColor("green", "Set a Noise threshold (0 to 1): ")
    threshold = scala.io.StdIn.readFloat()
    while(threshold < 0 || threshold > 1) {
      printColor("red", "Threshold not valid - Set a new threshold (0 to 1): ")
      threshold = scala.io.StdIn.readFloat()
    }
    
  }
  
  IMFramework(log, sc, imf, threshold)

  sc.stop

}
