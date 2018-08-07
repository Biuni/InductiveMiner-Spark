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

  /*val log = List(
    List("a","b"),
    List("c","c","c")
  )*/

  /*val log = List(
    List("a","b","c"),
    List("a","c","b","h","c"),
    List("a","d","e"),
    List("a","d","e","f","d","e")
  )*/

  // Log di esempio per Imf
  /*val log = List(
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
  )*/

  /*val log = List(
    List("a","b","c"),
    List("d","e","f"),
    List("g","h","i")
    )*/

  val log = List(
    List("a","b","c"),
    List("b","e","f"),
    List("g","h","i")
  )

  //graph.vertices.foreach(println)
  //graph.edges.foreach(println)

  /*val vertexName: RDD[(VertexId, (String))] =
    sc.parallelize(Array((1L, ("a")), (2L, ("b")),
	               (3L, ("c")), (4L, ("d")),
	               (5L, ("e")), (6L, ("f")),
	               (7L, ("h"))))
  // Create an RDD for edges
  val edgeName: RDD[Edge[String]] =
    sc.parallelize(Array(Edge(1L, 2L, "1"), Edge(2L, 3L, "1"),
	               Edge(1L, 3L, "1"),   Edge(3L, 2L, "1"),
		       Edge(2L, 7L, "1"),   Edge(7L, 3L, "1"),
		       Edge(1L, 4L, "1"),   Edge(4L, 5L, "1"),
		       Edge(5L, 6L, "1"),   Edge(6L, 4L, "1")))

  val graph = Graph(vertexName, edgeName)*/

  val IMtype = chooseIM()

  var graph = createGraph(log, IMtype._1, sc)

  println("\n*** GRAPH ***\n")
  println("* EDGES *\n")
  graph.edges.foreach(println)
  println("\n")
  println("* VERTICES *\n")
  graph.vertices.foreach(println)
  println("\n")

  //IMFramework(log, sc, IMtype._1, IMtype._2)

  if(!IMtype._1)
    IMFramework(graph)
    else IMFrameworkInf(graph,IMtype._2)

  sc.stop

}
