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

  /*val vertexName: RDD[(VertexId, (String))] =
    sc.parallelize(Array((1L, ("a")), (2L, ("b")),
	               (3L, ("c"))))
  // Create an RDD for edges
  val edgeName: RDD[Edge[String]] =
    sc.parallelize(Array(Edge(1L, 2L, "1"), Edge(3L, 3L, "1")))*/

  /*val log = List(
    List("a","b","c"),
    List("a","c","b","h","c"),
    List("a","d","e"),
    List("a","d","e","f","d","e")
  )*/

  val vertexName: RDD[(VertexId, (String))] =
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

  val graph = Graph(vertexName, edgeName)

  //val IMtype = chooseIM()
  
  //IMFramework(log, sc, IMtype._1, IMtype._2)

  IMFramework(graph)

  sc.stop

}
