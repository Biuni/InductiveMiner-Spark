package IM

import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd._

object FilterLog {

  /**
  * Filter Log
  * 
  * Filter the DFG using the threshold.
  */
  def filterLog(graph: Graph[String,String], threshold: Float) : (Graph[String,String]) = {

    var src = new ListBuffer[Long]()
    var dst = new ListBuffer[Long]()
    var weights = new ListBuffer[String]()
    var i = 0
    var isUnd : (Boolean,Int) = null
    var isUnder : Boolean = false
    var pos : Int = -1
    var edgeCount = graph.edges.count()
    var edgeDel : Int = -1
    var srcDel = new ListBuffer[Long]()
    var dstDel = new ListBuffer[Long]()

    var toDelete : Array[(List[Long],List[Long])] = graph.triplets.map(triplet => {
      // Aggiunge alle relative liste l'id sorgente e l'id destinazione di ogni arco
      src += triplet.srcId
      dst += triplet.dstId

      // Per ogni arco aggiunge il suo peso nella relativa lista
      weights += triplet.attr

      // Se è il primo arco passo al secondo
      if (src.length > 1) {
        /* Se l'arco i-esimo non ha lo stesso nodo sorgente dell'arco precedente allora
         * valuta le frequenze di ogni arco con lo stesso nodo sorgente
         */
        if(src(i) != src(i-1)) {
          weights -= triplet.attr
          isUnd = checkFreq(weights, threshold)
          isUnder = isUnd._1
          pos = isUnd._2
          // Se si è trovato un arco sotto-soglia valuto la posizione dell'arco da eliminare
            if(isUnd._1) {
              edgeDel = i - pos - 1
              srcDel += src(edgeDel)
              dstDel += dst(edgeDel)
              pos = 0
	    }
         /* Se non si è trovato un arco-soglia svuota la lista dei pesi degli archi partenti da uno stesso nodo
          * e vi aggiunge il peso del nuovo arco (il quale ha un nodo sorgente diverso da quello dell'arco precedente */
          weights.clear()
          weights += triplet.attr
        }
      }

      // Se è l'arco finale valuta le frequenze di ogni arco
      if(i == edgeCount-1) {
        isUnd = checkFreq(weights, threshold)
        // Se si è trovato un arco sotto-soglia valuto la posizione dell'arco da eliminare
        if(isUnd._1) {
          edgeDel = i - pos - 1
          srcDel += src(edgeDel)
          dstDel += dst(edgeDel)
        }
      }

      i = i+1

      (srcDel.toList,dstDel.toList)
	
    }).collect()

    // Crea lista di sourceID e destID degli archi da eliminare
    var sourceDel = new ListBuffer[Long]()
    var destDel = new ListBuffer[Long]()
	
    // Es. toDelete = List( (List(),List()), List(List(1),List(4)), List(List(1,3),List(4,1)) )
    if (toDelete.length > 1) {
      var l = toDelete(toDelete.length-1)
      for(i <- l._1) {
        // Es. sourceDel = List(1,3)
	sourceDel += i
      }
      for(i <- l._2) {
        // Es. destDel = List(4,1)
        destDel += i
      }
    }

    var graph2 : Graph[String,String] = graph

    for (j <- 0 until sourceDel.length) {
      // Filtra il grafo per ogni elemento in sourceDel e in destDel
      graph2 = filtering(graph2,sourceDel(j),destDel(j))
    }

    graph2
  }


  /** Crea il sottografo senza gli archi sotto-soglia e senza vertici con nessun arco entrante e uscente **/
  def filtering(graph: Graph[String,String], sdel: Long, ddel: Long) : Graph[String,String] = {

    /** Filtra gli archi **/
    var graphFiltered = graph.subgraph( epred=(triplet) => 
      
      ((triplet.srcId != sdel) || (triplet.dstId != ddel))

    )

    /** Filtra i vertici senza archi entranti e uscenti **/
    var lst = graphFiltered.outDegrees.map(x => x._1).collect()
    var lst2 = graphFiltered.inDegrees.map(x => x._1).collect()
    var union = lst.toList.union(lst2.toList)

    var graphFiltered2 = graphFiltered.subgraph( vpred=(id,att) => 
   
      union.contains(id)

    )

    graphFiltered2

  }

  /** Valuta se le frequenze degli archi che partono da uno stesso nodo sono sotto-soglia **/
  def checkFreq(weights: ListBuffer[String], threshold: Float) : (Boolean, Int) = {

    // CODE: https://s15.postimg.cc/9no9nrs0r/filter_Log.jpg

    var max : Int = -1
    var weights2 = new ListBuffer[String]()
    var isUnd : Boolean = false
    var w1Length : Int = weights.length-1
    var i = 0
    var pos : Int = -1

    // Per ogni singolo peso calcola se è minore di: threshold * max, dove max è il massimo peso degli altri archi (escluso quello in questione)
    for(weight <- weights) {
      w1Length = w1Length - 1
      weights2 = weights.drop(weight.toInt)
      for(w_weight <- weights2) {
	if(w_weight.toInt > max) {
	  max = w_weight.toInt
	}
      }

      // Se il peso è sotto-soglia salva la posizione in cui si trova nella lista di pesi esaminata
      if(weight.toInt < threshold * max) {
	isUnd = true
	if(i == 0) {pos = weights.length-1 }
	else pos = w1Length
      }

	i = i +1

    }

   (isUnd, pos)

  }

}
