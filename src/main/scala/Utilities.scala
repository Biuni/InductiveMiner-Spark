package IM

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer

object Utilities {

  def createDFG : Array[Array[Int]] = {
    val rddLog = sc.parallelize(log)
    val activities = rddLog.distinct().collect().toList.flatten.distinct.sorted

    val rddEdges = sc.parallelize(log)
    var edges = rddEdges.map(list =>{
      var aux = new ListBuffer[(String,String)]()
      for(a <- 0 until list.length-1){
        aux.+=((list(a),list(a+1)))
      }
      aux.toList
    }).collect().toList.flatten

    var matrix = new Array[Array[Int]](activities.length)
    var rddActivities = sc.parallelize(activities)

    var arrT = rddActivities.map(list => {
      var riga = activities.indexOf(list)
      var arr =  new Array[Int](activities.length)
      for(a<-0 until activities.length){
        if(edges.contains((list,activities(a)))){
          arr.update(a, 1)
        }
      }
      (riga,arr)
    }).sortByKey(true).collect()

    for(a<-0 until arrT.length){
      matrix(a)=arrT(a)._2
    }
    
    // Debug Only
    stampaMatrice(matrix, activities)

    matrix
  }

  def stampaMatrice(mAtr: Array[Array[Int]], sigma: List[String]) = {

    val arr = sigma.toArray.sorted
    print("           ")
    for(a<-0 until sigma.length){
      print(arr(a)+" ")
    }
    println()
    for(a<-0 until sigma.length){
      print("Riga("+a+"): "+arr(a)+" ")
      for(b<-0 until sigma.length){
        print(mAtr(a)(b)+" ")
      }
      println()
    }

  }

}
