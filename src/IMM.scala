import breeze.numerics._
import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import scala.math.E

/**
  * Created by guido on 13/01/2017.
  */

object IMM {
  private def fact(n: Int): Int = if (n == 0) 1 else n * fact(n - 1)
  private def binomial(n: Int, k: Int) = fact(n) / (fact(k) * fact(n - k))


  //mariano pascarella
  //***********************
  def randomBFS(G: Graph[(String, Double), Float], vertexV : VertexId): Graph[(Int, Int, Double, String), Float]={

    val r = scala.util.Random

    val g = G.mapVertices( (id, attr) =>
      if (id == vertexV) (1, 0, attr._2, attr._1)
      else (0, 0, attr._2, attr._1)
    )

    val A = 1.1

    def vprog (id: VertexId, attr: (Int, Int, Double, String), message: Double) : (Int, Int, Double, String) = {
      if(message > 1.0){
        if(id == vertexV)
          (1, 1, attr._3, attr._4)
        else
          attr
      }else{
        if(attr._4 == "HN"){
          if(message >= attr._3)
            (1, 1, attr._3, attr._4)
          else
            (0, 1, attr._3, attr._4)
        }else
          (1, 1, attr._3, attr._4)
      }
    }
    //Message
    def sendMsg (triplet: EdgeTriplet[(Int, Int, Double, String), Float]) : Iterator[(VertexId, Double)] = {
      val sourceVertex = triplet.srcAttr
      val destVertex = triplet.dstAttr
      if(sourceVertex._2 == 0 && destVertex._1 == 1)
        Iterator((triplet.srcId, r.nextDouble()))
      else
        Iterator.empty
    }

    //Per mia scelta decido di prendere il valore più grande
    def mergeMsg (msg1: Double, msg2: Double) : Double = math.max(msg1, msg2)

    val rr = g.pregel(A, Int.MaxValue, EdgeDirection.In)(vprog, sendMsg, mergeMsg)

    rr

  }
  //***********************


  //mariano pascarella *edit teta,n ,added FR ,Boolean
  //***********************
  def nodeSelection(R: Array[Graph[(Int, Int, Double, String), Float]], k: Int, n: Int): (Array[VertexId],Int) ={
    val teta=R.length
    val SSet : Array[VertexId] = new Array[VertexId](k)
    var RRsetCover : Array[Boolean] = new Array[Boolean](teta)
    var FR : Array[Boolean] = Array.fill(teta)(false)
    for(j <- 0 until k){
      val cover : Array[Boolean] = new Array[Boolean](teta)
      val tempCover : Array[Boolean] = new Array[Boolean](teta)
      var max = 0
      var idS = 0
      for(z <- 0 until n){
        var count = 0
        for(h <- 0 until teta){
          if(!RRsetCover(h)){
            if(R(h).subgraph(vpred = (id, _) => id == (z+1)).numVertices > 0){
              count = count + 1
              cover(h) = true
            }else
              cover(h) = false
          }
        }
        if(max < count){
          max = count
          idS = z
          Array.copy(cover,0,tempCover,0,cover.length)
        }
      }
      SSet(j) = idS+1
      RRsetCover =  tempCover    // Qui basta il riferimento, tanto viene Ricreato un nuovo array per tempCover
      FR=FR.zip(tempCover).map { case (x, y) => x || y }
    }
    (SSet,FR.count(_ == true)/teta)
  }
  //***********************

  def sampling (G:Graph[(String, Double), Float], k: Int, ε1 : Double, l: Double) : Array[Graph[(Int, Int, Double, String), Float]] = {
    val n = G.subgraph(vpred = (_, attr) => attr._1 == "N").numVertices.toInt//numVertices.toInt
    println("n = "+n)
    //val log2 = (x: Double) => log10(x)/log10(2.0)
    val r = scala.util.Random

    var LB =1

    val ε = ε1 *sqrt(2)

    println("print = "+ (2+(2/3)*ε)*(log(binomial(n.toInt,k))+(l*log(n))+log(log2(n)))*n)

    val lambda = ((2+(2/3)*ε)*(log(binomial(n.toInt,k))+(l*log(n))+log(log2(n)))*n)/pow(ε,2)//Equation 9
    println("lambda = "+lambda)
    val alpha =sqrt(l*log(n)+log(2))
    val beta = sqrt((1-1/E)*(log(binomial(n.toInt,k))+(l*log(n))+log(2)))
    val lambda1 =2*n*pow((1-1/E)*alpha+beta,2)
    println("alpha beta lambda1 -> "+alpha+" "+beta+" "+lambda1)
    val fine = log2(n-1)
    var i =1
    var LBbreak=false
    println("first while")
    while(i<=fine  && !LBbreak){
      val x=n / pow(2,i)
      val tetai=(lambda/x).toInt

      //genera RR sets
      //mariano pascarella
      //***********************
      val RRset : Array[Graph[(Int, Int, Double, String), Float]] = new Array[Graph[(Int, Int, Double, String), Float]](tetai)
      for(i <- 0 until tetai){
        RRset(i) = randomBFS(G, r.nextInt(n)+1).subgraph(vpred = (_, attr) => attr._1 == 1 && attr._4 == "N")
      }
      //***********************

      println("internal node selection")
      val res : (Array[VertexId],Int) =nodeSelection(RRset,k,n)
      val FRsR =res._2
      if(n*FRsR>=(1+ε)*x){
        LB= n*FRsR.toInt/(1+ε.toInt)
        LBbreak=true
      }
      i += 1
    }

    val teta=(lambda1/LB).toInt
    //genera RR sets
    //mariano pascarella
    //***********************
    val R : Array[Graph[(Int, Int, Double, String), Float]] = new Array[Graph[(Int, Int, Double, String), Float]](teta)
    for(i <- 0 until teta){
      R(i) = randomBFS(G, r.nextInt(n)+1).subgraph(vpred = (_, attr) => attr._1 == 1 && attr._4 == "N")
    }
    //***********************

    R
  }

  def imm(G:Graph[(String, Double), Float], k: Int, ε1 : Double, l1: Double): Array[VertexId] ={
    val n = G.subgraph(vpred = (_, attr) => attr._1 == "N").numVertices.toInt//numVertices.toInt
    val l=l1*(1+log(2)/log(n))
    println("sampling")
    val R:Array[Graph[(Int, Int, Double, String), Float]]=sampling (G, k, ε1, l)
    println("node selection")
    nodeSelection(R,k,n)._1
  }

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\spark")
    val conf = new SparkConf().setAppName("IMM").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    val path = "C:\\Users\\guido\\IdeaProjects\\IMM\\datasets"
    val nodoRDD: RDD[(VertexId, (String, Double))] = sc.textFile( path+"\\node.txt").map { line =>
      val row = line.split(";")
      (row(0).toLong, (row(1), 1))
    }

    val hyperNodoRDD: RDD[(VertexId, (String, Double))] = sc.textFile(path+"\\edgeExpa.txt").map { line =>
      val row = line.split(";")
      (row(0).toLong, (row(1), row(2).toDouble))
    }

    //edge
    val edgeRDD1: RDD[Edge[Float]] = sc.textFile(path+"\\edgeExpaOut.txt").map { line =>
      val row = line.split(";")
      val idIn = row(0).toLong
      val peso = row(2).toFloat
      val idOut = row(3).toLong
      Edge(idOut, idIn, peso)
    }


    val edgeRDD2: RDD[Edge[Float]] = sc.textFile(path+"\\edgeExpaIn.txt").map { line =>
      val row = line.split(";")
      val idIn = row(0).toLong
      val peso = row(2).toFloat
      val idOut = row(3).toLong
      Edge(idIn, idOut, peso)
    }

    val edgeRDD : RDD[Edge[Float]] = edgeRDD1.++(edgeRDD2)

    val vertexRDD : RDD[((VertexId, (String, Double)))]  = nodoRDD.union(hyperNodoRDD)

    val graph: Graph[(String, Double), Float] = Graph(vertexRDD, edgeRDD)

//IMM call
    val l = 1
    val ε = 0.2
    val k = 2

    val Sstar  : Array[VertexId] =imm(graph, k, ε, l)
    for(e <- Sstar) println(e)

    sc.stop()
  }


}
