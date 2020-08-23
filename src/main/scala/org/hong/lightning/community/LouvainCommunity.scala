package org.hong.lightning.community

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.Try

/**
  * Created by huang hong
  */
case class VertexInfo(vid: Long,
                      community: Long = -1L,
                      communitySigmaTot: Double = 0.0, //this value might be outdated in the execution
                      selfWeight: Double = 0.0, // weight of self loop, used when the node represents an aggregation of internal nodes of the same community in the previous level
                      adjacentWeight: Double = 0.0, //outgoing edges except the self loop
                      changed: Boolean = false,
                      converge: Boolean = false
                     )

object LouvainCommunity {
  /**
    *
    *  numPartitions           图的块数，大图推荐2000到5000
    *  probability              节点更新社区的概率，设成0.6
    *  numberOfIterations       求社区的迭代次数 设成10次
    *
    */
  def run(edgeRDD:RDD[((Long,Long),Double)],numPartitions:Int,numIterations:Int,probability:Double):RDD[(Long,Long)] ={

    val graph=createLouvainGraphFromMap(edgeRDD)
    val community=louvainOneLevel(graph,probability,numIterations).vertices.mapValues(_.community)
    community

  }


  //下面是使用graphX的实现，留着备用




  def createLouvainGraphFromMap(edgeInput: RDD[((Long, Long), Double)]): Graph[VertexInfo, Double] = {

    //    val textFile = sc.textFile(path)

    val edg = edgeInput


    val edgeSeq = edg//.distinct
    val edges = edgeSeq.map { case (id, d) => Edge(id._1, id._2, d) }
    val graph = Graph.fromEdges(edges, 0.0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK_SER)
      .partitionBy(PartitionStrategy.EdgePartition2D)

    /*
     * collect adjacent weights of nodes in the graph
     * */
    //fill in adjacent weights with mapreduceTriplet
    //这里假设无向图，并且一条边重复两次，正向反向各一次
    def msgFun(triplet: EdgeContext[_, Double, Double]) {
      triplet.sendToDst(triplet.attr)
    }

    /*
    * 计算边的投资权重：目标节点计算投资总额
    * */
    def reduceFun(a: Double, b: Double) = a + b


    val vertexGroup: VertexRDD[(Double)] = graph.aggregateMessages[Double](msgFun, reduceFun)

    println(vertexGroup.count())

    //initializing the vertex. for the purpose of verification, the selfWeight variable is set to 1.0, which means the total weight of the internal edges of the community in the "previous level" is 0.5. Because this is an undirected graph, the self-loop is weighted 0.5x2=1.0
    val LouvainGraph = graph.outerJoinVertices(vertexGroup)((vid, name, weight) => {
      VertexInfo(vid = vid, selfWeight = 0.0, community = vid, communitySigmaTot = weight.getOrElse(0.0), adjacentWeight = weight.getOrElse(0.0))
    })


    LouvainGraph

  }




  def louvainOneLevel(initialGraph: Graph[VertexInfo, Double], probability: Double,numberOfIterations:Int): /*RDD[(VertexInfo,VertexInfo,Double)]*/Graph[VertexInfo, Double] = {
    var changed = false
    var counter = 0
    var converge = false
    var LouvainGraph = initialGraph


    var communityInfo:VertexRDD[Map[Long,(Double,Double)]]=null
    var gw = 0.0
    /*
		     * calculate total weight of the network
		     * */
    //The total weight of the network, it is twice the actual total weight of the whole graph.Because the self-loop will be considered once, the other edges will be considered twice.
    val graphWeight = LouvainGraph.vertices.values.map(v => v.selfWeight + v.adjacentWeight).reduce(_ + _)

    gw = graphWeight



    do {

      /*
		     *operations of collecting sigmaTot
		     * */
      //Calculate sigma tot for each community
      val sigmaTot = LouvainGraph.vertices.map(v => (v._2.community, v._2.selfWeight + v._2.adjacentWeight)).reduceByKey(_ + _).cache()

      //assign to each vertex the sigmaTot value of its community
      val communitiesSigmaTot=LouvainGraph.vertices.map(v=>(v._2.community,v._1)).cache()
      val vertice_sigmaTot=sigmaTot.join(communitiesSigmaTot).map{case (_,(v_sigmaTot,vid))
      =>{
        (vid,v_sigmaTot)
      }}.cache()



      val preG1=LouvainGraph
      LouvainGraph=LouvainGraph.outerJoinVertices(vertice_sigmaTot)(
        (vid,v,v_sigmaTot)
        =>{

          VertexInfo(vid=vid, selfWeight =v.selfWeight , community = v.community, communitySigmaTot = v_sigmaTot.getOrElse(0), adjacentWeight = v.adjacentWeight,changed=v.changed,converge=v.converge)
        }
      )
      LouvainGraph.edges.count()
      LouvainGraph.vertices.count()

      preG1.unpersist(blocking = false)



      /*
		     * exchange community information and sigmaTot
		     * */
      //exchange community information and sigmaTot, prepare to calculate k_i_in
      val preMessages = communityInfo

      communityInfo = LouvainGraph.aggregateMessages(exchangeMsg, mergeMsg).cache() //The problem is, when mapReduceTriplet, only work on Louvain graph, not newLouvain Graph.
      println(communityInfo.count())
      /*
		     * update community
		     * */


      //计算最优社区
      val updatedVert = LouvainGraph.vertices.join(communityInfo).map{
        case (vid,(v,d))=>{

          val bigMap = d//d.reduceLeft(_ ++ _);


          val gainOfCurrentCommunity=if (bigMap.contains(v.community)) {
            //这种情况下发过来的信息就包含该节点所属社区
            q(v.community, v.community, v.communitySigmaTot, bigMap(v.community)._2, v.adjacentWeight, v.selfWeight, graphWeight)
          } // fixed 2
          else {
            //这种情况下当前节点自己属于一个社区
            q(v.community, v.community, v.communitySigmaTot, 0, v.adjacentWeight, v.selfWeight, graphWeight)

          }


          val gainList= bigMap.map{
            case (communityId, (sigmaTot, edgeWeight))=>{
              val gain = q(v.community, communityId, sigmaTot, edgeWeight, v.adjacentWeight, v.selfWeight, graphWeight) //fixed 2
              (communityId,gain)
            }
          }

          val maxGain=gainList.maxBy(_._2)
          val bestCommunity=if( maxGain._2>gainOfCurrentCommunity )
          {
            maxGain._1
          }else{
            v.community
          }


          val r = scala.util.Random
          val newConverge= (v.community == bestCommunity)
          val (newBestCommunity,newChanged)=if (v.community != bestCommunity && r.nextFloat <= probability) {
            (bestCommunity,true)

          } else {
            (v.community,false)
            //v.changed = false
          }

          //VertexInfo(vid=v.vid,community = newBestCommunity,communitySigmaTot=v.communitySigmaTot,selfWeight = v.selfWeight,adjacentWeight = v.adjacentWeight,changed=newChanged,converge=newConverge)
          (vid,(newBestCommunity,newChanged,newConverge))
        }
      }.cache()

      updatedVert.count()


      sigmaTot.unpersist(blocking = false)
      communitiesSigmaTot.unpersist(blocking = false)
      vertice_sigmaTot.unpersist(blocking = false)


      val prevG=LouvainGraph

      LouvainGraph=LouvainGraph.outerJoinVertices(updatedVert){
        (vid,v,option)=>{

          val (newBestCommunity,newChanged,newConverge)=option.getOrElse((v.community,v.changed,v.converge))


          VertexInfo(vid=v.vid,community = newBestCommunity,communitySigmaTot=v.communitySigmaTot,selfWeight = v.selfWeight,adjacentWeight = v.adjacentWeight,changed=newChanged,converge=newConverge)

        }
      }
      LouvainGraph.cache()



      prevG.vertices.unpersist(blocking = false)//.vertices.unpersist(blocking = false)
      prevG.edges.unpersist(blocking = false)
      updatedVert.unpersist(blocking = false)
      if(preMessages!=null)preMessages.unpersist(blocking = false)




      val conv = LouvainGraph.vertices.values.map(v => if (v.converge) { 0 } else { 1 }).reduce(_ + _)
      if (conv == 0) { converge = true }
      else {
        converge = false
      }
      counter = counter + 1

      println("轮次"+counter+"活跃节点个数"+conv)

    } while (!converge&&counter<numberOfIterations)


    LouvainGraph
  }




  private def exchangeMsg(et: EdgeContext[VertexInfo, Double, Map[Long, (Double, Double)]] /*[VertexInfo, Double]*/) = {

    et.sendToDst(Map(et.srcAttr.community -> (et.srcAttr.communitySigmaTot, et.attr)))
    et.sendToSrc(Map(et.dstAttr.community -> (et.dstAttr.communitySigmaTot, et.attr)))

  }

  private def mergeMsg(m1: Map[Long, (Double, Double)], m2: Map[Long, (Double, Double)]) = {


    val infoMap = scala.collection.mutable.HashMap[Long, (Double, Double)]()
    m1.foreach(f => {
      if (infoMap.contains(f._1)) {
        val w = f._2._2
        val (sig, weight) = infoMap(f._1)
        infoMap(f._1) = (sig, weight + w)
      } else {
        infoMap(f._1) = f._2
      }

    })
    m2.foreach(f => {
      if (infoMap.contains(f._1)) {
        val w = f._2._2
        val (sig, weight) = infoMap(f._1)
        infoMap(f._1) = (sig, weight + w)
      } else {
        infoMap(f._1) = f._2
      }

    })
    infoMap.toMap
  }

  /*Louvain update:
   * Calculate the improvement like this: first remove the node from the current community i and become an isolated community by himself. Then try to add this node to one of the communities(neighboring communities or community i)
   * and fetch the maximum
   * */
  private def q(currCommunityId: Long, joinCommunityId: Long, joinCommunitySigmaTot: Double, edgeWeightInJoinCommunity: Double, adjacentWeight: Double, selfWeight: Double, totalEdgeWeight: Double): Double = {

    var joinOriginalCommunity = currCommunityId == joinCommunityId
    val M = totalEdgeWeight;

    val k_i_in = edgeWeightInJoinCommunity;

    val k_i = adjacentWeight + selfWeight; //self-loop is included in the calculation of k_i

    var sigma_tot = 0.0
    if (joinOriginalCommunity) {
      sigma_tot = joinCommunitySigmaTot - k_i;
    }
    /*if you are calculating gain of modularity for the current community,previously in the calculation
 		* of sigmaTot for the community the edge weight of the current node and his self loop(selfWeight) is
 		* included. Now we are artificially "adding" the node the the original community and therefore the adjacent edges
 		*self loop should not be included in the sigmaTot of the current community
 		* */

    else {
      sigma_tot = joinCommunitySigmaTot
    }

    val deltaQ = k_i_in - (k_i * sigma_tot / M)

    return deltaQ;
  }

}
