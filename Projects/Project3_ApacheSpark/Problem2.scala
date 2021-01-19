import java.util._
import scala.List
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object Problem2 {
  //given a point, get its corresponding cellId
  def getCellId(point: String, gridSize: Int, numOfGrids: Int): Int ={
    val coordinates: Array[String] = point.split(",")
    val x = Integer.valueOf(coordinates(0))
    val y = Integer.valueOf(coordinates(1))
    val cellId = (((y / gridSize)+1) * numOfGrids) + (x / gridSize)
    cellId
  }
  //get list of all neighboring cells to a given cell
  def getNeighboringCells(cellId: Int, numOfGrids: Int): List[Int] ={
    var neighCellsBuffer = ListBuffer[Int]()
    val xCell = cellId % numOfGrids
    val leftN = cellId-1
    val rightN = cellId+1
    val topN = cellId+numOfGrids
    val bottomN = cellId-numOfGrids
    val bottomLeftN = bottomN-1
    val bottomRightN = bottomN+1
    val topLeftN = topN-1
    val topRightN = topN+1
    //for non boundary points
    if ((cellId > numOfGrids) && (cellId <= (numOfGrids * (numOfGrids - 1)))){
      if(xCell==1){
        neighCellsBuffer += (rightN, topN, bottomN, topRightN, bottomRightN)
      }
      else if(xCell>1){
        neighCellsBuffer += (rightN, leftN, topN, bottomN, topRightN, topLeftN, bottomRightN, bottomLeftN)
      }
      else{
        neighCellsBuffer += (leftN, topN, bottomN, topLeftN, bottomLeftN)
      }
    }
      //bottom-most grid list
    else if(cellId <= numOfGrids){
      if(xCell==1){
        neighCellsBuffer += (rightN, topN, topRightN)
      }
      else if(xCell>1){
        neighCellsBuffer += (rightN, leftN, topN, topRightN, topLeftN)
      }
      else{
        neighCellsBuffer += (leftN, topN, topLeftN)
      }
    }
      //top-most grid list
    else{
      if(xCell==1){
        neighCellsBuffer += (rightN, bottomN, bottomRightN)
      }
      else if(xCell>1){
        neighCellsBuffer += (rightN, leftN, bottomN, bottomRightN, bottomLeftN)
      }
      else{
        neighCellsBuffer += (leftN, bottomN, bottomLeftN)
      }
    }
    val neighCells = neighCellsBuffer.toList
    neighCells
  }

  def getCellDensity(currentCell: Int, neighbourCells: List[Int], lookupMap: scala.collection.Map[Int,Int]): Float={
    val xCount = lookupMap.getOrElse(currentCell,0)
    var density: Float = 0
    val numOfNeigh = neighbourCells.length
    var yAvg: Float = 0
    if(numOfNeigh>0){
      var allNeighPoint: Float = 0
      for(i <- 0 until numOfNeigh){
        val yCount = lookupMap.getOrElse(neighbourCells(i), 0)
        allNeighPoint = allNeighPoint + yCount
      }
      yAvg = allNeighPoint/numOfNeigh
    }
    if(yAvg!=0){
      density = xCount/yAvg
    }
    density
  }
  //get all neighbouring points' individual relative density index
  def getNeighborDensity(currentCell: Int, neighbourRDDLookup: scala.collection.Map[Int,List[Int]], densityLookupMap: scala.collection.Map[Int,Float]): String={
    var neighbourDensity: String = ""
    val neighbors = neighbourRDDLookup.getOrElse(currentCell,null)
    if(neighbors!=null){
      for(i <- 0 until neighbors.length){
        val density = densityLookupMap.getOrElse(neighbors(i),0)
        if(i==0) {
          neighbourDensity = "Neighbors=> "+String.valueOf(neighbors(i)) + ":" + String.valueOf(density)
        }
        else{
          neighbourDensity = neighbourDensity + ", " + String.valueOf(neighbors(i)) + ":" + String.valueOf(density)
        }
      }
    }

    neighbourDensity
  }
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Cell Density Calculation")
    val sc = new SparkContext(conf)

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val pointFile = sc.textFile(args(0))

    //Convert it to RDD of cellId and total points in that cell
    val pointsPairRDD = pointFile.map(line => (getCellId(line,20,500),1)).reduceByKey(_+_)
    //RDD of cellId with all its Neighbors
    val neighborRDD = pointsPairRDD.map(cell => (cell._1,getNeighboringCells(cell._1,500)))
    //Point RDD to map collection
    val pointCountLookupMap = pointsPairRDD.collectAsMap()
    val cellDensityPairRDD = neighborRDD.map(line => (line._1, getCellDensity(line._1, line._2, pointCountLookupMap)))
    //top 50 dense cell RDD
    val highDensityCells_top50_RDD = sc.parallelize(cellDensityPairRDD.sortBy(_._2,false).take(50))
    highDensityCells_top50_RDD.saveAsTextFile(args(1))

    val cellDensityLookupMap = cellDensityPairRDD.collectAsMap()
    val neighbourRDDLookup = neighborRDD.collectAsMap()

    //top 50 highly dense cells with their Relative Density Index along with list of their neighbors and their Relative Density
    val neighborDensityRDD = highDensityCells_top50_RDD.map(line => (line._1+":"+line._2, getNeighborDensity(line._1,neighbourRDDLookup,cellDensityLookupMap)))
    neighborDensityRDD.saveAsTextFile(args(2))
  }

}