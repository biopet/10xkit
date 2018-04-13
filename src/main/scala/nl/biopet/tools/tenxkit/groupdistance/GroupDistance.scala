package nl.biopet.tools.tenxkit.groupdistance

import java.io.{File, PrintWriter}

import nl.biopet.tools.tenxkit
import nl.biopet.tools.tenxkit.{DistanceMatrix, VariantCall}
import nl.biopet.utils.tool.ToolCommand
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.clustering._
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object GroupDistance extends ToolCommand[Args] {
  def emptyArgs = Args()
  def argsParser = new ArgsParser(this)

  def main(args: Array[String]): Unit = {
    val cmdArgs = cmdArrayToArgs(args)

    logger.info("Start")

    val sparkConf: SparkConf =
      new SparkConf(true).setMaster(cmdArgs.sparkMaster)
    implicit val sparkSession: SparkSession =
      SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    implicit val sc: SparkContext = sparkSession.sparkContext
    logger.info(
      s"Context is up, see ${sparkSession.sparkContext.uiWebUrl.getOrElse("")}")

    logger.info("Reading input data")
    val distanceMatrix = sc.broadcast(DistanceMatrix.fromFile(cmdArgs.distanceMatrix))
    val correctCells = tenxkit.parseCorrectCells(cmdArgs.correctCells)
    val correctCellsMap = tenxkit.correctCellsMap(correctCells)
    val vectors = (if (cmdArgs.inputFile.isDirectory) {
                     variantsToVectors(VariantCall
                                         .fromPartitionedVcf(cmdArgs.inputFile,
                                                             cmdArgs.reference,
                                                             correctCellsMap),
                                       correctCells)
                   } else if (cmdArgs.inputFile.getName.endsWith(".vcf.gz")) {
                     variantsToVectors(VariantCall
                                         .fromVcfFile(cmdArgs.inputFile,
                                                      cmdArgs.reference,
                                                      correctCellsMap,
                                                      50000000),
                                       correctCells)
                   } else {
                     distanceMatrixToVectors(distanceMatrix.value, correctCells)
                   }).toDF("sample", "features").cache()


    val bkm = new BisectingKMeans()
      .setK(cmdArgs.numClusters)
      //.setMaxIter(cmdArgs.numIterations)
      .setSeed(cmdArgs.seed)

    val model = bkm.fit(vectors)

    val predictions: RDD[(Int, Iterable[Int])] = model
      .transform(vectors)
      .select("sample", "prediction")
      .as[Prediction]
      .rdd
      .groupBy(_.prediction)
      .repartition(cmdArgs.numClusters)
      .map(x => x._1 -> x._2.map(s => s.sample))
      .cache()

    val distances = predictions.map { case (idx, samples) =>
      val histogram = distanceMatrix.value.subgroupHistograms(samples.toList, samples.toList)
      idx -> histogram.totalDistance / samples.size
    }.collectAsMap()

    val map = predictions.collectAsMap()
    val split = splitCluster(map(1).toList, distanceMatrix)
    val newBla = split ::: map.filter(_._1 != 1).values.toList
    val map2 = newBla.zipWithIndex.map {
      case (samples, idx) =>
        val histogram = distanceMatrix.value.subgroupHistograms(samples.toList, samples.toList)
        idx -> histogram.totalDistance / samples.size
    }.toMap

    newBla.zipWithIndex.foreach {
      case (samples, idx) =>
        val writer =
          new PrintWriter(new File(cmdArgs.outputDir, s"cluster.$idx.txt"))
        samples.foreach(s => writer.println(correctCells.value(s)))
        writer.close()
    }

    val removecosts = calculateSampleMoveCosts(sc.parallelize(newBla.map(_.toList)), distanceMatrix)
    val removeCosts = removecosts.groupBy(_._1.group).map(x => x._1 -> x._2.map(_._2.addCost).sum)
    val removeGroup = removeCosts.collect().minBy(_._2)._1
    removecosts.map { case (current, moveTo) =>
      if (current.group == removeGroup) {
        GroupSample(moveTo.group, current.sample)
      } else current
    }

    sc.stop()
    logger.info("Done")
  }

  case class Prediction(sample: Int, prediction: Int)
  case class GroupSample(group: Int, sample: Int)
  case class MoveToCost(group: Int, addCost: Double, removeCost: Double)

  def calculateSampleMoveCosts(predictions: RDD[List[Int]],
                              distanceMatrix: Broadcast[DistanceMatrix])(implicit sc: SparkContext): RDD[(GroupSample, MoveToCost)] = {
    val groups = sc.broadcast(predictions.collect().zipWithIndex.map(_.swap).toMap)
    val groupDistances = sc.broadcast(groups.value.map(x => x._1 -> distanceMatrix.value.subGroupDistance(x._2)))
    val total = distanceMatrix.value.samples.length
    predictions.flatMap(a => a).repartition(total).map { sample =>
      val group = groups.value.find(_._2.contains(sample)).map(_._1).get
      val removeCost: Double = groupDistances.value(group) - distanceMatrix.value.subGroupDistance(groups.value(group).filterNot(_ == sample))
      GroupSample(group, sample) -> groups.value.filter(_._1 != group).map { case (a,b) =>
        val addCost = distanceMatrix.value.subGroupDistance(sample :: b) - groupDistances.value(a)
        MoveToCost(a, addCost, removeCost)
      }.minBy(_.addCost)
    }
  }


  def reCluster(predictions: RDD[List[Int]],
                distanceMatrix: Broadcast[DistanceMatrix])(implicit sc: SparkContext): RDD[List[Int]] = {
    predictions
  }

  def splitCluster(group: List[Int],
                   distanceMatrix: Broadcast[DistanceMatrix])(implicit sc: SparkContext): List[List[Int]] = {
    val sampleSplit = group.map { s1 =>
      val maxDistance = group.flatMap(s2 => distanceMatrix.value(s1, s2).map(s2 -> _)).maxBy(_._2)
      s1 -> maxDistance
    }
    val max = sampleSplit.maxBy(_._2._2)

    def grouping(samples: List[Int], g1: List[Int], g2: List[Int]): List[List[Int]] = {
      if (samples.nonEmpty) {
        val distances1 = samples.map(s1 => s1 -> g1.flatMap(s2 => distanceMatrix.value(s1, s2)).sum / g1.size)
        val distances2 = samples.map(s1 => s1 -> g2.flatMap(s2 => distanceMatrix.value(s1, s2)).sum / g2.size)
        val sample = distances1.zip(distances2).map(x => (x._1._2 - x._2._2).abs).zipWithIndex.maxBy(_._1)._2
        if (distances1(sample)._2 > distances2(sample)._2) {
          grouping(samples.filter(_ != samples(sample)), g1, samples(sample) :: g2)
        } else {
          grouping(samples.filter(_ != samples(sample)), samples(sample) :: g1, g2)
        }
      } else List(g1, g2)
    }

    grouping(group.filter(_ != max._1).filter(_ != max._2._1), max._1 :: Nil, max._2._1 :: Nil)
  }

  def variantsToVectors(
      variants: RDD[VariantCall],
      correctCells: Broadcast[Array[String]]): RDD[(Int, linalg.Vector)] = {
    variants
      .flatMap { v =>
        val alleles = 0 :: v.altAlleles.indices.map(_ + 1).toList
        correctCells.value.indices.map { sample =>
          val sa = v.samples.get(sample) match {
            case Some(a) =>
              val total = a.map(_.total).sum
              alleles.map(a(_).total.toDouble / total)
            case _ => alleles.map(_ => 0.0)
          }
          sample -> (v.contig, v.pos, sa)
        }
      }
      .groupByKey(correctCells.value.length)
      .map {
        case (sample, list) =>
          val sorted = list.toList.sortBy(y => (y._1, y._2))
          (sample, Vectors.dense(sorted.flatMap(_._3).toArray))
      }
  }

  def distanceMatrixToVectors(matrix: DistanceMatrix,
                              correctSamples: Broadcast[Array[String]])(
      implicit sc: SparkContext): RDD[(Int, linalg.Vector)] = {
    require(matrix.samples sameElements correctSamples.value)
    val samples = matrix.samples.indices.toList
    val samplesFiltered = samples.filter(s1 =>
      samples.map(s2 => matrix(s1, s2)).count(_.isDefined) >= 1000)
    logger.info(s"Removed ${samples.size - samplesFiltered.size} samples")
    val vectors = samplesFiltered.map(
      s1 =>
        s1 -> Vectors.dense(
          samplesFiltered.map(s2 => matrix(s1, s2).getOrElse(0.0)).toArray))
    sc.parallelize(vectors)//, vectors.size)
  }

  def descriptionText: String =
    """
      |
    """.stripMargin

  def manualText: String =
    """
      |
    """.stripMargin

  def exampleText: String =
    """
      |
    """.stripMargin

}
