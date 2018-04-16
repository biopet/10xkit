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

import scala.collection.mutable

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
    val distanceHistogram = distanceMatrix.value.totalHistogram
    val distanceStats = distanceHistogram.aggregateStats
    val distanceMean = distanceStats("mean").toString.toDouble
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

    val (groups, trash) = reCluster(predictions.flatMap(x => x._2.map(GroupSample(x._1, _))), distanceMatrix, cmdArgs.numClusters, cmdArgs.numIterations, sc.emptyRDD, cmdArgs.outputDir, correctCells, distanceMean)
    sc.clearJobGroup()

    writeGroups(groups.cache(), trash.cache(), cmdArgs.outputDir, correctCells)

    sc.stop()
    logger.info("Done")
  }

  def writeGroups(groups: RDD[GroupSample], trash: RDD[Int], outputDir: File, correctCells: Broadcast[Array[String]]): Unit = {
    val trashData = trash.collect()

    val writer =
      new PrintWriter(new File(outputDir, s"trash.txt"))
    trashData.foreach(s => writer.println(correctCells.value(s)))
    writer.close()

    groups.groupBy(_.group).foreach {
      case (idx, samples) =>
        val writer =
          new PrintWriter(new File(outputDir, s"cluster.$idx.txt"))
        samples.foreach(s => writer.println(correctCells.value(s.sample)))
        writer.close()
    }
  }

  case class Prediction(sample: Int, prediction: Int)
  case class GroupSample(group: Int, sample: Int)
  case class MoveFromCost(group: Int, addCost: Double, removeCost: Double)
  case class MoveToCost(group: Int, addCost: Double)

  def calculateSampleMoveCosts(predictions: RDD[(Int, List[Int])],
                              distanceMatrix: Broadcast[DistanceMatrix])(implicit sc: SparkContext): RDD[(GroupSample, MoveFromCost)] = {
    val groups = sc.broadcast(predictions.collectAsMap())
    val groupDistances = sc.broadcast(groups.value.map(x => x._1 -> distanceMatrix.value.subGroupDistance(x._2)))
    val total = distanceMatrix.value.samples.length
    predictions.flatMap(a => a._2).repartition(total).map { sample =>
      val group = groups.value.find(_._2.contains(sample)).map(_._1).get
      val removeCost: Double = groupDistances.value(group) - distanceMatrix.value.subGroupDistance(sample, groups.value(group).filterNot(_ == sample))
      GroupSample(group, sample) -> groups.value.filter(_._1 != group).map { case (a,b) =>
        val addCost = distanceMatrix.value.subGroupDistance(sample, sample :: b) - groupDistances.value(a)
        MoveFromCost(a, addCost, removeCost)
      }.minBy(_.addCost)
    }
  }

  private val cache: mutable.Map[Int, List[RDD[_]]] = mutable.Map()

  def reCluster(groups: RDD[GroupSample],
                distanceMatrix: Broadcast[DistanceMatrix],
                expectedGroups: Int,
                maxIterations: Int,
                trash: RDD[Int],
                outputDir: File,
                correctCells: Broadcast[Array[String]],
                distanceMean: Double,
                iteration: Int = 1)(implicit sc: SparkContext): (RDD[GroupSample], RDD[Int]) = {
    cache.keys.filter(_ < iteration - 1).foreach(cache(_).foreach(_.unpersist()))
    cache += (iteration - 1) -> (groups.cache() :: cache.getOrElse(iteration - 1, Nil))
    cache += (iteration - 1) -> (trash.cache() :: cache.getOrElse(iteration - 1, Nil))

    {
      val iterationDir = new File(outputDir, s"iteration-${iteration - 1}")
      iterationDir.mkdir()
      writeGroups(groups, trash, iterationDir, correctCells)
    }

    sc.setJobGroup(s"Iteration $iteration", s"Iteration $iteration")

    val groupBy = groups.groupBy(_.group).map(x => x._1 -> x._2.map(_.sample))
    cache += iteration -> (groupBy.cache() :: cache.getOrElse(iteration, Nil))
    val ids = groupBy.keys.collect()
    val numberOfGroups = ids.length

    if (maxIterations - iteration <= 0 && numberOfGroups == expectedGroups) (groups, trash) else {

      val groupDistances = sc.broadcast(groupBy.map { case (idx, samples) =>
        val histogram = distanceMatrix.value.subgroupHistograms(samples.toList, samples.toList)
        idx -> histogram.totalDistance / samples.size
      }.collectAsMap().toMap)

      val avgDistance = groupDistances.value.values.sum / groupDistances.value.size
      if (groupDistances.value.values.exists(_ >= (avgDistance * 2))) {
        val bla = groupBy.flatMap { case (group, samples) =>
          if (groupDistances.value(group) >= (avgDistance * 2)) {
            splitCluster(samples.toList, distanceMatrix)
          } else {
            List(samples.toList)
          }
        }.collect().zipWithIndex.flatMap(x => x._1.map(GroupSample(x._2, _)))
        reCluster(sc.parallelize(bla), distanceMatrix, expectedGroups, maxIterations, trash, outputDir, correctCells, distanceMean, iteration + 1)
      } else {
        if (numberOfGroups > expectedGroups) {
          val removecosts = calculateSampleMoveCosts(groupBy.map(x => x._1 -> x._2.toList), distanceMatrix)
          cache += iteration -> (removecosts.cache() :: cache.getOrElse(iteration, Nil))

          val removeGroup = removecosts.groupBy(_._1.group).map(x => x._1 -> x._2.map(_._2.addCost).sum).collect().minBy(_._2)._1
          reCluster(removecosts.map { case (current, moveTo) =>
            if (current.group == removeGroup) {
              GroupSample(moveTo.group, current.sample)
            } else current
          }, distanceMatrix, expectedGroups, maxIterations, trash, outputDir, correctCells, distanceMean, iteration + 1)
        } else {
          //FIXME: method does not work correctly yet
          val newGroups = divedeTrash(groups, trash, distanceMatrix, groupDistances)
          val removecosts = calculateSampleMoveCosts(newGroups.groupBy(_.group).map(x => x._1 -> x._2.map(_.sample).toList), distanceMatrix)
          cache += iteration -> (removecosts.cache() :: cache.getOrElse(iteration, Nil))

          val newTrash = removecosts.filter { case (current, moveCost) => moveCost.removeCost > moveCost.addCost}.map(_._1.sample)
          val newGroups2 = removecosts.flatMap { case (current, moveCost) =>
            if (moveCost.removeCost > moveCost.addCost) None
            else Some(current)
          }
          reCluster(newGroups2, distanceMatrix, expectedGroups, maxIterations, newTrash, outputDir, correctCells, distanceMean, iteration + 1)
        }
      }
    }
  }

  def calculateNewCost(sample: Int, groups: Map[Int, List[Int]], distanceMatrix: DistanceMatrix): Map[Int, Double] = {
    groups.map{ case (group, list) => group -> distanceMatrix.subGroupDistance(sample, sample :: list)}
  }

  def divedeTrash(groups: RDD[GroupSample],
                  trash: RDD[Int],
                  distanceMatrix: Broadcast[DistanceMatrix],
                  groupDistances: Broadcast[Map[Int, Double]])(implicit sc: SparkContext): RDD[GroupSample] = {
    val groupsBroadcast = sc.broadcast(groups.groupBy(_.group).map(x => x._1 -> x._2.map(_.sample).toList).collectAsMap().toMap)
    trash.map { s =>
      val newCosts = calculateNewCost(s, groupsBroadcast.value, distanceMatrix.value)
      val diff = for ((key, value) <- groupDistances.value) yield MoveToCost(key, newCosts(key) - value)
      GroupSample(diff.minBy(_.addCost).group, s)
    } ++ groups
  }

  def splitCluster(group: List[Int],
                   distanceMatrix: Broadcast[DistanceMatrix]): List[List[Int]] = {
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
