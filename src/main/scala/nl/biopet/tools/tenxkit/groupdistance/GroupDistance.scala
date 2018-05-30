/*
 * Copyright (c) 2018 Biopet
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package nl.biopet.tools.tenxkit.groupdistance

import java.io.{File, PrintWriter}

import nl.biopet.tools.tenxkit
import nl.biopet.tools.tenxkit.{DistanceMatrix, TenxKit}
import nl.biopet.utils.tool.ToolCommand
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.clustering._
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

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
    implicit val sc: SparkContext = sparkSession.sparkContext
    logger.info(
      s"Context is up, see ${sparkSession.sparkContext.uiWebUrl.getOrElse("")}")

    logger.info("Reading input data")
    val distanceMatrix =
      sc.broadcast(
        DistanceMatrix
          .fromFileSpark(cmdArgs.distanceMatrix, cmdArgs.countMatrix)
          .collect()(0))
    val correctCells = tenxkit.parseCorrectCells(cmdArgs.correctCells)

    val result = totalRun(distanceMatrix,
                          cmdArgs.outputDir,
                          cmdArgs.numClusters,
                          cmdArgs.numIterations,
                          cmdArgs.seed,
                          correctCells,
                          cmdArgs.skipKmeans)

    Await.result(result.writeFuture, Duration.Inf)

    sparkSession.stop()
    logger.info("Done")
  }

  case class Result(groups: RDD[GroupSample],
                    trash: RDD[Int],
                    writeFuture: Future[_])

  def totalRun(distanceMatrix: Broadcast[DistanceMatrix],
               outputDir: File,
               numClusters: Int,
               numIterations: Int,
               seed: Long,
               correctCells: Broadcast[IndexedSeq[String]],
               skipKmeans: Boolean = false)(
      implicit sc: SparkContext,
      sparkSession: SparkSession): Result = {
    val (initGroups, trashInit) =
      initGroup(distanceMatrix, numClusters, seed, correctCells, skipKmeans)
    initGroups.cache()

    val (groups, trash) = reCluster(
      initGroups,
      distanceMatrix,
      numClusters,
      numIterations,
      trashInit,
      outputDir,
      correctCells
    )
    sc.clearJobGroup()

    val writeFuture = Future(
      writeGroups(groups.cache(), trash.cache(), outputDir, correctCells))

    Result(groups, trash, writeFuture)
  }

  def initGroup(distanceMatrix: Broadcast[DistanceMatrix],
                numClusters: Int,
                seed: Long,
                correctCells: Broadcast[IndexedSeq[String]],
                skipKmeans: Boolean = false)(
      implicit sc: SparkContext,
      sparkSession: SparkSession): (RDD[GroupSample], RDD[Int]) = {
    import sparkSession.implicits._
    if (skipKmeans) {
      (sc.parallelize(correctCells.value.indices.map(i => GroupSample(1, i)),
                      correctCells.value.length),
       sc.emptyRDD)
    } else {
      val (vectorsRdd, trash) =
        distanceMatrixToVectors(distanceMatrix.value, correctCells)
      val vectors = vectorsRdd.toDF("sample", "features").cache()

      val bkm = new BisectingKMeans()
        .setK(numClusters)
        //.setMaxIter(cmdArgs.numIterations)
        .setSeed(seed)

      val model = bkm.fit(vectors)

      // Predict cluster for each cell
      (model
         .transform(vectors)
         .select("sample", "prediction")
         .as[Prediction]
         .rdd
         .groupBy(_.prediction)
         .repartition(numClusters)
         .map { case (group, list) => group -> list.map(_.sample) }
         .flatMap { case (g, l) => l.map(GroupSample(g, _)) },
       trash)
    }
  }

  def writeGroups(groups: RDD[GroupSample],
                  trash: RDD[Int],
                  outputDir: File,
                  correctCells: Broadcast[IndexedSeq[String]]): Unit = {
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
                               distanceMatrix: Broadcast[DistanceMatrix])(
      implicit sc: SparkContext): RDD[(GroupSample, MoveFromCost)] = {
    val groups = sc.broadcast(predictions.collectAsMap())
    val total = distanceMatrix.value.samples.length
    predictions.flatMap { case (_, l) => l }.repartition(total).map { sample =>
      val group = groups.value
        .find { case (_, v) => v.contains(sample) }
        .map { case (k, _) => k }
        .getOrElse(0)
      val removeCost: Double = distanceMatrix.value
        .subGroupDistance(sample, groups.value(group).filterNot(_ == sample))
      GroupSample(group, sample) -> groups.value
        .filter { case (g, _) => g != group }
        .map {
          case (a, b) =>
            val addCost =
              distanceMatrix.value.subGroupDistance(sample, sample :: b)
            MoveFromCost(a, addCost, removeCost)
        }
        .minBy(_.addCost)
    }
  }

  private val cache: mutable.Map[Int, List[RDD[_]]] = mutable.Map()

  def sameGroups(groups1: RDD[GroupSample],
                 groups2: RDD[GroupSample]): Future[Boolean] = {
    val c1 = groups1.collectAsync().map(_.toSet)
    val c2 = groups2.collectAsync().map(_.toSet)
    c1.zip(c2).map { case (r1, r2) => r1 == r2 }
  }

  def sameTrash(trash1: RDD[Int], trash2: RDD[Int]): Future[Boolean] = {
    val c1 = trash1.collectAsync().map(_.toSet)
    val c2 = trash2.collectAsync().map(_.toSet)
    c1.zip(c2).map { case (r1, r2) => r1 == r2 }
  }

  def reCluster(groups: RDD[GroupSample],
                distanceMatrix: Broadcast[DistanceMatrix],
                expectedGroups: Int,
                maxIterations: Int,
                trash: RDD[Int],
                outputDir: File,
                correctCells: Broadcast[IndexedSeq[String]],
                iteration: Int = 1)(
      implicit sc: SparkContext): (RDD[GroupSample], RDD[Int]) = {
    cache.keys
      .filter(_ < iteration - 1)
      .foreach(cache(_)
        .foreach { x =>
          try {
            if (x.getStorageLevel != StorageLevel.NONE)
              x.unpersist()
          } catch {
            case _: NullPointerException =>
          }

        })
    cache += (iteration - 1) -> (groups
      .cache() :: cache.getOrElse(iteration - 1, Nil))
    cache += (iteration - 1) -> (trash.cache() :: cache.getOrElse(iteration - 1,
                                                                  Nil))

    val groupBy =
      groups.groupBy(_.group).map { case (g, list) => g -> list.map(_.sample) }
    cache += iteration -> (groupBy.cache() :: cache.getOrElse(iteration, Nil))

    val groupDistances = sc.broadcast(
      groupBy
        .map {
          case (idx, samples) =>
            val histogram =
              distanceMatrix.value.subgroupHistograms(samples.toList,
                                                      samples.toList)
            idx -> histogram.totalDistance / samples.size
        }
        .collectAsMap()
        .toMap)

    {
      val iterationDir = new File(outputDir, s"iteration-${iteration - 1}")
      iterationDir.mkdir()
      writeGroups(groups, trash, iterationDir, correctCells)
    }

    sc.setJobGroup(s"Iteration $iteration", s"Iteration $iteration")

    val ids = groupBy.keys.collect()
    val numberOfGroups = ids.length

    if (maxIterations - iteration <= 0 && numberOfGroups == expectedGroups)
      (groups, trash)
    else if (numberOfGroups == 0) {
      reCluster(trash.map(GroupSample(0, _)),
                distanceMatrix,
                expectedGroups,
                maxIterations,
                sc.emptyRDD,
                outputDir,
                correctCells,
                iteration)
    } else {
      val avgDistance = groupDistances.value.values.sum / groupDistances.value.size
      if (numberOfGroups > expectedGroups) {
        val removecosts = calculateSampleMoveCosts(groupBy.map {
          case (g, l) => g -> l.toList
        }, distanceMatrix)
        cache += iteration -> (removecosts
          .cache() :: cache.getOrElse(iteration, Nil))

        val (removeGroup, _) = removecosts
          .groupBy { case (x, _) => x.group }
          .map { case (g, l) => g -> l.map { case (_, x) => x.addCost }.sum }
          .collect()
          .minBy { case (_, x) => x }
        reCluster(
          removecosts.map {
            case (current, moveTo) =>
              if (current.group == removeGroup) {
                GroupSample(moveTo.group, current.sample)
              } else current
          },
          distanceMatrix,
          expectedGroups,
          maxIterations,
          trash,
          outputDir,
          correctCells,
          iteration + 1
        )
      } else if (numberOfGroups < expectedGroups || groupDistances.value.values
                   .exists(_ >= (avgDistance * 2))) {
        // Split groups where the distance to big
        val (splitRdd, extraTrash) = splitCluster(groups,
                                                  ids,
                                                  distanceMatrix,
                                                  groupDistances,
                                                  avgDistance,
                                                  expectedGroups)
        cache += iteration -> (splitRdd
          .cache() :: cache.getOrElse(iteration, Nil))
        reCluster(splitRdd,
                  distanceMatrix,
                  expectedGroups,
                  maxIterations,
                  trash.union(extraTrash),
                  outputDir,
                  correctCells,
                  iteration + 1)
      } else {
        val newGroups =
          divedeTrash(groups, trash, distanceMatrix, groupDistances)
        val removecosts = calculateSampleMoveCosts(
          newGroups
            .groupBy(_.group)
            .map { case (g, l) => g -> l.map(_.sample).toList },
          distanceMatrix)
        cache += iteration -> (removecosts
          .cache() :: cache.getOrElse(iteration, Nil))

        val newTrash = removecosts
          .filter {
            case (current, moveCost) => moveCost.removeCost > moveCost.addCost
          }
          .keys
          .map(_.sample)
        val newGroups2 = removecosts.flatMap {
          case (current, moveCost) =>
            if (moveCost.removeCost > moveCost.addCost) None
            else Some(current)
        }
        cache += iteration -> (newGroups2
          .cache() :: cache.getOrElse(iteration, Nil))
        cache += iteration -> (newTrash
          .cache() :: cache.getOrElse(iteration, Nil))

        val same =
          sameGroups(groups, newGroups2).zip(sameTrash(trash, newTrash)).map {
            case (g, t) => g && t
          }
        if (Await.result(same, Duration.Inf))
          (newGroups2, newTrash)
        else
          reCluster(newGroups2,
                    distanceMatrix,
                    expectedGroups,
                    maxIterations,
                    newTrash,
                    outputDir,
                    correctCells,
                    iteration + 1)
      }
    }
  }

  def calculateNewCost(sample: Int,
                       groups: Map[Int, List[Int]],
                       distanceMatrix: DistanceMatrix): Map[Int, Double] = {
    groups.map {
      case (group, list) =>
        group -> distanceMatrix.subGroupDistance(sample, sample :: list)
    }
  }

  def divedeTrash(groups: RDD[GroupSample],
                  trash: RDD[Int],
                  distanceMatrix: Broadcast[DistanceMatrix],
                  groupDistances: Broadcast[Map[Int, Double]])(
      implicit sc: SparkContext): RDD[GroupSample] = {
    val groupsBroadcast = sc.broadcast(
      groups
        .groupBy(_.group)
        .map { case (g, l) => g -> l.map(_.sample).toList }
        .collectAsMap()
        .toMap)
    trash.map { s =>
      val newCosts =
        calculateNewCost(s, groupsBroadcast.value, distanceMatrix.value)
      val diff = for ((key, value) <- groupDistances.value)
        yield MoveToCost(key, newCosts(key) - value)
      GroupSample(diff.minBy(_.addCost).group, s)
    } ++ groups
  }

  private def splitGrouping(
      samples: List[Int],
      g1: List[Int],
      g2: List[Int],
      distanceMatrix: Broadcast[DistanceMatrix],
      distances1: mutable.Map[Int, Double] = mutable.Map(),
      distances2: mutable.Map[Int, Double] = mutable.Map())
    : (List[Int], List[Int]) = {
    require(g1.nonEmpty && g2.nonEmpty)
    if (distances1.isEmpty) {
      samples.foreach(
        s1 =>
          distances1 += s1 -> g1
            .flatMap(s2 => distanceMatrix.value(s1, s2))
            .sum)
    }
    if (distances2.isEmpty) {
      samples.foreach(
        s1 =>
          distances2 += s1 -> g2
            .flatMap(s2 => distanceMatrix.value(s1, s2))
            .sum)
    }
    if (samples.nonEmpty) {
      val (sample, distance) = distances1
        .zip(distances2)
        .map {
          case ((s, v1), (_, v2)) =>
            s -> ((v1 / g1.length) - (v2 / g2.length)).abs
        }
        .maxBy { case (_, x) => x }
      val d1 = distances1
        .filter { case (x, _) => x == sample }
        .map { case (_, x) => x }
        .headOption
        .getOrElse(0.0) / g1.length
      val d2 = distances2
        .filter { case (x, _) => x == sample }
        .map { case (_, x) => x }
        .headOption
        .getOrElse(0.0) / g2.length
      distances1 -= sample
      distances2 -= sample
      val leftoverSamples = samples.filter(_ != sample)
      if (d1 > d2) {
        distances2.foreach {
          case (s, d) =>
            distances2 += s -> (d + distanceMatrix
              .value(sample, s)
              .getOrElse(0.0))
        }
        splitGrouping(leftoverSamples,
                      g1,
                      sample :: g2,
                      distanceMatrix,
                      distances1,
                      distances2)
      } else {
        distances1.foreach {
          case (s, d) =>
            distances1 += s -> (d + distanceMatrix
              .value(sample, s)
              .getOrElse(0.0))
        }
        splitGrouping(leftoverSamples,
                      sample :: g1,
                      g2,
                      distanceMatrix,
                      distances1,
                      distances2)
      }
    } else (g1, g2)
  }

  def splitCluster(samples: RDD[GroupSample],
                   groupIds: Array[Int],
                   distanceMatrix: Broadcast[DistanceMatrix],
                   groupDistances: Broadcast[Map[Int, Double]],
                   avgDistance: Double,
                   expectedGroups: Int)(
      implicit sc: SparkContext): (RDD[GroupSample], RDD[Int]) = {
    val splitGroupId = groupIds.maxBy(
      group =>
        groupDistances
          .value(group) == groupDistances.value.values.max && (groupDistances.value(
          group) >= (avgDistance * 2) || (groupIds.length < expectedGroups)))
    val newGroupId = groupIds.max + 1

    val restGroups = samples.filter(_.group != splitGroupId)

    val splitGroup = samples.filter(_.group == splitGroupId)
    val splitSamples = sc.broadcast(splitGroup.map(_.sample).collect())

    val total = splitGroup
      .map { s1 =>
        val distances = splitSamples.value
          .flatMap(s2 => distanceMatrix.value(s1.sample, s2).map(s2 -> _))
        s1.sample -> (if (distances.nonEmpty)
                        Some(distances.maxBy { case (_, x) => x })
                      else None)
      }
      .collect()
    val sorted = total
      .flatMap { case (s1, s2) => s2.map(s1 -> _) }
      .sortBy { case (_, (_, x)) => x }
      .reverse

    sorted.headOption match {
      case Some((s1, (s2, _))) =>
        (splitGroup
           .repartition(1)
           .mapPartitions { it =>
             val splitSamples = it.map(_.sample).toList
             val (g1, g2) =
               splitGrouping(splitSamples.filter(s => s != s1 && s != s2),
                             s1 :: Nil,
                             s2 :: Nil,
                             distanceMatrix)
             (g1.map(GroupSample(splitGroupId, _)) ::: g2.map(
               GroupSample(newGroupId, _))).iterator
           }
           .union(restGroups),
         sc.emptyRDD[Int])
      case _ => (restGroups, splitGroup.map(_.sample))
    }
  }

  def distanceMatrixToVectors(matrix: DistanceMatrix,
                              correctSamples: Broadcast[IndexedSeq[String]])(
      implicit sc: SparkContext): (RDD[(Int, linalg.Vector)], RDD[Int]) = {
    require(matrix.samples == correctSamples.value)
    val samples = matrix.samples.indices.toList
    val totalSamples = samples.length
    val samplesFiltered = samples.filter(
      s1 =>
        samples
          .map(s2 => matrix(s1, s2))
          .count(_.isDefined)
          .toDouble / totalSamples >= 0.25)
    val trash = samples.diff(samplesFiltered)
    logger.info(s"Removed ${samples.size - samplesFiltered.size} samples")
    val vectors = samplesFiltered.map(
      s1 =>
        s1 -> Vectors.dense(
          samplesFiltered.map(s2 => matrix(s1, s2).getOrElse(0.0)).toArray))
    (sc.parallelize(vectors), sc.parallelize(trash))
  }

  def descriptionText: String =
    """
      |This tool we try to group distances together. The result should be a clusters of 1 single sample.
      |
      |This tool will execute multiple iterations to find the groups.
    """.stripMargin

  def manualText: String =
    """
      |This tool will require the distance matrix and the expected number of samples and a list of correct cells.
      |The tool will require a spark cluster up and running.
    """.stripMargin

  def exampleText: String =
    s"""
      |Default run with 5 expected samples:
      |${TenxKit.sparkExample(
         "GroupDistance",
         "--sparkMaster",
         "<spark master>",
         "-d",
         "<distance matrix>",
         "-o",
         "<output directory>",
         "--numClusters",
         "5",
         "--correctCells",
         "<correct cells file>"
       )}
      |
    """.stripMargin

}
