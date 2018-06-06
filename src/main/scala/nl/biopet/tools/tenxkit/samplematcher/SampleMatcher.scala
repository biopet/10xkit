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

package nl.biopet.tools.tenxkit.samplematcher

import java.io.File

import htsjdk.samtools.SAMSequenceDictionary
import nl.biopet.tools.tenxkit
import nl.biopet.tools.tenxkit.{DistanceMatrix, TenxKit, VariantCall}
import nl.biopet.tools.tenxkit.calculatedistance.CalulateDistance
import nl.biopet.tools.tenxkit.cellreads.CellReads
import nl.biopet.tools.tenxkit.evalsubgroups.EvalSubGroups
import nl.biopet.tools.tenxkit.extractgroupvariants.ExtractGroupVariants
import nl.biopet.tools.tenxkit.groupdistance.GroupDistance
import nl.biopet.tools.tenxkit.groupdistance.GroupDistance.GroupSample
import nl.biopet.tools.tenxkit.variantcalls.CellVariantcaller
import nl.biopet.tools.tenxkit.variantcalls.CellVariantcaller.getPartitions
import nl.biopet.utils.tool.ToolCommand
import nl.biopet.utils.ngs.fasta.getCachedDict
import nl.biopet.utils.io.resourceToFile
import nl.biopet.utils.ngs.intervals.BedRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

object SampleMatcher extends ToolCommand[Args] {
  def emptyArgs = Args()
  def argsParser = new ArgsParser(this)

  def main(args: Array[String]): Unit = {
    val cmdArgs = cmdArrayToArgs(args)

    logger.info("Start")

    val resourceFile = File.createTempFile("samplematcher.", ".xml")
    resourceToFile("/nl/biopet/tools/tenxkit/spark-scheduling.xml",
                   resourceFile)
    val sparkConf: SparkConf =
      new SparkConf(true)
        .setMaster(cmdArgs.sparkMaster)
        .set("spark.scheduler.mode", "FAIR")
        .set("spark.scheduler.allocation.file", resourceFile.getAbsolutePath)

    implicit val sparkSession: SparkSession =
      SparkSession.builder().config(sparkConf).getOrCreate()
    implicit val sc: SparkContext = sparkSession.sparkContext
    logger.info(
      s"Context is up, see ${sparkSession.sparkContext.uiWebUrl.getOrElse("")}")

    val correctCells = tenxkit.parseCorrectCells(cmdArgs.correctCellsFile)
    val correctCellsMap = tenxkit.correctCellsMap(correctCells)
    val dict = sc.broadcast(getCachedDict(cmdArgs.reference))

    val regions =
      tenxkit
        .createRegions(
          cmdArgs.inputFile,
          cmdArgs.reference,
          CellVariantcaller.getPartitions(cmdArgs.inputFile,
                                          cmdArgs.partitions,
                                          fileSizePerPartition =
                                            cmdArgs.fileBinSize),
          cmdArgs.intervals
        )
        .sortBy(_._2)

    val futures = new ListBuffer[Future[Any]]()

    val variantsResult =
      runVariant(cmdArgs, regions, correctCells, correctCellsMap, dict)

    val contigs = regions.groupBy(_._1.map(_.chr).head).map(x => x._1 -> x._2.map(_._2).sum).toList.sortBy(_._2).reverse.map(_._1)

    val contigFutures = contigs
      .map { contig =>
        contig -> Future(variantsResult
          .contigs(contig)
          .filteredVariants)
      }
      .toMap

    val calculateDistanceResult =
      runCalculateDistance(cmdArgs, contigFutures, correctCells)
    futures += calculateDistanceResult.totalFuture

    val distanceMatrix =
      calculateDistanceResult.correctedDistancesMatrixRdd.flatMap(
        _.collectAsync()
          .map(x => sc.broadcast(x(0))))

    val groupDistanceResult =
      distanceMatrix.map { x =>
        sc.setJobGroup("Group distance", "Group distance")
        runGroupDistance(cmdArgs, x, correctCells)
      }
    sc.clearJobGroup()
    futures += groupDistanceResult.flatMap(_.writeFuture)

    //TODO: extra jobs

    val vcfHeader = sc.broadcast(tenxkit.vcfHeader(correctCells.value))
    variantsResult.writeFilterVariants(
      new File(cmdArgs.outputDir, "variantcalling"),
      correctCells,
      dict,
      vcfHeader,
      cmdArgs.seqError)
    futures += variantsResult.totalFuture

    val extractGroupVariantsResult =
      groupDistanceResult.zip(variantsResult.sortedFilteredVariants).flatMap {
        case (g, v) =>
          sc.setJobGroup("Extract group variants", "Extract group variants")
          runExtractGroupVariants(cmdArgs, v, g.groups, g.trash, dict)
      }
    futures += extractGroupVariantsResult.flatMap(x =>
      Future.sequence(x.futures))

    futures += groupDistanceResult.zip(distanceMatrix).flatMap {
      case (g, d) =>
        sc.setJobGroup("Eval sub groups", "Eval sub groups")

        Future.sequence(
          runEvalSubGroups(cmdArgs, g.groups, g.trash, correctCells, d))
    }

    sc.clearJobGroup()

    sc.setLocalProperty("spark.scheduler.pool", "low-prio")
    futures += runCellReads(cmdArgs, dict)

    //TODO: Extract bam files

    Await.result(Future.sequence(futures.toList), Duration.Inf)

    sparkSession.stop()
    logger.info("Done")
  }

  def runVariant(cmdArgs: Args,
                 regions: List[(List[BedRecord], Long)],
                 correctCells: Broadcast[IndexedSeq[String]],
                 correctCellsMap: Broadcast[Map[String, Int]],
                 dict: Broadcast[SAMSequenceDictionary])(
      implicit sc: SparkContext): CellVariantcaller.Result = {
    val dir = new File(cmdArgs.outputDir, "variantcalling")
    dir.mkdir()
    CellVariantcaller.totalRun(
      cmdArgs.inputFile,
      dir,
      cmdArgs.reference,
      dict,
      regions,
      cmdArgs.sampleTag,
      Some(cmdArgs.umiTag),
      correctCells,
      correctCellsMap,
      sc.broadcast(cmdArgs.cutoffs),
      cmdArgs.seqError,
      writeFilteredVcf = false
    )
  }

  def runCalculateDistance(cmdArgs: Args,
                           variants: Map[String, Future[RDD[VariantCall]]],
                           correctCells: Broadcast[IndexedSeq[String]])(
      implicit sc: SparkContext,
      sparkSession: SparkSession): CalulateDistance.Result = {
    val dir = new File(cmdArgs.outputDir, "calculatedistance")
    dir.mkdir()
    CalulateDistance.runTotalPerContig(
      variants,
      dir,
      correctCells,
      cmdArgs.cutoffs.minAlleleDepth,
      cmdArgs.method,
      cmdArgs.writeScatters
    )
  }

  def runGroupDistance(cmdArgs: Args,
                       distanceMatrix: Broadcast[DistanceMatrix],
                       correctCells: Broadcast[IndexedSeq[String]])(
      implicit sc: SparkContext,
      sparkSsession: SparkSession): GroupDistance.Result = {
    val dir = new File(cmdArgs.outputDir, "groupdistance")
    dir.mkdir()
    GroupDistance.totalRun(distanceMatrix,
                           dir,
                           cmdArgs.expectedSamples,
                           cmdArgs.numIterations,
                           cmdArgs.seed,
                           correctCells,
                           cmdArgs.skipKmeans)
  }

  def runExtractGroupVariants(cmdArgs: Args,
                              variants: RDD[VariantCall],
                              groups: RDD[GroupSample],
                              trash: RDD[Int],
                              dict: Broadcast[SAMSequenceDictionary])(
      implicit sc: SparkContext): Future[ExtractGroupVariants.Results] = {

    val groupMap =
      (groups.map(g => g.sample -> s"cluster.${g.group}") ++ trash.map(
        _ -> "trash"))
        .collectAsync()
        .map(_.toMap)
        .map(sc.broadcast(_))

    val dir = new File(cmdArgs.outputDir, "extractgroupvariants")
    dir.mkdir()
    groupMap.map(
      ExtractGroupVariants
        .totalRun(variants, _, dir, cmdArgs.cutoffs.minSampleDepth, dict))
  }

  def runEvalSubGroups(cmdArgs: Args,
                       groups: RDD[GroupSample],
                       trash: RDD[Int],
                       correctCells: Broadcast[IndexedSeq[String]],
                       distanceMatrix: Broadcast[DistanceMatrix])(
      implicit sc: SparkContext): List[Future[Unit]] = {
    val evalGroupDir = new File(cmdArgs.outputDir, "evalsubgroups")
    evalGroupDir.mkdir()
    sc.setLocalProperty("spark.scheduler.pool", null)

    val nameGroups = groups
      .groupBy(_.group)
      .map {
        case (group, samples) =>
          s"cluster.$group" -> samples
            .map(g => correctCells.value(g.sample))
            .toList
      }
      .repartition(1)
      .mapPartitions(x => Iterator(x.toMap))
    val distanceEval = nameGroups.foreachAsync(
      EvalSubGroups.evalDistanceMatrix(distanceMatrix.value, evalGroupDir, _))

    val knownTrue =
      if (cmdArgs.knownTrue.nonEmpty)
        Some(
          nameGroups.foreachAsync(EvalSubGroups
            .calculateRecallPrecision(cmdArgs.knownTrue, evalGroupDir, _)))
      else None

    distanceEval :: knownTrue.toList
  }

  def runCellReads(cmdArgs: Args, dict: Broadcast[SAMSequenceDictionary])(
      implicit sc: SparkContext): Future[Unit] = {
    val dir = new File(cmdArgs.outputDir, "cellreads")
    dir.mkdir()
    sc.setLocalProperty("spark.scheduler.pool", "low-prio")

    CellReads.runTotal(cmdArgs.inputFile,
                       cmdArgs.reference,
                       dir,
                       cmdArgs.sampleTag,
                       cmdArgs.intervals,
                       dict)
  }

  def descriptionText: String =
    """
      |This tool will try to find a given number of clusters based on variant data per cell.
      |
      |In this tools multiple module from the biopet 10x kit is executed in memory.
    """.stripMargin

  def manualText: String =
    """
      |The input to this tool is the bamfile that is create by Cellranger.
      |
      |This tool require to run on spark.
      |The reference file should have a dict and a fai file next to it.
      |
    """.stripMargin

  def exampleText: String =
    s"""
      |Default run with 5 expected samples:
      |${TenxKit.sparkExample(
         "SampleMatcher",
         "--sparkMaster",
         "<spark master>",
         "-i",
         "<cellranger bam file>",
         "-R",
         "<reference fasta file>",
         "-o",
         "<output directory>",
         "-c",
         "5",
         "--correctCells",
         "<correct cells file>"
       )}
      |
    """.stripMargin

}
