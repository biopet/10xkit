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

package nl.biopet.tools.tenxkit.calculatedistance

import java.io.{File, PrintWriter}

import htsjdk.samtools.SAMSequenceDictionary
import nl.biopet.tools.tenxkit
import nl.biopet.tools.tenxkit.calculatedistance.methods.Method
import nl.biopet.tools.tenxkit.variantcalls.CellVariantcaller
import nl.biopet.tools.tenxkit.variantcalls.CellVariantcaller.getPartitions
import nl.biopet.tools.tenxkit.{
  DistanceMatrix,
  TenxKit,
  VariantCall,
  variantcalls
}
import nl.biopet.utils.ngs.{bam, fasta}
import nl.biopet.utils.tool.{AbstractOptParser, ToolCommand}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object CalulateDistance extends ToolCommand[Args] {
  def argsParser: AbstractOptParser[Args] = new ArgsParser(this)
  def emptyArgs: Args = Args()

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

    val correctCells = tenxkit.parseCorrectCells(cmdArgs.correctCells)
    val correctCellsMap = tenxkit.correctCellsMap(correctCells)
    val dict = sc.broadcast(fasta.getCachedDict(cmdArgs.reference))

    val futures: ListBuffer[Future[Any]] = ListBuffer()

    val (variants, variantFutures) =
      getVariants(cmdArgs, correctCells, correctCellsMap, dict)
    futures ++= variantFutures

    val result = totalRun(variants,
                          cmdArgs.outputDir,
                          correctCells,
                          cmdArgs.minAlleleCoverage,
                          cmdArgs.method,
                          //cmdArgs.additionalMethods,
                          cmdArgs.writeScatters)
    result.writeCounts(new File(cmdArgs.outputDir, "count.positions.tsv"))
    result.writeUncorrectedMatrix(
      new File(cmdArgs.outputDir, s"distance.${cmdArgs.method}.tsv"))
    result.writeCorrectedMatrix(
      new File(cmdArgs.outputDir, s"distance.corrected.${cmdArgs.method}.tsv"))
    Await.result(result.totalFuture, Duration.Inf)

    sparkSession.stop()
    logger.info("Done")
  }

  case class Result(distanceRdd: RDD[(SampleCombinationKey, (Double, Long))],
                    correctCells: Broadcast[IndexedSeq[String]]) {
    private val futures = new ListBuffer[Future[Any]]()
    def totalFuture: Future[List[Any]] = Future.sequence(futures.toList)
    lazy val correctedDistancesRdd: RDD[(SampleCombinationKey, Double)] =
      distanceRdd.map { case (k, (d, c)) => k -> d / c }
    lazy val uncorrectedDistancesRdd: RDD[(SampleCombinationKey, Double)] =
      distanceRdd.map { case (k, (d, _)) => k -> d }
    val countsRdd: RDD[(SampleCombinationKey, Long)] = distanceRdd.map {
      case (k, (_, c)) => k -> c
    }
    lazy val correctedDistancesMatrixRdd: RDD[DistanceMatrix] =
      DistanceMatrix.rddToMatrix(correctedDistancesRdd, correctCells)
    lazy val uncorrectedDistancesMatrixRdd: RDD[DistanceMatrix] =
      DistanceMatrix.rddToMatrix(uncorrectedDistancesRdd, correctCells)

    def writeCorrectedMatrix(outputFile: File): Unit = {
      futures += correctedDistancesMatrixRdd
        .collectAsync()
        .map(_(0).writeFile(outputFile))
    }

    def writeUncorrectedMatrix(outputFile: File): Unit = {
      futures += uncorrectedDistancesMatrixRdd
        .collectAsync()
        .map(_(0).writeFile(outputFile))
    }

    def writeCounts(outputFile: File): Unit = {
      futures += writeCountPositions(outputFile, countsRdd, correctCells)
    }
  }

  def runPerContig(variants: Map[String, RDD[VariantCall]],
                   outputDir: File,
                   correctCells: Broadcast[IndexedSeq[String]],
                   minAlleleCoverage: Int,
                   method: String,
                   scatters: Boolean = false)(
      implicit sc: SparkContext): Map[String, Result] = {
    val contigsDir = new File(outputDir, "contigs")
    contigsDir.mkdir()
    variants.map {
      case (k, v) =>
        sc.setJobGroup(s"Group distance: $k", s"Group distance: $k")
        val contigDir = new File(contigsDir, k)
        contigDir.mkdir()
        k -> totalRun(v,
                      contigDir,
                      correctCells,
                      minAlleleCoverage,
                      method,
                      scatters,
                      outputMatrix = false)
    }
  }

  def runTotalPerContig(
      variants: Map[String, RDD[VariantCall]],
      outputDir: File,
      correctCells: Broadcast[IndexedSeq[String]],
      minAlleleCoverage: Int,
      method: String,
      scatters: Boolean = false)(implicit sc: SparkContext): Result = {
    val contigResults = runPerContig(variants,
                                     outputDir,
                                     correctCells,
                                     minAlleleCoverage,
                                     method,
                                     scatters)

    sc.setJobGroup("Group distance", "Group distance")

    val futures = new ListBuffer[Future[Any]]()

    val distances = sc
      .union(contigResults.map(_._2.distanceRdd).toSeq)
      .reduceByKey { case ((d1, c1), (d2, c2)) => (d1 + d2, c1 + c2) }

    val counts = distances.map { case (k, v) => k -> v._2 }
    val correctedDistances = distances.map {
      case (k, v) => k -> v._1 / v._2
    }
    val correctedDistancesMatrix =
      DistanceMatrix.rddToMatrix(correctedDistances, correctCells)
    sc.setLocalProperty("spark.scheduler.pool", "high-prio")
    val matrix = correctedDistancesMatrix.collectAsync().map(_(0))

    val result = Result(distances, correctCells)
    result.writeCorrectedMatrix(
      new File(outputDir, s"distance.corrected.$method.tsv"))

    result
  }

  def totalRun(
      variants: RDD[VariantCall],
      outputDir: File,
      correctCells: Broadcast[IndexedSeq[String]],
      minAlleleCoverage: Int,
      method: String,
      scatters: Boolean = false,
      outputMatrix: Boolean = true)(implicit sc: SparkContext): Result = {
    val futures = new ListBuffer[Future[_]]()

    val distances =
      combinationDistance(method,
                          outputDir,
                          variants,
                          correctCells,
                          minAlleleCoverage)

    if (scatters) {
      val combinations = createCombinations(variants, minAlleleCoverage)
      futures += writeScatters(outputDir, combinations, correctCells)
    }

    val result = Result(distances, correctCells)

    result
  }

  def combinationDistance(methodString: String,
                          outputDir: File,
                          variants: RDD[VariantCall],
                          correctCells: Broadcast[IndexedSeq[String]],
                          minAlleleCoverage: Int)(implicit sc: SparkContext)
    : RDD[(SampleCombinationKey, (Double, Long))] = {
    val method = sc.broadcast(Method.fromString(methodString))

    variants
      .mapPartitions { it =>
        val result = mutable.Map[SampleCombinationKey, (Double, Long)]()
        it.foreach { variant =>
          val samples = variant.samples.filter {
            case (_, alleles) =>
              alleles.map(_.total).sum > minAlleleCoverage
          }.keys
          val samplesAd =
            samples.map(s => s -> variant.samples(s).map(_.total)).toMap
          for {
            s1 <- samples
            s2 <- samples
            if s2 > s1
          } {
            val key = SampleCombinationKey(s1, s2)
            if (!result.contains(key)) result += key -> (0.0, 0L)
            val (d, c) = result(key)
            result(key) =
              (d + method.value.calculate(samplesAd(s1), samplesAd(s2)), c + 1)
          }
        }
        result.iterator
      }
      .reduceByKey { case ((d1, c1), (d2, c2)) => (d1 + d2, c1 + c2) }
  }

  def createCombinations(variants: RDD[VariantCall], minAlleleCoverage: Int)
    : RDD[(SampleCombinationKey, Iterable[AlleleDepth])] = {
    variants
      .flatMap { variant =>
        val samples = variant.samples.filter {
          case (_, alleles) =>
            alleles.map(_.total).sum > minAlleleCoverage
        }.keys
        for (s1 <- samples; s2 <- samples if s2 > s1) yield {
          SampleCombinationKey(s1, s2) -> AlleleDepth(
            variant.samples(s1).map(_.total),
            variant.samples(s2).map(_.total))
        }
      }
      .groupByKey()
  }

  def countPositionsRdd(
      variants: RDD[VariantCall],
      minAlleleCoverage: Int): RDD[(SampleCombinationKey, Int)] = {
    variants
      .mapPartitions { it =>
        val result = mutable.Map[SampleCombinationKey, Int]()
        it.foreach { variant =>
          val samples = variant.samples.filter {
            case (_, alleles) =>
              alleles.map(_.total).sum > minAlleleCoverage
          }.keys
          for {
            s1 <- samples
            s2 <- samples
            if s2 > s1
          } {
            val key = SampleCombinationKey(s1, s2)
            if (!result.contains(key)) result += key -> 1
            else result(key) = result(key) + 1
          }
        }
        result.iterator
      }
      .reduceByKey(_ + _)
  }

  def writeCountPositions(
      outputFile: File,
      counts: RDD[(SampleCombinationKey, Long)],
      correctCells: Broadcast[IndexedSeq[String]]): Future[File] = {
    counts
      .groupBy { case (key, _) => key.sample1 }
      .map {
        case (s1, list) =>
          val map = list.groupBy { case (key, _) => key.sample2 }.map {
            case (key, l) =>
              key -> l.headOption.map { case (_, d) => d }.getOrElse(0)
          }
          s1 -> (for (s2 <- correctCells.value.indices.toArray) yield {
            map.get(s2)
          })
      }
      .repartition(1)
      .mapPartitions { it =>
        val map = it.toMap
        val writer =
          new PrintWriter(outputFile)
        writer.println(correctCells.value.mkString("Sample\t", "\t", ""))
        for (s1 <- correctCells.value.indices) {
          writer.print(s"${correctCells.value(s1)}\t")
          writer.println(
            correctCells.value.indices
              .map(s2 => map.get(s1).flatMap(_.lift(s2)))
              .map(x => x.flatten.getOrElse("."))
              .mkString("\t"))
        }
        writer.close()
        Iterator(outputFile)
      }
      .collectAsync()
      .map(_(0))
  }

  def writeScatters(
      outputDir: File,
      combinations: RDD[(SampleCombinationKey, Iterable[AlleleDepth])],
      correctCells: Broadcast[IndexedSeq[String]]): Future[Unit] = {
    val scatterDir = new File(outputDir, "scatters")
    val fractionPairs = combinations.map {
      case (key, alleles) =>
        key -> alleles.map { pos =>
          val total1 = pos.ad1.sum
          val total2 = pos.ad2.sum
          val fractions1 = pos.ad1.map(_.toDouble / total1)
          val fractions2 = pos.ad2.map(_.toDouble / total2)
          fractions1.zip(fractions2).map {
            case (f1, f2) => FractionPairDistance(f1, f2)
          }
        }
    }

    fractionPairs.foreachAsync {
      case (c, b) =>
        val sample1 = correctCells.value(c.sample1)
        val sample2 = correctCells.value(c.sample2)
        val dir = new File(scatterDir, sample1)
        dir.mkdirs()
        val writer = new PrintWriter(new File(dir, sample2 + ".tsv"))
        writer.println(s"#$sample1\t$sample2\tDistance")
        b.flatten.foreach(x =>
          writer.println(x.f1 + "\t" + x.f2 + "\t" + x.distance))
        writer.close()
    }
  }

  def getVariants(cmdArgs: Args,
                  correctCells: Broadcast[IndexedSeq[String]],
                  correctCellsMap: Broadcast[Map[String, Int]],
                  dict: Broadcast[SAMSequenceDictionary])(
      implicit sc: SparkContext): (RDD[VariantCall], List[Future[_]]) = {
    val futures = new ListBuffer[Future[_]]()
    val v = cmdArgs.inputFile.getName match {
      case name if name.endsWith(".bam") =>
        val result =
          readBamFile(cmdArgs, correctCells, correctCellsMap, cmdArgs.binSize)
        lazy val vcfHeader = sc.broadcast(tenxkit.vcfHeader(correctCells.value))
        result.writeFilterVariants(cmdArgs.outputDir,
                                   correctCells,
                                   dict,
                                   vcfHeader,
                                   0.005f)
        futures += result.totalFuture
        result.filteredVariants
      case name
          if name.endsWith(".vcf") | name.endsWith(".vcf.gz") | cmdArgs.inputFile.isDirectory =>
        VariantCall.fromVcfFile(cmdArgs.inputFile,
                                dict,
                                correctCellsMap,
                                cmdArgs.binSize)
      case _ =>
        throw new IllegalArgumentException(
          "Input file must be a bam or vcf file")
    }
    (v.filter(_.totalAltRatio >= cmdArgs.minTotalAltRatio)
       .repartition(v.partitions.length),
     futures.toList)
  }

  def readBamFile(
      cmdArgs: Args,
      correctCells: Broadcast[IndexedSeq[String]],
      correctCellsMap: Broadcast[Map[String, Int]],
      binsize: Int)(implicit sc: SparkContext): CellVariantcaller.Result = {
    logger.info(s"Starting variant calling on '${cmdArgs.inputFile}'")
    logger.info(
      s"Using default parameters, to set different cutoffs please use the CellVariantcaller module")
    val dict = sc.broadcast(bam.getDictFromBam(cmdArgs.inputFile))
    val partitions =
      (dict.value.getReferenceLength.toDouble / binsize).ceil.toInt
    val cutoffs = sc.broadcast(variantcalls.Cutoffs())
    val regions =
      tenxkit.createRegions(cmdArgs.inputFile,
                            cmdArgs.reference,
                            getPartitions(cmdArgs.inputFile, None),
                            cmdArgs.intervals)

    val result = CellVariantcaller.totalRun(
      cmdArgs.inputFile,
      cmdArgs.outputDir,
      cmdArgs.reference,
      dict,
      regions,
      cmdArgs.sampleTag,
      cmdArgs.umiTag,
      correctCells,
      correctCellsMap,
      cutoffs,
      variantcalls.Args().seqError
    )

    result
  }

  def descriptionText: String =
    """
      |This tool will calculate for each cell combination a sum of distances.
      |The distances are the relative distance to the middle line for each allele divided by the total coverage on that position.
    """.stripMargin

  def manualText: String =
    """
      |The input file can be a bam file or a vcf file.
      |When a bam file is given the variantcall step is execute before and execute everything in memory.
      |This is faster then running it separately but does not give control to all option of the variantcalling step.
    """.stripMargin

  def exampleText: String =
    s"""
      |Default run with bam file:
      |${TenxKit.sparkExample(
         "CalulateDistance",
         "-i",
         "<bam file>",
         "-R",
         "<reference fasta>",
         "-o",
         "<output directory>",
         "--correctCells",
         "<correctCells.txt>",
         "--sparkMaster",
         "<spark master>"
       )}
      |
    """.stripMargin

}
