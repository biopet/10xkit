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
import org.apache.spark.{FutureAction, SparkConf, SparkContext}

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

    val combinations = createCombinations(variants, cmdArgs.minAlleleCoverage)

    (cmdArgs.method :: cmdArgs.additionalMethods).distinct.sorted
      .foreach(
        m =>
          futures += combinationDistance(m,
                                         cmdArgs.outputDir,
                                         combinations,
                                         correctCells)
            .foreachAsync(
              _.writeFile(new File(cmdArgs.outputDir, s"distance.$m.csv"))))

    if (cmdArgs.writeScatters)
      futures += writeScatters(cmdArgs.outputDir, combinations, correctCells)

    futures += writeCountPositions(cmdArgs.outputDir,
                                   combinations,
                                   correctCells)

    Await.result(Future.sequence(futures), Duration.Inf)

    sparkSession.stop()
    logger.info("Done")
  }

  case class Result(distanceMatrix: Future[DistanceMatrix],
                    writeFileFutures: List[Future[Any]])

  def totalRun(variants: RDD[VariantCall],
               outputDir: File,
               correctCells: Broadcast[IndexedSeq[String]],
               minAlleleCoverage: Int,
               method: String,
               additionalMethods: List[String] = Nil,
               scatters: Boolean = false)(implicit sc: SparkContext): Result = {
    val futures = new ListBuffer[Future[_]]()
    val combinations = createCombinations(variants, minAlleleCoverage)

    val rdd = combinationDistance(method, outputDir, combinations, correctCells)
    futures += rdd.foreachAsync(
      _.writeFile(new File(outputDir, s"distance.$method.csv")))

    additionalMethods
      .filter(_ != method)
      .distinct
      .sorted
      .foreach(m =>
        futures += combinationDistance(m, outputDir, combinations, correctCells)
          .foreachAsync(_.writeFile(new File(outputDir, s"distance.$m.csv"))))

    if (scatters)
      futures += writeScatters(outputDir, combinations, correctCells)

    Result(Future(rdd.first()), futures.toList)
  }

  def combinationDistance(
      methodString: String,
      outputDir: File,
      combinations: RDD[(SampleCombinationKey, Iterable[AlleleDepth])],
      correctCells: Broadcast[IndexedSeq[String]])(
      implicit sc: SparkContext): RDD[DistanceMatrix] = {
    val method = sc.broadcast(Method.fromString(methodString))
    combinations
      .map {
        case (key, alleleDepts) =>
          key -> alleleDepts
            .map(x => method.value.calculate(x.ad1, x.ad2))
            .sum
      }
      .groupBy { case (key, _) => key.sample1 }
      .map {
        case (s1, list) =>
          val map = list.groupBy { case (key, _) => key.sample2 }.map {
            case (key, l) =>
              key -> l.headOption.map { case (_, d) => d }.getOrElse(0.0)
          }
          s1 -> (for (s2 <- correctCells.value.indices.toArray) yield {
            map.get(s2)
          })
      }
      .repartition(1)
      .mapPartitions { it =>
        val map = it.toMap
        val values = for (s1 <- correctCells.value.indices) yield {
          for (s2 <- correctCells.value.indices) yield {
            map.get(s1).flatMap(_.lift(s2)).flatten
          }
        }
        Iterator(DistanceMatrix(values, correctCells.value))
      }
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

  def writeCountPositions(
      outputDir: File,
      combinations: RDD[(SampleCombinationKey, Iterable[AlleleDepth])],
      correctCells: Broadcast[IndexedSeq[String]]): Future[Unit] = {
    combinations
      .map { case (key, list) => key -> list.size }
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
      .foreachPartitionAsync { it =>
        val map = it.toMap
        val writer =
          new PrintWriter(new File(outputDir, "count.positions.csv"))
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
      }
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
        futures += result.totalFuture
        Await.result(result.filteredVariants, Duration.Inf)
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
    val result = CellVariantcaller.totalRun(
      cmdArgs.inputFile,
      cmdArgs.outputDir,
      cmdArgs.reference,
      dict,
      partitions,
      cmdArgs.intervals,
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
