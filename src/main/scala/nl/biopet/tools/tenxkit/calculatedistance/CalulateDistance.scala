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

import nl.biopet.tools.tenxkit
import nl.biopet.tools.tenxkit.{DistanceMatrix, variantcalls}
import nl.biopet.tools.tenxkit.variantcalls.{CellVariantcaller, VariantCall}
import nl.biopet.utils.ngs.{bam, fasta, vcf}
import nl.biopet.utils.ngs.intervals.BedRecordList
import nl.biopet.utils.tool.{AbstractOptParser, ToolCommand}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{FutureAction, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.broadcast

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

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
    import sparkSession.implicits._
    implicit val sc: SparkContext = sparkSession.sparkContext
    logger.info(
      s"Context is up, see ${sparkSession.sparkContext.uiWebUrl.getOrElse("")}")

    val correctCells = tenxkit.parseCorrectCells(cmdArgs.correctCells)
    val correctCellsMap = tenxkit.correctCellsMap(correctCells)

    val futures: ListBuffer[Future[Any]] = ListBuffer()

    val variants: RDD[VariantCall] = {
      val v = cmdArgs.inputFile.getName match {
        case name if name.endsWith(".bam") =>
          val result =
            readBamFile(cmdArgs, correctCells, correctCellsMap, cmdArgs.binSize)
          futures += result.totalFuture
          Await.result(result.filteredVariants, Duration.Inf)
        case name if name.endsWith(".vcf") || name.endsWith(".vcf.gz") =>
          readVcfFile(cmdArgs, correctCellsMap, cmdArgs.binSize)
        case _ =>
          throw new IllegalArgumentException(
            "Input file must be a bam or vcf file")
      }
      v.filter(_.totalAltRatio >= cmdArgs.minTotalAltRatio)
        .repartition(v.partitions.length)
    }

    val combinations = variants
      .flatMap { variant =>
        val samples = variant.samples
          .filter(_._2.map(_.total).sum > cmdArgs.minAlleleCoverage)
          .keys
        for (s1 <- samples; s2 <- samples if s2 > s1) yield {
          SampleCombinationKey(s1, s2) -> AlleleDepth(
            variant.samples(s1).map(_.total),
            variant.samples(s2).map(_.total))
        }
      }
      .groupByKey()
      .cache()

    val totalCombinations = combinations.count()
    logger.info(s"Total number of samples combinations: $totalCombinations")

    val fractionPairs = combinations.map {
      case (key, alleles) =>
        key -> alleles.map { pos =>
          val total1 = pos.ad1.sum
          val total2 = pos.ad2.sum
          //TODO: Filter alleles
          val fractions1 = pos.ad1.map(_.toDouble / total1)
          val fractions2 = pos.ad2.map(_.toDouble / total2)
          fractions1.zip(fractions2).map(x => FractionPairDistance(x._1, x._2))
        }
    }

    def combinationDistance(power: Int = 1): Future[Unit] = {
      val rdd: RDD[(SampleCombinationKey, Double)] =
        if (power == 1)
          fractionPairs.map(x => x._1 -> x._2.map(_.map(_.distance).sum).sum)
        else
          fractionPairs.map(x =>
            x._1 -> x._2.map(_.map(y => math.pow(y.distance, power)).sum).sum)

      rdd
        .groupBy(_._1.sample1)
        .map {
          case (s1, list) =>
            val map = list.groupBy(_._1.sample2).map(x => x._1 -> x._2.head._2)
            s1 -> (for (s2 <- correctCells.value.indices.toArray) yield {
              map.get(s2)
            })
        }
        .repartition(1)
        .foreachPartitionAsync { it =>
          val map = it.toMap
          val values = for (s1 <- correctCells.value.indices.toArray) yield {
            for (s2 <- correctCells.value.indices.toArray) yield {
              map.get(s1).flatMap(_.lift(s2)).flatten
            }
          }
          val matrix = DistanceMatrix(values, correctCells.value)
          matrix.writeFile(new File(cmdArgs.outputDir, s"distance.$power.csv"))
        }
    }

    futures += combinationDistance()
    futures += combinationDistance(2)
    futures += combinationDistance(3)
    futures += combinationDistance(4)

    if (cmdArgs.writeScatters) {
      val scatterDir = new File(cmdArgs.outputDir, "scatters")
      futures += fractionPairs.foreachAsync {
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

    futures += combinations
      .map(x => x._1 -> x._2.size)
      .groupBy(_._1.sample1)
      .map {
        case (s1, list) =>
          val map = list.groupBy(_._1.sample2).map(x => x._1 -> x._2.head._2)
          s1 -> (for (s2 <- correctCells.value.indices.toArray) yield {
            map.get(s2)
          })
      }
      .repartition(1)
      .foreachPartitionAsync { it =>
        val map = it.toMap
        val writer =
          new PrintWriter(new File(cmdArgs.outputDir, "count.positions.csv"))
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

    def sufixColumns(df: DataFrame, sufix: String): DataFrame = {
      df.columns.foldLeft(df)((a, b) => a.withColumnRenamed(b, b + sufix))
    }

    //TODO: Grouping

    Await.result(Future.sequence(futures), Duration.Inf)

    logger.info("Done")
  }

  def readVcfFile(cmdArgs: Args,
                  sampleMap: Broadcast[Map[String, Int]],
                  binsize: Int)(implicit sc: SparkContext): RDD[VariantCall] = {
    val dict = sc.broadcast(fasta.getCachedDict(cmdArgs.reference))
    val regions =
      BedRecordList.fromReference(cmdArgs.reference).scatter(binsize)
    sc.parallelize(regions, regions.size).mapPartitions { it =>
      it.flatMap { list =>
        vcf
          .loadRegions(cmdArgs.inputFile, list.iterator)
          .map(VariantCall.fromVariantContext(_, dict.value, sampleMap.value))
      }
    }
  }

  def readBamFile(
      cmdArgs: Args,
      correctCells: Broadcast[Array[String]],
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
