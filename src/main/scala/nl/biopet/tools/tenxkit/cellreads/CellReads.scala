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

package nl.biopet.tools.tenxkit.cellreads

import java.io.File

import nl.biopet.tools.tenxkit.TenxKit
import nl.biopet.utils.Histogram
import nl.biopet.utils.tool.ToolCommand
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD

object CellReads extends ToolCommand[Args] {
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

    val reads: AlignmentRecordRDD =
      sc.loadBam(cmdArgs.inputFile.getAbsolutePath)
    generateHistograms(reads, cmdArgs.sampleTag, cmdArgs.outputDir)

    logger.info("Done")
  }

  def generateHistograms(reads: AlignmentRecordRDD,
                         sampleTag: String,
                         outputDir: File): Unit = {
    val groups = reads.rdd
      .flatMap { read =>
        read.getAttributes
          .split("\t")
          .find(_.startsWith(sampleTag + ":"))
          .map(read.getDuplicateRead -> _.split(":")(2))
      }
      .countByValue()

    val histogramDuplicates = new Histogram[Long]()
    val histogram = new Histogram[Long]()
    groups.groupBy(_._1._2).foreach {
      case (key, map) =>
        val dup = map.getOrElse((true, key), 0L)
        val nonDup = map.getOrElse((false, key), 0L)
        histogramDuplicates.add(dup + nonDup)
        histogram.add(nonDup)
    }
    histogram.writeHistogramToTsv(new File(outputDir, s"$sampleTag.csv"))
    histogramDuplicates.writeHistogramToTsv(
      new File(outputDir, s"$sampleTag.duplicates.csv"))
  }

  def descriptionText: String =
    """
      |This tool will generate a histogram of reads per cell barcode.
      |This can be used to validate output from cellranger or to set a alternative cutoff.
    """.stripMargin

  def manualText: String =
    s"""
       |By default this the sample tag is CB, this is the default tag used by Cellrenger.
       |If required the user can set this is a other tag with the '--sampleTag' option.
    """.stripMargin

  def exampleText: String =
    s"""
      |Default run:
      |${TenxKit.sparkExample("CellReads",
                              "-i",
                              "<input file>",
                              "-o",
                              "<output dir>",
                              "--sparkMaster",
                              "<spark master>")}
      |
      |Alternative tag:
      |${TenxKit.sparkExample("CellReads",
                              "-i",
                              "<input file>",
                              "-o",
                              "<output dir>",
                              "--sparkMaster",
                              "<spark master>",
                              "--sampleTag",
                              "<tag>")}
      |
    """.stripMargin
}
