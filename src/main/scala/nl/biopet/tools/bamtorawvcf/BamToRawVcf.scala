/*
 * Copyright (c) 2017 Biopet
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

package nl.biopet.tools.bamtorawvcf

import java.io.File

import nl.biopet.utils.Histogram
import nl.biopet.utils.tool.ToolCommand
import nl.biopet.utils.ngs.bam
import nl.biopet.utils.ngs.intervals.BedRecordList
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object BamToRawVcf extends ToolCommand[Args] {
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
    import sparkSession.implicits._
    logger.info(
      s"Context is up, see ${sparkSession.sparkContext.uiWebUrl.getOrElse("")}")

    val reads = sc.loadBam(cmdArgs.inputFile.getAbsolutePath)
    val groups = reads.rdd
      .flatMap { read =>
        read.getAttributes
          .split("\t")
          .find(_.startsWith(cmdArgs.sampleTag + ":"))
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
    histogram.writeHistogramToTsv(cmdArgs.outputFile)
    histogramDuplicates.writeHistogramToTsv(
      new File(cmdArgs.outputFile + ".duplucates"))

    logger.info("Done")
  }

  def descriptionText: String =
    """
      |
    """.stripMargin

  def manualText: String =
    s"""
      |
    """.stripMargin

  def exampleText: String =
    """
      |
    """.stripMargin
}
