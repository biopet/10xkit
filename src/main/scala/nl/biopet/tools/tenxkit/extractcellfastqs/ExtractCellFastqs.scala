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

package nl.biopet.tools.tenxkit.extractcellfastqs

import java.io.File

import htsjdk.samtools.fastq.{FastqReader, FastqRecord, FastqWriterFactory}
import htsjdk.samtools.{QueryInterval, SAMRecord, SamReaderFactory}
import nl.biopet.tools.tenxkit
import nl.biopet.tools.tenxkit.TenxKit
import nl.biopet.utils.ngs.bam.getDictFromBam
import nl.biopet.utils.tool.ToolCommand
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object ExtractCellFastqs extends ToolCommand[Args] {
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

    val correctCells = tenxkit.parseCorrectCells(cmdArgs.correctCells)
    val correctCellsMap = tenxkit.correctCellsMap(correctCells)
    val dict = sc.broadcast(getDictFromBam(cmdArgs.inputFile))
    val partitions = (cmdArgs.reference.length() / cmdArgs.binSize).toInt + 2
    val regions = sc.parallelize(tenxkit.createRegions(cmdArgs.inputFile,
                                                       cmdArgs.reference,
                                                       partitions,
                                                       cmdArgs.intervals),
                                 partitions)

    val samReads = regions.mapPartitions { it =>
      val regions = it.toList.flatten
      val reader = SamReaderFactory.makeDefault().open(cmdArgs.inputFile)
      val intervals = regions
        .map(
          r =>
            new QueryInterval(dict.value.getSequenceIndex(r.chr),
                              r.start + 1,
                              r.end))
        .sortBy(x => (x.referenceIndex, x.start))
        .foldLeft(ListBuffer[QueryInterval]()) {
          case (a, b) =>
            a.lastOption match {
              case Some(l)
                  if l.referenceIndex == b.referenceIndex && l.end + 1 == b.start =>
                a -= l
                a += new QueryInterval(l.referenceIndex, l.start, b.end)
              case _ => a += b
            }
        }

      reader
        .query(intervals.toArray, false)
        .filter(!_.getDuplicateReadFlag)
        .filter(!_.getSupplementaryAlignmentFlag)
    }

    val cells = samReads
      .flatMap(
        read =>
          Option(read.getAttribute(cmdArgs.sampleTag))
            .flatMap(x => correctCellsMap.value.get(x.toString))
            .map(_ -> FastqRead(read)))
      .groupByKey()

    cells.foreach {
      case (cell, reads) =>
        val cellName = correctCells.value(cell)
        val outputFileR1 = new File(cmdArgs.outputDir, cellName + "_R1.fq.gz")
        lazy val outputFileR2 =
          new File(cmdArgs.outputDir, cellName + "_R2.fq.gz")
        val writerR1 = new FastqWriterFactory().newWriter(outputFileR1)
        lazy val writerR2 = new FastqWriterFactory().newWriter(outputFileR2)
        reads.toList
          .groupBy(x => (x.id, List(x.pos, x.matePos.getOrElse(0)).sorted))
          .foreach {
            case (_, fragments) =>
              val f = fragments.distinct
              (f.lift(0), f.lift(1), f.lift(2)) match {
                case (Some(r1), Some(r2), None) =>
                  if (r1.pair.contains(false) && r2.pair.contains(true)) {
                    writerR1.write(r1.toFastqRecord)
                    writerR2.write(r2.toFastqRecord)
                  } else if (r1.pair.contains(true) && r2.pair.contains(false)) {
                    writerR1.write(r2.toFastqRecord)
                    writerR2.write(r1.toFastqRecord)
                  } else {
                    throw new IllegalStateException(
                      s"Read are not proper paired: $r1 - $r2")
                  }
                case (Some(r1), None, None) => writerR1.write(r1.toFastqRecord)
                case _ =>
                  throw new IllegalStateException(
                    "More then 2 reads for a single read id found")
              }
          }
        writerR1.close()
        writerR2.close()

        val test = new FastqReader(outputFileR2)
        if (!test.hasNext) {
          test.close()
          outputFileR2.delete()
        } else test.close()
    }

    sparkSession.stop()
    logger.info("Done")
  }

  case class FastqRead(id: String,
                       seq: Seq[Byte],
                       qual: Seq[Byte],
                       pair: Option[Boolean],
                       pos: Int,
                       matePos: Option[Int]) {
    def toFastqRecord: FastqRecord =
      new FastqRecord(id, seq.toArray, "", qual.toArray)
  }
  object FastqRead {
    def apply(read: SAMRecord): FastqRead = {
      val pair = if (read.getReadPairedFlag) {
        Some(read.getSecondOfPairFlag)
      } else None
      val matePos = if (read.getReadPairedFlag) {
        Some(read.getMateAlignmentStart)
      } else None

      FastqRead(read.getReadName,
                read.getReadBases,
                read.getBaseQualities,
                pair,
                read.getAlignmentStart,
                matePos)
    }
  }

  def descriptionText: String =
    """
      |This tools will extract fastq files for a given list of cell barcodes.
      |All reads that are marked as duplicate or secondary will be skipped by default.
    """.stripMargin

  def manualText: String =
    s"""
       |By default the sample tag is CB, this is the default tag used by Cellrenger.
       |If required the user can set this is a other tag with the '--sampleTag' option.
    """.stripMargin

  def exampleText: String =
    s"""
      |Default run:
      |${TenxKit.sparkExample(
         "ExtractCellFastqs",
         "-i",
         "<input file>",
         "-R",
         "<reference fasta>",
         "-o",
         "<output dir>",
         "--sparkMaster",
         "<spark master>",
         "--correctCells",
         "<barcode file>"
       )}
      |
      |Alternative tag:
      |${TenxKit.sparkExample(
         "ExtractCellFastqs",
         "-i",
         "<input file>",
         "-R",
         "<reference fasta>",
         "-o",
         "<output dir>",
         "--sparkMaster",
         "<spark master>",
         "--sampleTag",
         "<tag>",
         "--correctCells",
         "<barcode file>"
       )}
      |
    """.stripMargin
}
