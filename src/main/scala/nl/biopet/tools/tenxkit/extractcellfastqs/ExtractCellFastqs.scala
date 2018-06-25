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
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.formats.avro.AlignmentRecord

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

// import RDD load functions and conversion functions
import org.bdgenomics.adam.rdd.ADAMContext._

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

    val bamRecords = sc.loadBam(cmdArgs.inputFile.getAbsolutePath)

    val cells =
      bamRecordsToCells(bamRecords, correctCellsMap, cmdArgs.sampleTag)

    writeCellsFastq(cells, correctCells, cmdArgs.outputDir)

    sparkSession.stop()
    logger.info("Done")
  }

  case class FastqRead(id: String,
                       seq: Seq[Byte],
                       qual: Seq[Byte],
                       pair: Option[Boolean]) {
    def toFastqRecord: FastqRecord =
      new FastqRecord(id, seq.toArray, "", qual.toArray)

    override def toString: String = {
      val s = new String(seq.toArray)
      val q = new String(qual.toArray)
      s"($id, $s, $q, $pair)"
    }
  }
  object FastqRead {
    def apply(read: SAMRecord): FastqRead = {
      val pair = if (read.getReadPairedFlag) {
        Some(read.getSecondOfPairFlag)
      } else None
      FastqRead(read.getReadName,
                read.getReadBases,
                read.getBaseQualities,
                pair)
    }

    def apply(read: AlignmentRecord): FastqRead = {
      val pair = if (read.getReadPaired) {
        Some(read.getReadInFragment == 1)
      } else None
      FastqRead(read.getReadName,
                read.getSequence.map(_.toByte),
                read.getQual.map(_.toByte),
                pair)
    }
  }

  def bamRecordsToCells(bamRecords: AlignmentRecordRDD,
                        correctCellsMap: Broadcast[Map[String, Int]],
                        sampleTag: String): RDD[(Int, Iterable[FastqRead])] = {
    bamRecords.rdd
      .filter(_.getPrimaryAlignment)
      .filter(!_.getSupplementaryAlignment)
      .flatMap { read =>
        read.getAttributes
          .split("\t")
          .find(_.startsWith(sampleTag + ":"))
          .flatMap(_.split(":").lift(2))
          .flatMap(s => correctCellsMap.value.get(s))
          .map(s => s -> FastqRead(read))
      }
      .groupByKey()
  }

  def writeCellsFastq(cells: RDD[(Int, Iterable[FastqRead])],
                      correctCells: Broadcast[IndexedSeq[String]],
                      outputDir: File): Unit = {
    cells.foreach {
      case (cell, reads) =>
        val cellName = correctCells.value(cell)
        val outputFileR1 = new File(outputDir, cellName + "_R1.fq.gz")
        lazy val outputFileR2 =
          new File(outputDir, cellName + "_R2.fq.gz")
        val writerR1 = new FastqWriterFactory().newWriter(outputFileR1)
        lazy val writerR2 = new FastqWriterFactory().newWriter(outputFileR2)
        reads.toList
          .groupBy(_.id)
          .foreach {
            case (id, fragments) =>
              val f = fragments.distinct
              (f.lift(0), f.lift(1), f.lift(2)) match {
                case (Some(r1), Some(r2), None) =>
                  (r1.pair, r2.pair) match {
                    case (Some(false), Some(true)) =>
                      writerR1.write(r1.toFastqRecord)
                      writerR2.write(r2.toFastqRecord)
                    case (Some(true), Some(false)) =>
                      writerR1.write(r2.toFastqRecord)
                      writerR2.write(r1.toFastqRecord)
                    case _ =>
                      throw new IllegalStateException(
                        s"Read are not proper paired: $r1 - $r2")
                  }
                case (Some(r1), None, None) => writerR1.write(r1.toFastqRecord)
                case _ =>
                  throw new IllegalStateException(
                    s"More then 2 reads for a single read id found: $id")
              }
          }
        writerR1.close()
        writerR2.close()

        removeEmptyFastq(outputFileR2)
    }
  }

  def removeEmptyFastq(file: File): Unit = {
    val test = new FastqReader(file)
    if (!test.hasNext) {
      test.close()
      file.delete()
    } else test.close()
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
