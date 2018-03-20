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

import nl.biopet.utils.tool.ToolCommand
import nl.biopet.utils.ngs.bam
import nl.biopet.utils.ngs.intervals.BedRecordList
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD

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
    implicit val sc = sparkSession.sparkContext
    import sparkSession.implicits._
    logger.info(
      s"Context is up, see ${sparkSession.sparkContext.uiWebUrl.getOrElse("")}")

    val dict = bam.getDictFromBam(cmdArgs.inputFile)
    val regions = BedRecordList.fromDict(dict).scatter(cmdArgs.binSize)

//    val reads = regions.flatten.map(
//      r =>
//        r -> sc.loadIndexedBam(cmdArgs.inputFile.getAbsolutePath,
//                               ReferenceRegion(r.chr, r.start, r.end)))
//    val rdd = AlignmentRecordRDD(
//      sc.union(reads.map(x => x._2.rdd.filter(_.getStart >= x._1.start))),
//      reads.map(a => a._2.sequences).reduce(_ ++ _),
//      reads.map(a => a._2.recordGroups).reduce(_ ++ _),
//      Nil
//    )
    val rdd = sc.loadBam(cmdArgs.inputFile.getAbsolutePath)

    val groups = rdd.rdd.map(
      _.getAttributes
        .split("\t")
        .find(_.startsWith(cmdArgs.sampleTag + ":"))
        .map(_.split(":")(2)))

    val values =
      groups.map(x => x.getOrElse("None") -> 1).reduceByKey(_ + _).toDS()
    values.write.csv(cmdArgs.outputFile.getAbsolutePath)

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
