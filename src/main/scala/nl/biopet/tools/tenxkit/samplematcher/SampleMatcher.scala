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

import nl.biopet.tools.tenxkit.TenxKit
import nl.biopet.utils.tool.ToolCommand
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SampleMatcher extends ToolCommand[Args] {
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

    //TODO: Make pipeline

    sparkSession.stop()
    logger.info("Done")
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
