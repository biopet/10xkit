package nl.biopet.tools.tenxkit.cellreads

import java.io.File

import nl.biopet.tools.tenxkit.{Args, ArgsParser}
import nl.biopet.utils.Histogram
import nl.biopet.utils.tool.ToolCommand
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext._

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
