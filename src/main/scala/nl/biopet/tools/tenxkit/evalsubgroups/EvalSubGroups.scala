package nl.biopet.tools.tenxkit.evalsubgroups

import java.io.File

import nl.biopet.tools.tenxkit.DistanceMatrix
import nl.biopet.utils.tool.ToolCommand

import scala.io.Source
import scala.concurrent.ExecutionContext.Implicits.global

object EvalSubGroups extends ToolCommand[Args] {
  def emptyArgs = Args()
  def argsParser = new ArgsParser(this)

  def main(args: Array[String]): Unit = {
    val cmdArgs = cmdArrayToArgs(args)

    logger.info("Start")

    val readerIt = Source.fromFile(cmdArgs.inputFile).getLines()

    logger.info("Reading input data")

    val distanceMatrix = DistanceMatrix.fromFile(cmdArgs.inputFile)

    logger.info("Writing total data")
    val histogram = distanceMatrix.totalHistogram
    histogram.writeHistogramToTsv(
      new File(cmdArgs.outputDir, "total.histogram.tsv"))
    histogram.writeAggregateToTsv(
      new File(cmdArgs.outputDir, "total.aggregate.tsv"))

    logger.info("Binning total")
    val binnedHistogram = histogram.binned
    binnedHistogram.writeFilesAndPlot(cmdArgs.outputDir,
                                      "total.binned",
                                      "Distance",
                                      "Count",
                                      "Total")

    for ((name, file) <- cmdArgs.groups) {
      logger.info(s"Making histograms for '$name'")

      val subgroupHistogram = distanceMatrix.subgroupHistograms(
        name,
        Source.fromFile(file).getLines().toList)

      subgroupHistogram.group.writeHistogramToTsv(
        new File(cmdArgs.outputDir, s"$name.histogram.tsv"))
      subgroupHistogram.group.writeAggregateToTsv(
        new File(cmdArgs.outputDir, s"$name.aggregate.tsv"))

      subgroupHistogram.nonGroup.writeHistogramToTsv(
        new File(cmdArgs.outputDir, s"not_$name.histogram.tsv"))
      subgroupHistogram.nonGroup.writeAggregateToTsv(
        new File(cmdArgs.outputDir, s"not_$name.aggregate.tsv"))

      val sampleBinnedHistogram = subgroupHistogram.group.binned
      sampleBinnedHistogram.writeFilesAndPlot(cmdArgs.outputDir,
                                              s"$name.binned",
                                              "Distance",
                                              "Count",
                                              name)

      val notSampleBinnedHistogram = subgroupHistogram.nonGroup.binned
      notSampleBinnedHistogram.writeFilesAndPlot(cmdArgs.outputDir,
                                                 s"not_$name.binned",
                                                 "Distance",
                                                 "Count",
                                                 s"Not $name")
    }

    logger.info("Done")
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
