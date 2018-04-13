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

    logger.info("Reading input data")

    val distanceMatrix = DistanceMatrix.fromFile(cmdArgs.inputFile)
    val sampleMap = distanceMatrix.samples.zipWithIndex.toMap

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

    logger.info("Reading groups")
    val groups = cmdArgs.groups.map {
      case (sample, file) =>
        sample -> Source.fromFile(file).getLines().map(sampleMap).toList
    }

    for (((name1, list1), idx1) <- groups.zipWithIndex) {
      for (((name2, list2), idx2) <- groups.zipWithIndex if idx2 >= idx1) {
        //TODO: subMatrix

        val subgroupHistogram =
          distanceMatrix.subgroupHistograms(list1, list2)
        subgroupHistogram.writeHistogramToTsv(
          new File(cmdArgs.outputDir, s"$name1-$name2.histogram.tsv"))
        subgroupHistogram.writeAggregateToTsv(
          new File(cmdArgs.outputDir, s"$name1-$name2.aggregate.tsv"))

        val sampleBinnedHistogram = subgroupHistogram.binned
        sampleBinnedHistogram.writeFilesAndPlot(cmdArgs.outputDir,
                                                s"$name1-$name2.binned",
                                                "Distance",
                                                "Count",
                                                s"$name1-$name2")
      }
    }

//    for ((name, file) <- cmdArgs.groups) {
//      logger.info(s"Making histograms for '$name'")
//
//      val samples = Source.fromFile(file).getLines().toList
//
//      val subMatrix = distanceMatrix.extractSamples(samples)
//      subMatrix.writeFile(new File(cmdArgs.outputDir, s"$name.matrix.tsv"))
//
//      val subgroupHistogram = distanceMatrix.subgroupHistograms(name, samples)
//
//      subgroupHistogram.group.writeHistogramToTsv(
//        new File(cmdArgs.outputDir, s"$name.histogram.tsv"))
//      subgroupHistogram.group.writeAggregateToTsv(
//        new File(cmdArgs.outputDir, s"$name.aggregate.tsv"))
//
//      subgroupHistogram.nonGroup.writeHistogramToTsv(
//        new File(cmdArgs.outputDir, s"not_$name.histogram.tsv"))
//      subgroupHistogram.nonGroup.writeAggregateToTsv(
//        new File(cmdArgs.outputDir, s"not_$name.aggregate.tsv"))
//
//      val sampleBinnedHistogram = subgroupHistogram.group.binned
//      sampleBinnedHistogram.writeFilesAndPlot(cmdArgs.outputDir,
//                                              s"$name.binned",
//                                              "Distance",
//                                              "Count",
//                                              name)
//
//      val notSampleBinnedHistogram = subgroupHistogram.nonGroup.binned
//      notSampleBinnedHistogram.writeFilesAndPlot(cmdArgs.outputDir,
//                                                 s"not_$name.binned",
//                                                 "Distance",
//                                                 "Count",
//                                                 s"Not $name")
//    }

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
