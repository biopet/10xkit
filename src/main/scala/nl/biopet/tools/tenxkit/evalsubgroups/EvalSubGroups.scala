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

package nl.biopet.tools.tenxkit.evalsubgroups

import java.io.{File, PrintWriter}

import nl.biopet.tools.tenxkit.{DistanceMatrix, TenxKit}
import nl.biopet.utils.tool.ToolCommand

import scala.io.Source
import scala.concurrent.ExecutionContext.Implicits.global

object EvalSubGroups extends ToolCommand[Args] {
  def emptyArgs = Args()
  def argsParser = new ArgsParser(this)

  def main(args: Array[String]): Unit = {
    val cmdArgs = cmdArrayToArgs(args)

    logger.info("Start")

    logger.info("Reading groups")
    val groups = cmdArgs.groups.map {
      case (sample, file) =>
        sample -> Source.fromFile(file).getLines().toList
    }

    cmdArgs.distanceMatrix.foreach(
      evalDistanceMatrix(_, cmdArgs.outputDir, groups))

    if (cmdArgs.knownTrue.nonEmpty)
      calculateRecallPrecision(cmdArgs.knownTrue, cmdArgs.outputDir, groups)

    logger.info("Done")
  }

  private case class PrecisionRecall(precision: Double, recall: Double)

  private object PrecisionRecall {
    def calulate(knownKey: String,
                 knownList: List[String],
                 groupKey: String,
                 groupList: List[String],
                 outputDir: File): PrecisionRecall = {
      val overlap = knownList.intersect(groupList).length
      val missed = knownList.diff(groupList).length
      val wrong = groupList.diff(knownList).length

      val precision = overlap.toDouble / (overlap + wrong)
      val recall = overlap.toDouble / (overlap + missed)

      val outputFile =
        new File(outputDir, s"$knownKey-$groupKey.stats")
      val writer = new PrintWriter(outputFile)
      writer.println(s"Overlap\t$overlap")
      writer.println(s"Missed\t$missed")
      writer.println(s"Wrong\t$wrong")
      writer.println(s"Precision\t$precision")
      writer.println(s"Recall\t$recall")
      writer.close()
      PrecisionRecall(precision, recall)
    }
  }

  protected[tenxkit] def calculateRecallPrecision(
      knownTrueFile: Map[String, File],
      outputDir: File,
      groups: Map[String, List[String]]): Unit = {
    logger.info("Start calculating precision and recall")
    val knownTrue = knownTrueFile.map {
      case (sample, file) =>
        sample -> Source.fromFile(file).getLines().toList
    }

    val compareGroupsDir = new File(outputDir, "compare-groups")
    compareGroupsDir.mkdir()

    val result = for ((knownKey, knownList) <- knownTrue) yield {
      knownKey -> (for ((groupKey, groupList) <- groups) yield {
        groupKey -> PrecisionRecall
          .calulate(knownKey, knownList, groupKey, groupList, compareGroupsDir)
      })
    }

    val groupKeys = groups.keys.toList.sorted
    val knownKeys = knownTrue.keys.toList.sorted

    val precisionWriter = new PrintWriter(new File(outputDir, "precision.tsv"))
    val recallWriter = new PrintWriter(new File(outputDir, "recall.tsv"))
    precisionWriter.println(groupKeys.mkString("\t", "\t", ""))
    recallWriter.println(groupKeys.mkString("\t", "\t", ""))

    knownKeys.foreach { knownKey =>
      precisionWriter.println(
        groupKeys
          .map(result(knownKey)(_).precision)
          .mkString(s"$knownKey\t", "\t", ""))
      recallWriter.println(
        groupKeys
          .map(result(knownKey)(_).recall)
          .mkString(s"$knownKey\t", "\t", ""))
    }

    precisionWriter.close()
    recallWriter.close()
  }

  def evalDistanceMatrix(distanceMatrixFile: File,
                         outputDir: File,
                         groups: Map[String, List[String]]): Unit = {
    logger.info("Start reading distance matrix")
    evalDistanceMatrix(DistanceMatrix.fromFile(distanceMatrixFile),
                       outputDir,
                       groups)
  }

  def evalDistanceMatrix(distanceMatrix: DistanceMatrix,
                         outputDir: File,
                         groups: Map[String, List[String]]): Unit = {

    val sampleMap = distanceMatrix.samples.zipWithIndex.toMap
    val idxGroups = groups.map {
      case (name, list) => name -> list.map(sampleMap)
    }

    logger.info("Writing total data")
    val histogram = distanceMatrix.totalHistogram
    histogram.writeHistogramToTsv(new File(outputDir, "total.histogram.tsv"))
    histogram.writeAggregateToTsv(new File(outputDir, "total.aggregate.tsv"))

    logger.info("Binning total")
    val binnedHistogram = histogram.binned
    binnedHistogram.writeFilesAndPlot(outputDir,
                                      "total.binned",
                                      "Distance",
                                      "Count",
                                      "Total")

    for (((name1, list1), idx1) <- idxGroups.zipWithIndex) {
      for (((name2, list2), idx2) <- idxGroups.zipWithIndex if idx2 >= idx1) {
        val subgroupHistogram =
          distanceMatrix.subgroupHistograms(list1, list2)
        subgroupHistogram.writeHistogramToTsv(
          new File(outputDir, s"$name1-$name2.histogram.tsv"))
        subgroupHistogram.writeAggregateToTsv(
          new File(outputDir, s"$name1-$name2.aggregate.tsv"))

        val sampleBinnedHistogram = subgroupHistogram.binned
        sampleBinnedHistogram.writeFilesAndPlot(outputDir,
                                                s"$name1-$name2.binned",
                                                "Distance",
                                                "Count",
                                                s"$name1-$name2")
      }
    }
  }

  def descriptionText: String =
    """
      |This tool will compare all given groups.
      |The histogram for a Correct match should be a steep histogram starting at 0.
      |When multiple peaks are seen this might be a mixture of samples.
    """.stripMargin

  def manualText: String =
    """
      |This tool will require the complete distance matrix and a list of all cells per group.
      |It will generate a histogram for each combination of groups.
      |
      |When a known set is given the precision and recall of each combination of group and know set writen to a file.
    """.stripMargin

  def exampleText: String =
    s"""
      |Run with distance matrix example:
      |${TenxKit.example(
         "EvalSubGroups",
         "-o",
         "<output directory>",
         "-d",
         "<distance matrix>",
         "-g",
         "<group 1>=<group 1 file>",
         "-g",
         "<group 2>=<group 2 file>",
         "-g",
         "<<group 3>=group 3 file>"
       )}
      |
      |Run with know true set:
      |${TenxKit.example(
         "EvalSubGroups",
         "-o",
         "<output directory>",
         "-k",
         "sample1=<sample 1 file>",
         "-k",
         "sample2=<sample 2 file>",
         "-g",
         "<group 1>=<group 2 file>",
         "-g",
         "<group 2>=<group 2 file>"
       )}
      |
    """.stripMargin
}
