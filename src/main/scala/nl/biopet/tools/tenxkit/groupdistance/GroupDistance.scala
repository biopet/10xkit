package nl.biopet.tools.tenxkit.groupdistance

import nl.biopet.tools.tenxkit.DistanceMatrix
import nl.biopet.utils.tool.ToolCommand

import scala.collection.mutable

object GroupDistance extends ToolCommand[Args] {
  def emptyArgs = Args()
  def argsParser = new ArgsParser(this)

  def main(args: Array[String]): Unit = {
    val cmdArgs = cmdArrayToArgs(args)

    logger.info("Start")

    logger.info("Reading input data")
    val distanceMatrix = DistanceMatrix.fromFile(cmdArgs.inputFile)

    logger.info("Calculate stats")
//    val totalHistogram = distanceMatrix.totalHistogram
//    val totalStats = totalHistogram.aggregateStats
//    val cutoff: Double = totalStats("mean").toString.toDouble
    val cutoff = 1.0

    logger.info(s"Mean of $cutoff is found")

    val overlap = distanceMatrix.samples.zipWithIndex.map { case (_, idx) =>
      idx -> distanceMatrix.overlapSamples(idx, cutoff).toSet
    }.toMap
    val mOverlap = mutable.Map(overlap.toList:_*)

    val totalCounts = mutable.Map(distanceMatrix.samples.indices.map(s => s -> 0):_*)
    overlap.foreach(_._2.foreach(s => totalCounts(s) += 1))
    val s1 = totalCounts.toList.minBy(_._2)._1

    val bla = for (s2 <- overlap(s1).toList) yield {
      s2 -> overlap(s1).count(s => overlap(s2).contains(s))
    }

//    val bla = overlap.map { case (s1, set1) =>
//      s1 -> overlap.map { case (s2, set2) =>
//        s2 -> (set2.count(set1.contains).toDouble / set1.size)
//      }
//    }

//    val counts = distanceMatrix.samples.map{ sample => sample -> overlap.count(_._2.contains(sample))}.toMap

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
