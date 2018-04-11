package nl.biopet.tools.tenxkit.groupdistance

import nl.biopet.tools.tenxkit
import nl.biopet.tools.tenxkit.{DistanceMatrix, VariantCall}
import nl.biopet.utils.tool.ToolCommand

import scala.collection.mutable
import scala.util.Random

object GroupDistance extends ToolCommand[Args] {
  def emptyArgs = Args()
  def argsParser = new ArgsParser(this)

  def main(args: Array[String]): Unit = {
    val cmdArgs = cmdArrayToArgs(args)

    logger.info("Start")

    logger.info("Reading input data")
    val distanceMatrix = DistanceMatrix.fromFile(cmdArgs.inputFile)

    val variants = VariantCall.fromVcfFile(cmdArgs.inputFile)

    logger.info("Calculate stats")
//    val totalHistogram = distanceMatrix.totalHistogram
//    val totalStats = totalHistogram.aggregateStats
//    val cutoff: Double = totalStats("mean").toString.toDouble
    val cutoff = 0.00684343775694701

    logger.info(s"Mean of $cutoff is found")

    val numberGroups = 5
    val seed = 0



    val random = new Random(seed)
//    val randomGroups = random.shuffle(distanceMatrix.samples.indices).grouped(distanceMatrix.samples.length / numberGroups + 1)



    val overlap = distanceMatrix.samples.zipWithIndex.map { case (_, idx) =>
      idx -> distanceMatrix.overlapSamples(idx, cutoff).toSet
    }.toMap
    val mOverlap = mutable.Map(overlap.toList:_*)

    val matchAll = distanceMatrix.samples.indices.filter(i => mOverlap.values.forall(_.contains(i)))

    val minInitSize = 400

    def findInitGroups(todoSamples: List[Int], iterations: Int, result: List[List[Int]] = Nil): (List[List[Int]], List[Int]) = {
      val totalCounts = mutable.Map(todoSamples.map(s => s -> 0):_*)
      mOverlap.foreach(_._2.foreach(s => totalCounts(s) += 1))

      totalCounts.toList.sortBy(_._2).headOption match {
        case Some((s1, _))  if iterations > 0 =>
          val possibleSamples = mOverlap(s1)
          val newTodo = todoSamples.filterNot(possibleSamples.contains)
          possibleSamples.foreach(mOverlap -= _)
          mOverlap.map{ case (k, v) => mOverlap += k -> v.diff(possibleSamples)}

          findInitGroups(newTodo, iterations - 1, possibleSamples.toList :: result)
        case _ => (result, todoSamples)
      }
    }

    val (initGroup, leftover) = findInitGroups(distanceMatrix.samples.indices.toList, numberGroups)
    val groups = initGroup.map(_.map(distanceMatrix.samples).groupBy(_.split("-").head))

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
