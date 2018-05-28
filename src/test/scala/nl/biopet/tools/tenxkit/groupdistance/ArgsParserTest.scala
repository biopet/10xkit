package nl.biopet.tools.tenxkit.groupdistance

import java.io.File

import nl.biopet.test.BiopetTest
import org.testng.annotations.Test

class ArgsParserTest extends BiopetTest {
  @Test
  def test(): Unit = {
    val cmdArgs = GroupDistance.cmdArrayToArgs(
      Array(
        "-d",
        "distance.tsv",
        "-o",
        "out",
        "--correctCells",
        "correct.txt",
        "--sparkMaster",
        "local[1]",
        "--numClusters",
        "5",
        "--numIterations",
        "5",
        "--seed",
        "4321"
      ))
    cmdArgs.distanceMatrix shouldBe new File("distance.tsv")
    cmdArgs.outputDir shouldBe new File("out")
    cmdArgs.correctCells shouldBe new File("correct.txt")
    cmdArgs.sparkMaster shouldBe "local[1]"
    cmdArgs.numClusters shouldBe 5
    cmdArgs.numIterations shouldBe 5
    cmdArgs.seed shouldBe 4321
  }
}
