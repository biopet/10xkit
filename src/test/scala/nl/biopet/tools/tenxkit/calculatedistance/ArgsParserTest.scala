package nl.biopet.tools.tenxkit.calculatedistance

import java.io.File

import nl.biopet.test.BiopetTest
import org.testng.annotations.Test

class ArgsParserTest extends BiopetTest {
  @Test
  def test(): Unit = {
    val cmdArgs = CalulateDistance.cmdArrayToArgs(
      Array(
        "-i",
        "file.bam",
        "-R",
        "reference.fasta",
        "-o",
        "out",
        "--correctCells",
        "correct.txt",
        "--sparkMaster",
        "local[1]",
        "--minTotalAltRatio",
        "0.5",
        "-u",
        "RG",
        "--binSize",
        "10"
      ))
    cmdArgs.inputFile shouldBe new File("file.bam")
    cmdArgs.reference shouldBe new File("reference.fasta")
    cmdArgs.outputDir shouldBe new File("out")
    cmdArgs.correctCells shouldBe new File("correct.txt")
    cmdArgs.sparkMaster shouldBe "local[1]"
    cmdArgs.minTotalAltRatio shouldBe 0.5
    cmdArgs.umiTag shouldBe Some("RG")
    cmdArgs.binSize shouldBe 10
  }
}
