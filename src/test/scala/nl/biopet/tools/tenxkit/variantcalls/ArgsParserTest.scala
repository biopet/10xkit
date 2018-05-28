package nl.biopet.tools.tenxkit.variantcalls

import java.io.File

import nl.biopet.test.BiopetTest
import org.testng.annotations.Test

class ArgsParserTest extends BiopetTest {
  @Test
  def test(): Unit = {
    val cmdArgs = CellVariantcaller.cmdArrayToArgs(
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
        "--seqError",
        "0.5"
      ))
    cmdArgs.inputFile shouldBe new File("file.bam")
    cmdArgs.reference shouldBe new File("reference.fasta")
    cmdArgs.outputDir shouldBe new File("out")
    cmdArgs.correctCells shouldBe new File("correct.txt")
    cmdArgs.sparkMaster shouldBe "local[1]"
    cmdArgs.seqError shouldBe 0.5f
  }
}
