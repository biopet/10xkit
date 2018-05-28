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
