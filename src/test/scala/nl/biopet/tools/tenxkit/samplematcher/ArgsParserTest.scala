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

package nl.biopet.tools.tenxkit.samplematcher

import java.io.File

import nl.biopet.test.BiopetTest
import org.testng.annotations.Test

class ArgsParserTest extends BiopetTest {
  @Test
  def test(): Unit = {
    val cmdArgs = SampleMatcher.cmdArrayToArgs(
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
        "0.5",
        "--expectedSamples",
        "5",
        "--umiTag",
        "BN",
        "--maxPvalue",
        "0.1",
        "--minAlternativeDepth",
        "15",
        "--minCellAlternativeDepth",
        "16",
        "--minTotalDepth",
        "17",
        "--minAlleleDepth",
        "18",
        "--method",
        "pow1",
        "--additionalMethod",
        "pow3",
        "--writeScatters",
        "--numIterations",
        "100",
        "--skipKmeans",
        "--seed",
        "1234",
        "--minSampleDepth",
        "20"
      ))
    cmdArgs.inputFile shouldBe new File("file.bam")
    cmdArgs.reference shouldBe new File("reference.fasta")
    cmdArgs.outputDir shouldBe new File("out")
    cmdArgs.correctCellsFile shouldBe new File("correct.txt")
    cmdArgs.sparkMaster shouldBe "local[1]"
    cmdArgs.seqError shouldBe 0.5f
    cmdArgs.expectedSamples shouldBe 5
    cmdArgs.umiTag shouldBe "BN"
    cmdArgs.cutoffs.maxPvalue shouldBe 0.1f
    cmdArgs.cutoffs.minAlternativeDepth shouldBe 15
    cmdArgs.cutoffs.minCellAlternativeDepth shouldBe 16
    cmdArgs.cutoffs.minTotalDepth shouldBe 17
    cmdArgs.cutoffs.minAlleleDepth shouldBe 18
    cmdArgs.method shouldBe "pow1"
    cmdArgs.additionalMethods shouldBe List("pow3")
    cmdArgs.writeScatters shouldBe true
    cmdArgs.numIterations shouldBe 100
    cmdArgs.skipKmeans shouldBe true
    cmdArgs.seed shouldBe 1234L
    cmdArgs.cutoffs.minSampleDepth shouldBe 20
  }
}
