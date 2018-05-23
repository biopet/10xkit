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

package nl.biopet.tools.tenxkit

import java.io.File

import nl.biopet.test.BiopetTest
import nl.biopet.utils.io.getLinesFromFile
import org.testng.annotations.Test

class DistanceMatrixTest extends BiopetTest {

  def distanceMatrix = DistanceMatrix(
    IndexedSeq(IndexedSeq(None, Option(0.0), Option(1.0), Option(1.0)),
      IndexedSeq(None, None, Option(1.0), Option(1.0)),
      IndexedSeq(None, None, None, Option(0.0)),
      IndexedSeq(None, None, None, None)),
    IndexedSeq("cell1", "cell2", "cell3", "cell4")
  )

  def distanceFileContents = List(
    "Sample\tcell1\tcell2\tcell3\tcell4",
    "cell1\t.\t0.0\t1.0\t1.0",
    "cell2\t.\t.\t1.0\t1.0",
    "cell3\t.\t.\t.\t0.0",
    "cell4\t.\t.\t.\t."
  )

  @Test
  def testApply(): Unit = {
    val matrix = distanceMatrix

    matrix(0, 0) shouldBe None
    matrix(0, 1) shouldBe Some(0.0)
    matrix(1, 0) shouldBe Some(0.0)
    matrix(1, 3) shouldBe Some(1.0)
    matrix(3, 1) shouldBe Some(1.0)
    matrix(2, 3) shouldBe Some(0.0)
    matrix(3, 2) shouldBe Some(0.0)
  }

  @Test
  def testOverlap(): Unit = {
    val matrix = distanceMatrix

    matrix.overlapSamples(0, 0.5) shouldBe Array(0, 1)
    matrix.overlapSamples(1, 0.5) shouldBe Array(0, 1)
    matrix.overlapSamples(2, 0.5) shouldBe Array(2, 3)
    matrix.overlapSamples(3, 0.5) shouldBe Array(2, 3)
  }

  @Test
  def testWrong(): Unit = {
    val matrix = distanceMatrix

    matrix.wrongSamples(0, 0.5) shouldBe Array(2, 3)
    matrix.wrongSamples(1, 0.5) shouldBe Array(2, 3)
    matrix.wrongSamples(2, 0.5) shouldBe Array(0, 1)
    matrix.wrongSamples(3, 0.5) shouldBe Array(0, 1)
  }

  @Test
  def testExtractSamples(): Unit = {
    val matrix = distanceMatrix
    val submatrix = distanceMatrix.extractSamples(List("cell1", "cell2"))

    submatrix.samples shouldBe Array("cell1", "cell2")
    submatrix.values shouldBe Array(Array(None, Option(0.0)), Array(None, None))
  }

  @Test
  def testFile(): Unit = {
    val matrix = distanceMatrix
    val outputFile = File.createTempFile("distance.", ".tsv")
    matrix.writeFile(outputFile)
    getLinesFromFile(outputFile) shouldBe distanceFileContents
    val matrix2 = DistanceMatrix.fromFile(outputFile)
    matrix.samples shouldBe matrix2.samples
    matrix.values shouldBe matrix2.values
  }
}
