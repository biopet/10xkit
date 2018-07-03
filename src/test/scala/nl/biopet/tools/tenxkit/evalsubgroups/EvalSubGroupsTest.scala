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

import java.io.File

import nl.biopet.tools.tenxkit.DistanceMatrix
import nl.biopet.utils.test.tools.ToolTest
import org.testng.annotations.Test
import nl.biopet.utils.io.{getLinesFromFile, writeLinesToFile}

class EvalSubGroupsTest extends ToolTest[Args] {
  def toolCommand: EvalSubGroups.type = EvalSubGroups
  @Test
  def testNoArgs(): Unit = {
    intercept[IllegalArgumentException] {
      EvalSubGroups.main(Array())
    }
  }

  @Test
  def testPerfectPrecisionRecall(): Unit = {
    val group1 = List("cell1", "cell2")
    val group2 = List("cell3", "cell4")
    val group1File = File.createTempFile("group.", ".txt")
    group1File.deleteOnExit()
    val group2File = File.createTempFile("group.", ".txt")
    group2File.deleteOnExit()
    val groupsFile = File.createTempFile("group.", ".txt")
    groupsFile.deleteOnExit()

    writeLinesToFile(group1File, group1)
    writeLinesToFile(group2File, group2)
    writeLinesToFile(groupsFile,
                     group1.map(_ + "\tgroup1") ::: group2.map(_ + "\tgroup2"))

    val outputDir = File.createTempFile("test.", ".eval")
    outputDir.delete()
    outputDir.mkdir()

    EvalSubGroups.main(
      Array("-g",
            s"$groupsFile",
            "-k",
            s"sample1=$group1File",
            "-k",
            s"sample2=$group2File",
            "-o",
            s"$outputDir"))

    val precisionFile = new File(outputDir, "precision.tsv")
    val recallFile = new File(outputDir, "recall.tsv")

    precisionFile should exist
    recallFile should exist

    val content = List(
      "\tgroup1\tgroup2",
      "sample1\t1.0\t0.0",
      "sample2\t0.0\t1.0"
    )

    getLinesFromFile(precisionFile) shouldBe content
    getLinesFromFile(recallFile) shouldBe content
  }

  @Test
  def testDistanceMatrix(): Unit = {
    val group1 = List("cell1", "cell2")
    val group2 = List("cell3", "cell4")
    val groupsFile = File.createTempFile("group.", ".txt")
    groupsFile.deleteOnExit()

    val distanceMatrixFile = File.createTempFile("distance.", ".tsv")
    val distanceMatrix = DistanceMatrix(
      IndexedSeq(
        IndexedSeq(None, Option(0.0), Option(1.0), Option(1.0)),
        IndexedSeq(None, None, Option(1.0), Option(1.0)),
        IndexedSeq(None, None, None, Option(0.0)),
        IndexedSeq(None, None, None, None)
      ),
      (group1 ::: group2).toIndexedSeq
    )
    distanceMatrix.writeFile(distanceMatrixFile)

    writeLinesToFile(groupsFile,
                     group1.map(_ + "\tgroup1") ::: group2.map(_ + "\tgroup2"))

    val outputDir = File.createTempFile("test.", ".eval")
    outputDir.delete()
    outputDir.mkdir()

    EvalSubGroups.main(
      Array("-g",
            s"$groupsFile",
            "-d",
            s"$distanceMatrixFile",
            "-o",
            s"$outputDir"))

    val c11 = new File(outputDir, "group1-group1.histogram.tsv")
    val c12 = new File(outputDir, "group1-group2.histogram.tsv")
    val c22 = new File(outputDir, "group2-group2.histogram.tsv")
    val c21 = new File(outputDir, "group2-group1.histogram.tsv")

    c11 should exist
    c12 should exist
    c22 should exist
    c21 shouldNot exist
  }
}
