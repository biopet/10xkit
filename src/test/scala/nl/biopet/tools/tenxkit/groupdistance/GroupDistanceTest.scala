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

package nl.biopet.tools.tenxkit.groupdistance

import java.io.File

import nl.biopet.tools.tenxkit.DistanceMatrix
import nl.biopet.tools.tenxkit.groupdistance.GroupDistance.GroupSample
import nl.biopet.utils.test.tools.ToolTest
import nl.biopet.utils.io.getLinesFromFile
import nl.biopet.utils.spark
import org.apache.spark.SparkContext
import org.testng.annotations.{DataProvider, Test}

class GroupDistanceTest extends ToolTest[Args] {
  def toolCommand: GroupDistance.type = GroupDistance
  @Test
  def testNoArgs(): Unit = {
    intercept[IllegalArgumentException] {
      GroupDistance.main(Array())
    }
  }

  val matrix = DistanceMatrix(
    IndexedSeq(
      IndexedSeq(None, Some(0.0), Some(1.0), Some(1.0)),
      IndexedSeq(None, None, Some(1.0), Some(1.0)),
      IndexedSeq(None, None, None, Some(0.0)),
      IndexedSeq(None, None, None, None)
    ),
    IndexedSeq("sample1", "sample2", "sample3", "sample4")
  )

  @Test
  def testDefault(): Unit = {
    val matrixFile = File.createTempFile("matrix.", ".tsv")
    matrixFile.deleteOnExit()
    matrix.writeFile(matrixFile)
    val outputDir = File.createTempFile("GroupDistance.", ".out")
    outputDir.delete()
    outputDir.mkdir()

    GroupDistance.main(
      Array(
        "-d",
        matrixFile.getAbsolutePath,
        "-o",
        outputDir.getAbsolutePath,
        "--sparkMaster",
        "local[1]",
        "--numClusters",
        "2",
        "--correctCells",
        resourcePath("/samples4.txt")
      ))

    val clusterFiles = outputDir
      .listFiles()
      .filter(_.getName.startsWith("cluster."))
      .filter(_.getName.endsWith(".txt"))
    clusterFiles.length shouldBe 2
    val clusters = clusterFiles.map(getLinesFromFile)
    clusters.contains(List("sample1", "sample2")) shouldBe true
    clusters.contains(List("sample3", "sample4")) shouldBe true
  }

  @Test
  def testNoKmeans(): Unit = {
    val matrixFile = File.createTempFile("matrix.", ".tsv")
    matrixFile.deleteOnExit()
    matrix.writeFile(matrixFile)
    val outputDir = File.createTempFile("GroupDistance.", ".out")
    outputDir.delete()
    outputDir.mkdir()

    GroupDistance.main(
      Array(
        "-d",
        matrixFile.getAbsolutePath,
        "-o",
        outputDir.getAbsolutePath,
        "--sparkMaster",
        "local[1]",
        "--numClusters",
        "2",
        "--correctCells",
        resourcePath("/samples4.txt"),
        "--skipKmeans"
      ))

    val clusterFiles = outputDir
      .listFiles()
      .filter(_.getName.startsWith("cluster."))
      .filter(_.getName.endsWith(".txt"))
    clusterFiles.length shouldBe 2
    val clusters = clusterFiles.map(getLinesFromFile(_).sorted)
    clusters.contains(List("sample1", "sample2")) shouldBe true
    clusters.contains(List("sample3", "sample4")) shouldBe true
  }

  @DataProvider(name = "recluster")
  def reCLusterProvider: Array[Array[List[Any]]] = {
    Array(
      Array(List(GroupSample(0, 0), GroupSample(4, 1)), List[Int](1, 2)),
      Array(List(GroupSample(1, 0),
                 GroupSample(2, 1),
                 GroupSample(3, 2),
                 GroupSample(4, 3)),
            List[Int]()),
      Array(List(GroupSample(0, 0),
                 GroupSample(1, 1),
                 GroupSample(1, 2),
                 GroupSample(0, 3)),
            List[Int]()),
      Array(List(GroupSample(0, 0),
                 GroupSample(0, 1),
                 GroupSample(1, 2),
                 GroupSample(1, 3)),
            List[Int]()),
      Array(List(GroupSample(0, 0),
                 GroupSample(0, 1),
                 GroupSample(0, 2),
                 GroupSample(0, 3)),
            List[Int]())
    )
  }

  @Test(dataProvider = "recluster")
  def testRecluster(startGroups: List[GroupSample],
                    startTrash: List[Int]): Unit = {
    implicit val sc: SparkContext =
      spark.loadSparkContext("test", Some("local[1]"))
    val outputDir = File.createTempFile("GroupDistance.", ".out")
    outputDir.delete()
    outputDir.mkdir()

    val cells =
      sc.broadcast(IndexedSeq("sample1", "sample2", "sample3", "sample4"))

    val matrixBroadcast = sc.broadcast(matrix)
    val (groups, trash) = GroupDistance.reCluster(sc.parallelize(startGroups),
                                                  matrixBroadcast,
                                                  2,
                                                  20,
                                                  sc.parallelize(startTrash),
                                                  outputDir,
                                                  cells)

    trash.collect() shouldBe empty
    val g = groups
      .collect()
      .groupBy(_.group)
      .map(x => x._1 -> x._2.map(_.sample).toList.sorted)
    g.values.toList.contains(List(0, 1))
    g.values.toList.contains(List(2, 3))

    sc.stop()
  }
}
