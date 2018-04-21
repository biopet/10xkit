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

package nl.biopet.tools.tenxkit.mergebams

import java.io.File

import htsjdk.samtools.{SamReaderFactory, ValidationStringency}
import nl.biopet.utils.test.tools.ToolTest
import org.testng.annotations.{DataProvider, Test}

import scala.collection.JavaConversions._

class MergeBamsTest extends ToolTest[Args] {
  def toolCommand: MergeBams.type = MergeBams
  @Test
  def testNoArgs(): Unit = {
    intercept[IllegalArgumentException] {
      MergeBams.main(Array())
    }
  }

  @Test
  def testToLessBamFiles(): Unit = {
    intercept[IllegalArgumentException] {
      MergeBams.main(
        Array("--inputBam",
              "bam1=<file>",
              "--inputBarcode",
              "bam1=<file>",
              "--outputBam",
              "<file>",
              "--outputBarcodes",
              "<file>"))
    }.getMessage shouldBe "requirement failed: At least 2 bam files should be given"
  }

  @Test
  def testMissingBarcodeFile(): Unit = {
    intercept[IllegalArgumentException] {
      MergeBams.main(
        Array("--inputBam",
              "bam1=<file>",
              "--inputBarcode",
              "bam1=<file>",
              "--inputBam",
              "bam2=<file>",
              "--outputBam",
              "<file>",
              "--outputBarcodes",
              "<file>"))
    }.getMessage shouldBe "requirement failed: Not all given bam files have a barcode file"
  }

  @Test
  def testMissingBamFile(): Unit = {
    intercept[IllegalArgumentException] {
      MergeBams.main(
        Array(
          "--inputBam",
          "bam1=<file>",
          "--inputBarcode",
          "bam1=<file>",
          "--outputBam",
          "<file>",
          "--outputBarcodes",
          "<file>",
          "--inputBam",
          "bam2=<file>",
          "--inputBarcode",
          "bam2=<file>",
          "--inputBarcode",
          "bam3=<file>"
        ))
    }.getMessage shouldBe "requirement failed: Not all given barcode files have a bam file"
  }

  @Test
  def testDefault(): Unit = {
    val outputBam = File.createTempFile("merge.", ".bam")
    outputBam.deleteOnExit()
    val outputBarcodes = File.createTempFile("merge.", ".txt")
    outputBarcodes.deleteOnExit()
    MergeBams.main(
      Array(
        "--inputBam",
        s"bam1=${resourcePath("/wgs2.realign.bam")}",
        "--inputBarcode",
        s"bam1=${resourcePath("/wgs2.readgroups.txt")}",
        "--inputBam",
        s"bam2=${resourcePath("/wgs2.realign.bam")}",
        "--inputBarcode",
        s"bam2=${resourcePath("/wgs2.readgroups.txt")}",
        "--outputBam",
        s"$outputBam",
        "--outputBarcodes",
        s"$outputBarcodes",
        "--sampleTag",
        "RG"
      ))

    val reader = SamReaderFactory
      .makeDefault()
      .validationStringency(ValidationStringency.SILENT)
      .open(outputBam)
    val it1 = reader.iterator()
    it1.size shouldBe 40000
    it1.close()

    val it2 = reader.iterator()
    val cells =
      it2.flatMap(x => Option(x.getAttribute("RG")).map(_.toString)).toSet
    cells shouldBe Set("bam1-wgs2-lib2",
                       "bam1-wgs2-lib1",
                       "bam2-wgs2-lib2",
                       "bam2-wgs2-lib1")
    it2.close()
    reader.close()
  }

  @DataProvider(name = "fractions")
  def fractions: Array[Array[Any]] =
    Array(Array(0.1),
          Array(0.2),
          Array(0.3),
          Array(0.4),
          Array(0.5),
          Array(0.6),
          Array(0.7),
          Array(0.8),
          Array(0.9),
          Array(1.0))

  @Test(dataProvider = "fractions")
  def testDownsample(fraction: Double): Unit = {
    val outputBam = File.createTempFile("merge.", ".bam")
    outputBam.deleteOnExit()
    val outputBarcodes = File.createTempFile("merge.", ".txt")
    outputBarcodes.deleteOnExit()
    MergeBams.main(
      Array(
        "--inputBam",
        s"bam1=${resourcePath("/wgs2.realign.bam")}",
        "--inputBarcode",
        s"bam1=${resourcePath("/wgs2.readgroups.txt")}",
        "--inputBam",
        s"bam2=${resourcePath("/wgs2.realign.bam")}",
        "--inputBarcode",
        s"bam2=${resourcePath("/wgs2.readgroups.txt")}",
        "--outputBam",
        s"$outputBam",
        "--outputBarcodes",
        s"$outputBarcodes",
        "--sampleTag",
        "RG",
        "--downsampleFraction",
        fraction.toString
      ))

    val reader = SamReaderFactory
      .makeDefault()
      .validationStringency(ValidationStringency.SILENT)
      .open(outputBam)
    val it1 = reader.iterator()
    val aim = 40000 * fraction
    val reads = it1.size
    reads >= (aim - (aim / 10)) shouldBe true
    reads <= (aim + (aim / 10)) shouldBe true
    it1.close()

    reader.close()
  }
}
