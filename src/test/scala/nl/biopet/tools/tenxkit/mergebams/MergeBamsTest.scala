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

import nl.biopet.utils.test.tools.ToolTest
import org.testng.annotations.Test

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
}
