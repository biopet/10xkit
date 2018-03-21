/*
 * Copyright (c) 2017 Biopet
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

package nl.biopet.tools.bamtorawvcf

import java.io.File

import nl.biopet.utils.test.tools.ToolTest
import org.testng.annotations.Test

class BamToRawVcfTest extends ToolTest[Args] {
  def toolCommand: BamToRawVcf.type = BamToRawVcf
  @Test
  def testNoArgs(): Unit = {
    intercept[IllegalArgumentException] {
      BamToRawVcf.main(Array())
    }
  }

  @Test
  def testDefault(): Unit = {
    val outputFile = File.createTempFile("test.", ".csv")
    outputFile.delete()
    outputFile.deleteOnExit()
    BamToRawVcf.main(
      Array(
        "-i",
        resourcePath("/paired01.bam"),
        "--sparkMaster",
        "local[2]",
        "-R",
        "<some fasta>",
        "--sampleTag",
        "NM",
        "-o",
        outputFile.getAbsolutePath
      ))
  }
}
