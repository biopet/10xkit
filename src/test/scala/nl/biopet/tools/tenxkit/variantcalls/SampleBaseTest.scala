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

package nl.biopet.tools.tenxkit.variantcalls

import nl.biopet.test.BiopetTest
import org.testng.annotations.Test

class SampleBaseTest extends BiopetTest {
  @Test
  def test(): Unit = {
    val seq = "AATTCCGGAA"
    val bases = SampleBase.createBases(1,
                                       true,
                                       seq.getBytes,
                                       "AAAAAAAAAA".getBytes,
                                       "3M1D3M1I3M")
    val sortedBases = bases.sortBy { case (x, _) => x }
    sortedBases.size shouldBe seq.length
    sortedBases.map { case (_, x) => x.allele }.mkString shouldBe "AATTCCGGAA"
    val (_, base2) = sortedBases(2)
    val (_, base6) = sortedBases(6)
    base2.delBases shouldBe 1
    base6.allele shouldBe "CG"
  }
}
