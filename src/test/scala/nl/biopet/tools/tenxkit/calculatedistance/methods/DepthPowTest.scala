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

package nl.biopet.tools.tenxkit.calculatedistance.methods

import nl.biopet.test.BiopetTest
import org.testng.annotations.Test

class DepthPowTest extends BiopetTest {
  @Test
  def test(): Unit = {
    val pow1 = new DepthPow(1)
    val pow2 = new DepthPow(2)
    val value = pow1.calculate(IndexedSeq(2, 0, 0), IndexedSeq(0, 2, 0))
    value.isNaN shouldBe false

    pow1.calculate(IndexedSeq(1, 1), IndexedSeq(1, 1)) shouldBe 0.0
    pow1.calculate(IndexedSeq(0, 1), IndexedSeq(1, 1)) shouldBe math.sqrt(2.0) / 2 / 2
    pow2.calculate(IndexedSeq(1, 1), IndexedSeq(1, 1)) shouldBe 0.0
    pow2.calculate(IndexedSeq(0, 1), IndexedSeq(1, 0)) shouldBe math.pow(
      math.sqrt(2.0) / 2,
      2)
  }
}
