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

import nl.biopet.test.BiopetTest
import org.testng.annotations.Test

class DistanceHistogramTest extends BiopetTest {
  @Test
  def testBin(): Unit = {
    val hist = new DistanceHistogram()
    for (i <- 0 to 20) hist.add(i.toDouble / 10000)

    hist.binned.countsMap shouldBe Map(0.0 -> 5, 0.001 -> 10, 0.002 -> 6)
  }

  @Test
  def testTotalDistance(): Unit = {
    val hist = new DistanceHistogram()
    hist.add(1.0)
    hist.add(2.0)

    hist.totalDistance shouldBe 3.0
  }

}
