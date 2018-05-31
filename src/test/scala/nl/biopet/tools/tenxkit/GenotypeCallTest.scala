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

class GenotypeCallTest extends BiopetTest {
  @Test
  def testGenotype(): Unit = {
    GenotypeCall.fromAd(Array(0, 10)).genotype.flatten shouldBe Array(1, 1)
    GenotypeCall.fromAd(Array(1, 10)).genotype.flatten shouldBe Array(1, 1)
    GenotypeCall.fromAd(Array(5, 5)).genotype.flatten shouldBe Array(0, 1)
    GenotypeCall.fromAd(Array(10, 0)).genotype.flatten shouldBe Array(0, 0)
    GenotypeCall.fromAd(Array(0, 5, 5)).genotype.flatten shouldBe Array(1, 2)
    GenotypeCall.fromAd(Array(2, 3, 2)).genotype.flatten shouldBe Array(0, 1)
    GenotypeCall.fromAd(Array(2, 3, 3)).genotype.flatten shouldBe Array(1, 2)
    GenotypeCall.fromAd(Array(2, 3, 3), 3).genotype.flatten shouldBe Array(0,
                                                                           1,
                                                                           2)
    GenotypeCall.fromAd(Array(10, 1, 20), 3).genotype.flatten shouldBe Array(0,
                                                                             2,
                                                                             2)
    GenotypeCall.fromAd(Array(20, 1, 20), 4).genotype.flatten shouldBe Array(0,
                                                                             0,
                                                                             2,
                                                                             2)
    GenotypeCall.fromAd(Array(10, 1, 30), 4).genotype.flatten shouldBe Array(0,
                                                                             2,
                                                                             2,
                                                                             2)
    GenotypeCall.fromAd(Array(10, 10, 20), 4).genotype.flatten shouldBe Array(0,
                                                                              1,
                                                                              2,
                                                                              2)
  }

  @Test
  def testMinDepth(): Unit = {
    GenotypeCall
      .fromAd(Array(5, 1, 1, 1, 1, 1, 1, 1), 2, 2)
      .genotype shouldBe Array(None, Some(0))
  }

  @Test
  def testProbability(): Unit = {
    GenotypeCall.adToProbability(0.5, 0, 2) shouldBe 0.0
    GenotypeCall.adToProbability(0.5, 1, 2) shouldBe 1.0
    GenotypeCall.adToProbability(0.5, 2, 2) shouldBe 0.0

    GenotypeCall.adToProbability(0.0, 0, 2) shouldBe 1.0
    GenotypeCall.adToProbability(0.0, 1, 2) shouldBe 0.0
    GenotypeCall.adToProbability(0.0, 2, 2) shouldBe 0.0

    GenotypeCall.adToProbability(1.0, 0, 2) shouldBe 0.0
    GenotypeCall.adToProbability(1.0, 1, 2) shouldBe 0.0
    GenotypeCall.adToProbability(1.0, 2, 2) shouldBe 1.0

    GenotypeCall.adToProbability(1.0, 0, 3) shouldBe 0.0
    GenotypeCall.adToProbability(1.0, 1, 3) shouldBe 0.0
    GenotypeCall.adToProbability(1.0, 2, 3) shouldBe 0.0
    GenotypeCall.adToProbability(1.0, 3, 3) shouldBe 1.0

    GenotypeCall.adToProbability(1.0 / 3, 0, 3) shouldBe 0.0
    GenotypeCall.adToProbability(1.0 / 3, 1, 3) shouldBe 1.0
    GenotypeCall.adToProbability(1.0 / 3, 2, 3) shouldBe 0.0
    GenotypeCall.adToProbability(1.0 / 3, 3, 3) shouldBe 0.0

    GenotypeCall.adToProbability(0.0, 0, 3) shouldBe 1.0
    GenotypeCall.adToProbability(0.0, 1, 3) shouldBe 0.0
    GenotypeCall.adToProbability(0.0, 2, 3) shouldBe 0.0
    GenotypeCall.adToProbability(0.0, 3, 3) shouldBe 0.0

    GenotypeCall.adToProbability(1.0 / 3 * 2, 0, 3) shouldBe 0.0
    GenotypeCall.adToProbability(1.0 / 3 * 2, 1, 3) shouldBe 0.0
    GenotypeCall.adToProbability(1.0 / 3 * 2, 2, 3) shouldBe 1.0
    GenotypeCall.adToProbability(1.0 / 3 * 2, 3, 3) shouldBe 0.0

    GenotypeCall.adToProbability(1.0, 0, 4) shouldBe 0.0
    GenotypeCall.adToProbability(1.0, 1, 4) shouldBe 0.0
    GenotypeCall.adToProbability(1.0, 2, 4) shouldBe 0.0
    GenotypeCall.adToProbability(1.0, 3, 4) shouldBe 0.0
    GenotypeCall.adToProbability(1.0, 4, 4) shouldBe 1.0

    GenotypeCall.adToProbability(1.0 / 4, 0, 4) shouldBe 0.0
    GenotypeCall.adToProbability(1.0 / 4, 1, 4) shouldBe 1.0
    GenotypeCall.adToProbability(1.0 / 4, 2, 4) shouldBe 0.0
    GenotypeCall.adToProbability(1.0 / 4, 3, 4) shouldBe 0.0
    GenotypeCall.adToProbability(1.0 / 4, 4, 4) shouldBe 0.0

    GenotypeCall.adToProbability(0.0, 0, 4) shouldBe 1.0
    GenotypeCall.adToProbability(0.0, 1, 4) shouldBe 0.0
    GenotypeCall.adToProbability(0.0, 2, 4) shouldBe 0.0
    GenotypeCall.adToProbability(0.0, 3, 4) shouldBe 0.0
    GenotypeCall.adToProbability(0.0, 4, 4) shouldBe 0.0

    GenotypeCall.adToProbability(1.0 / 4 * 2, 0, 4) shouldBe 0.0
    GenotypeCall.adToProbability(1.0 / 4 * 2, 1, 4) shouldBe 0.0
    GenotypeCall.adToProbability(1.0 / 4 * 2, 2, 4) shouldBe 1.0
    GenotypeCall.adToProbability(1.0 / 4 * 2, 3, 4) shouldBe 0.0
    GenotypeCall.adToProbability(1.0 / 4 * 2, 4, 4) shouldBe 0.0

    GenotypeCall.adToProbability(1.0 / 4 * 3, 0, 4) shouldBe 0.0
    GenotypeCall.adToProbability(1.0 / 4 * 3, 1, 4) shouldBe 0.0
    GenotypeCall.adToProbability(1.0 / 4 * 3, 2, 4) shouldBe 0.0
    GenotypeCall.adToProbability(1.0 / 4 * 3, 3, 4) shouldBe 1.0
    GenotypeCall.adToProbability(1.0 / 4 * 3, 4, 4) shouldBe 0.0
  }
}
