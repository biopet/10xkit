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

package object calculatedistance {
  case class SampleCombinationKey(sample1: Int, sample2: Int)
  case class AlleleDepth(ad1: IndexedSeq[Int], ad2: IndexedSeq[Int])

  case class FractionPairDistance(f1: Double, f2: Double, distance: Double)

  object FractionPairDistance {
    def apply(f1: Double, f2: Double): FractionPairDistance =
      FractionPairDistance(f1, f2, distanceMidle(f1, f2))
    def midlePoint(f1: Double, f2: Double): Double = ((f1 - f2) / 2) + f1
    def distanceMidle(f1: Double, f2: Double): Double =
      math.sqrt(math.pow(midlePoint(f1, f2) - f1, 2) * 2)
  }
}
