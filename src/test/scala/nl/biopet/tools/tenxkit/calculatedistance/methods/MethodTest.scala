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

class MethodTest extends BiopetTest {
  @Test
  def testFromStringPow(): Unit = {
    val method1 = Method.fromString("pow1").asInstanceOf[Pow]
    method1.pow shouldBe 1.0

    val method2 = Method.fromString("pow4").asInstanceOf[Pow]
    method2.pow shouldBe 4.0
  }

  @Test
  def testFromStringChi2(): Unit = {
    val method = Method.fromString("chi2")
    method.isInstanceOf[Chi2] shouldBe true
    method.isInstanceOf[Chi2Pow] shouldBe false
  }

  @Test
  def testFromStringChi2Pow(): Unit = {
    val method1 = Method.fromString("chi2pow1-1").asInstanceOf[Chi2Pow]
    method1.isInstanceOf[Chi2] shouldBe true
    method1.resultsPow shouldBe 1.0
    method1.countsPow shouldBe 1.0

    val method2 = Method.fromString("chi2pow3-5").asInstanceOf[Chi2Pow]
    method2.resultsPow shouldBe 5.0
    method2.countsPow shouldBe 3.0
  }

}
