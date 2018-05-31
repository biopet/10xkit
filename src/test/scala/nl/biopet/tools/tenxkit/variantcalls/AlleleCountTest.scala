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

class AlleleCountTest extends BiopetTest {
  @Test
  def testApply(): Unit = {
    AlleleCount().total shouldBe 0
    AlleleCount().forwardReads shouldBe 0
    AlleleCount().reverseReads shouldBe 0
    AlleleCount().forwardUmi shouldBe 0
    AlleleCount().reverseUmi shouldBe 0

    AlleleCount(1).total shouldBe 1
    AlleleCount(1).forwardReads shouldBe 1
    AlleleCount(1).reverseReads shouldBe 0
    AlleleCount(1).forwardUmi shouldBe 1
    AlleleCount(1).reverseUmi shouldBe 0

    AlleleCount(0, 1).total shouldBe 1
    AlleleCount(0, 1).forwardReads shouldBe 0
    AlleleCount(0, 1).reverseReads shouldBe 1
    AlleleCount(0, 1).forwardUmi shouldBe 0
    AlleleCount(0, 1).reverseUmi shouldBe 1

    AlleleCount(1, 1).total shouldBe 2
    AlleleCount(1, 1).forwardReads shouldBe 1
    AlleleCount(1, 1).reverseReads shouldBe 1
    AlleleCount(1, 1).forwardUmi shouldBe 1
    AlleleCount(1, 1).reverseUmi shouldBe 1
  }

  @Test
  def testPlus(): Unit = {
    val a1 = AlleleCount(1, 2, 3, 4)
    val a2 = AlleleCount(4, 3, 2, 1)

    val c = a1 + a2

    c.forwardUmi shouldBe 5
    c.forwardReads shouldBe 5
    c.reverseUmi shouldBe 5
    c.reverseReads shouldBe 5

    c.total shouldBe 10
    c.totalReads shouldBe 10
  }
}
