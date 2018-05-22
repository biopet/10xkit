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
    val a1 = AlleleCount(1,2,3,4)
    val a2 = AlleleCount(4,3,2,1)

    val c = a1 + a2

    c.forwardUmi shouldBe 5
    c.forwardReads shouldBe 5
    c.reverseUmi shouldBe 5
    c.reverseReads shouldBe 5

    c.total shouldBe 10
    c.totalReads shouldBe 10
  }
}
