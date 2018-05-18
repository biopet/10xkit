package nl.biopet.tools.tenxkit

import nl.biopet.test.BiopetTest
import nl.biopet.tools.tenxkit.variantcalls.AlleleCount
import org.testng.annotations.Test

class VariantCallTest extends BiopetTest {
  @Test
  def testUniqueAlleles(): Unit = {
    val call1 = VariantCall(1, 1, "A", Array("C"), Map(1 -> Array(AlleleCount(2), AlleleCount(2)), 2 -> Array(AlleleCount(2), AlleleCount(2))))
    call1.getUniqueAlleles(Map(1 -> "group1", 2 -> "group2")) shouldBe empty

    val call2 = VariantCall(1, 1, "A", Array("C"), Map(1 -> Array(AlleleCount(2), AlleleCount(2)), 2 -> Array(AlleleCount(), AlleleCount(2))))
    call2.getUniqueAlleles(Map(1 -> "group1", 2 -> "group2")) shouldBe Array("group1" -> "A")
  }
}
