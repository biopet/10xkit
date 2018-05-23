package nl.biopet.tools.tenxkit

import nl.biopet.test.BiopetTest
import nl.biopet.tools.tenxkit.variantcalls.AlleleCount
import org.testng.annotations.Test

class GroupCallTest extends BiopetTest {
  @Test
  def testBasicAlleleMethods(): Unit = {
    val groupsMap = Map(0 -> "g2", 1 -> "g1", 2 -> "g2")
    val v1 = VariantCall(
      0,
      1000,
      "A",
      IndexedSeq("G"),
      Map(0 -> IndexedSeq(AlleleCount(10), AlleleCount(0)),
          1 -> IndexedSeq(AlleleCount(5), AlleleCount(5)),
          2 -> IndexedSeq(AlleleCount(0), AlleleCount(10)))
    )
    val g1 = v1.toGroupCall(groupsMap)

    g1.allAlleles shouldBe Array("A", "G")
    g1.alleleDepth shouldBe Seq(15, 15)
    g1.alleleReadDepth shouldBe Seq(15, 15)
  }

  @Test
  def testBasicTotalMethods(): Unit = {
    val groupsMap = Map(0 -> "g2", 1 -> "g1", 2 -> "g2")
    val v1 = VariantCall(
      0,
      1000,
      "A",
      IndexedSeq("G"),
      Map(0 -> IndexedSeq(AlleleCount(10), AlleleCount(0)),
          1 -> IndexedSeq(AlleleCount(5), AlleleCount(5)),
          2 -> IndexedSeq(AlleleCount(0), AlleleCount(10)))
    )
    val g1 = v1.toGroupCall(groupsMap)

    g1.altDepth shouldBe 15
    g1.referenceDepth shouldBe 15
    g1.totalDepth shouldBe 30
    g1.totalReadDepth shouldBe 30
    g1.hasNonReference shouldBe true
    g1.totalAltRatio shouldBe 0.5
  }
  @Test
  def testCreate(): Unit = {
    val groupsMap = Map(0 -> "g2", 1 -> "g1", 2 -> "g2")
    val v1 = VariantCall(
      0,
      1000,
      "A",
      IndexedSeq("G"),
      Map(0 -> IndexedSeq(AlleleCount(10), AlleleCount(0)),
          1 -> IndexedSeq(AlleleCount(5), AlleleCount(5)),
          2 -> IndexedSeq(AlleleCount(0), AlleleCount(10)))
    )
    val g1 = v1.toGroupCall(groupsMap)
    g1 shouldBe GroupCall(
      v1.contig,
      v1.pos,
      v1.refAllele,
      v1.altAlleles,
      Map("g1" -> IndexedSeq(AlleleCount(5, 0, 5, 0), AlleleCount(5, 0, 5, 0)),
          "g2" -> IndexedSeq(AlleleCount(10, 0, 10, 0),
                             AlleleCount(10, 0, 10, 0))),
      Map("g1" -> GenotypeCall.fromAd(IndexedSeq(5, 5)),
          "g2" -> GenotypeCall.fromAd(IndexedSeq(10, 10))),
      Map("g1" -> 1, "g2" -> 2)
    )
  }
}
