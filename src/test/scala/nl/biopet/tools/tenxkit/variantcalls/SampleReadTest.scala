package nl.biopet.tools.tenxkit.variantcalls

import nl.biopet.test.BiopetTest
import org.testng.annotations.Test

class SampleReadTest extends BiopetTest {
  @Test
  def test(): Unit = {
    val read = SampleRead("name", "chr1", 10, 20, "AATTCCGGAA", "AAAAAAAAAA", "3M1D3M1I3M", strand = true)
    read.sampleBases.map(_.allele).mkString shouldBe read.sequence
    read.sampleBases.size shouldBe 10
    read.sampleBases(2).delBases shouldBe 1
    read.sampleBases(6).allele shouldBe "CG"
  }
}
