package nl.biopet.tools.tenxkit.variantcalls

import nl.biopet.test.BiopetTest
import org.testng.annotations.Test

class SampleReadTest extends BiopetTest {
  @Test
  def test(): Unit = {
    val bases = SampleRead.sampleBases(1,
                                       10,
                                       1,
                                       true,
                                       "AATTCCGGAA".getBytes,
                                       "AAAAAAAAAA".getBytes,
                                       "3M1D3M1I3M")
    bases.map(_._2.allele).mkString shouldBe "AATTCCGGAA"
    bases.size shouldBe 10
    bases(2)._2.delBases shouldBe 1
    bases(6)._2.allele shouldBe "CG"
  }
}
