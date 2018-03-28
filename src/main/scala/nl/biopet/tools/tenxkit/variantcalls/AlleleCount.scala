package nl.biopet.tools.tenxkit.variantcalls

case class AlleleCount(forwardUmi: Int = 0,
                       reverseUmi: Int = 0,
                       forwardReads: Int = 0,
                       reverseReads: Int = 0) {
  def total: Int = forwardUmi + reverseUmi
  def totalReads: Int = forwardReads + reverseReads

  def +(other: AlleleCount): AlleleCount = {
    AlleleCount(this.forwardUmi + other.forwardUmi,
                this.reverseUmi + other.reverseUmi,
                this.forwardReads + other.forwardReads,
                this.reverseReads + other.reverseReads)
  }
}
