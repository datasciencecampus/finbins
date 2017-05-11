package uk.gov.ons.dsc.utils.stringmetric

trait Algorithm[A] {
	def compute(a: A): Option[A]
}
