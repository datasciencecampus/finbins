package uk.gov.ons.dsc.utils.stringmetric

trait Metric[A, B] {
	def compare(a: A, b: A): Option[B]
}
