package uk.gov.ons.dsc.utils.stringmetric

trait Tokenizer[A] {
	def tokenize(a: A): Option[Array[A]]
}
