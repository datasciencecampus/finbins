package uk.gov.ons.dsc.utils.stringmetric

import scala.collection.immutable.Map

sealed trait AlgorithmDecorator[A] {
	val withMemoization: Algorithm[A]

	val withTransform: (Transform[A] => Algorithm[A])
}


final case class StringAlgorithmDecorator(sa: StringAlgorithm) extends AlgorithmDecorator[Array[Char]] {
	override val withMemoization: StringAlgorithm = new StringAlgorithm {
		private val base: StringAlgorithm = sa
		private var memo: Map[String, Option[String]] = Map()

		override def compute(a: Array[Char]): Option[Array[Char]] = compute(a.toString).map(_.toCharArray)

		override def compute(a: String): Option[String] =
			if (memo.contains(a)) memo(a)
			else {
				memo = memo + (a -> base.compute(a))
				memo(a)
			}
	}

	override val withTransform: (StringTransform => StringAlgorithm) = (st) => new StringAlgorithm {
		private val base: StringAlgorithm = sa
		private val transform: StringTransform = st

		override def compute(a: Array[Char]): Option[Array[Char]] = base.compute(transform(a))

		override def compute(a: String): Option[String] = compute(a.toCharArray).map(_.mkString)
	}
}
