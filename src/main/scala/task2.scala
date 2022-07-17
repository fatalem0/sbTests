/**
  *  Есть список из интов. Подсчитать количество перемен знака в последовательности (ноль не перемена)
  *  Для List(0, 0, 1, 0, 0, 1, 0, 1) - 0 перемен знака
  *  Для List(1, 0 , -1)              - 1 перемена
  *  Для List(1, 0 , -1, 0, 0, 0, -1) - тоже 1 перемена
  */
object PositiveNegativeCount extends App {
  /** Функция для подсчета количества перемен знака
   *
   *   Функция работает следующим образом:
   *   1) Сначала убираем нули в списке
   *   2) Последовательно разбиваем новый список на пары (например, для in - (1, -2), (-2, 3), (3, 3) и тд)
   *   3) Подсчитываем количество пар, где произошла перемена знака
   *
   * @param ints список из интов
   * @return количество перемен знака
   */
  def countTransactions(ints: List[Int]): Int = {
    val filteredInts = ints.filter(_ != 0)
    filteredInts
      .zip(filteredInts.tail)
      .count((x: (Int, Int)) => x._1 > 0 && x._2 < 0 || x._1 < 0 && x._2 > 0)
  }

  val in = List(1,-2,3,3,4,-2,-2,-3,1)
  println(s"Количество изменений знака в последовательности $in: ${countTransactions(in)}")

  val first = List(0, 0, 1, 0, 0, 1, 0, 1)
  println(s"Количество изменений знака в последовательности $first: ${countTransactions(first)}")

  val second = List(1, 0 , -1)
  println(s"Количество изменений знака в последовательности $second: ${countTransactions(second)}")

  val third = List(1, 0 , -1, 0, 0, 0, -1)
  println(s"Количество изменений знака в последовательности $third: ${countTransactions(third)}")
}