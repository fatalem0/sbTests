package task1.excel

import org.apache.spark.sql.SparkSession

import java.util

/*
  пример через работу с Excel
  для запуска в конфигурации используем SDK для Java 11
 */
object ReturnProperIds {
  /** Метод, возвращающий список всех id, подходящих под определение вставки задним числом.
   * Вставка задним числом - операция вставки записи в таблицу, при которой:
   * ID(n) > ID(n-1)
   * Timestamp(n) < max(Timestamp(1):Timestamp(n-1))
   *
   * @param spark объект спарк-сессии
   * @param path  путь до файла
   * @return список всех id, подходящих под определение вставки задним числом
   */
  def findProperIds(spark: SparkSession, path: String): util.List[String] = {
    import spark.implicits._

    val df = spark.read
      .format("excel")
      .option("header", "true")
      .load(path)

    df.createOrReplaceTempView("timestamps")

    val query =
      """
        |SELECT DISTINCT t1.Id
        |FROM timestamps t1
        |INNER JOIN (SELECT Id,
        |            Timestamp
        |            FROM timestamps) t2
        |ON t1.Id > t2.Id AND t1.Timestamp < t2.Timestamp;
        |""".stripMargin

    spark.sql(query).map(row => row.mkString("")).collectAsList()
  }

  def main(args: Array[String]): Unit = {
    val xlsxFilePath = "src/main/scala/task1/excel/task1.xlsx"
    val spark = SparkSession
      .builder()
      .appName("Return Proper Ids")
      .master("local[*]")
      .getOrCreate()

    println(findProperIds(spark, xlsxFilePath))

    spark.close()
  }
}
