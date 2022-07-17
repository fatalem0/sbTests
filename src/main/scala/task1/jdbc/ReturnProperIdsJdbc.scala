package task1.jdbc

import org.apache.spark.sql.SparkSession

import java.util

/*
  пример через работу с JDBC
  перед запуском нужно сконнектиться с базой и запустить init.sql для инициализации таблицы
  для запуска в конфигурации используем SDK для Java 11
 */
object ReturnProperIdsJdbc {
  /** Метод, возвращающий список всех id, подходящих под определение вставки задним числом.
   * Вставка задним числом - операция вставки записи в таблицу, при которой:
   * ID(n) > ID(n-1)
   * Timestamp(n) < max(Timestamp(1):Timestamp(n-1))
   *
   * @param spark объект спарк-сессии
   * @return список всех id, подходящих под определение вставки задним числом
   */
  def findProperIdsJdbc(spark: SparkSession): util.List[String] = {
    import spark.implicits._

    val jdbcDf = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/sberbank_tests")
      .option("dbtable", "timestamps")
      .option("user", "postgres")
      .option("driver", "org.postgresql.Driver")
      .load()

    jdbcDf.createOrReplaceTempView("jdbc_timestamps")

    val query =
      """
        |SELECT DISTINCT t1.Id
        |FROM jdbc_timestamps t1
        |INNER JOIN (SELECT Id,
        |            Timestamp
        |            FROM jdbc_timestamps) t2
        |ON t1.Id > t2.Id AND t1.Timestamp < t2.Timestamp
        |ORDER BY t1.Id;
        |""".stripMargin

    spark.sql(query).map(row => row.mkString("")).sort().collectAsList()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Return Proper Ids Jdbc")
      .master("local[*]")
      .getOrCreate()

    println(findProperIdsJdbc(spark))

    spark.close()
  }
}
