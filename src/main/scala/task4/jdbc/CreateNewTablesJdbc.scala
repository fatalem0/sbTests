package task4.jdbc

import org.apache.spark.sql.functions.typedlit
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/*
  пример через работу с JDBC
  перед запуском нужно сконнектиться с базой и запустить init.sql для инициализации таблиц
  для запуска в конфигурации используем SDK для Java 11
 */
object CreateNewTablesJdbc {
  private val dtf = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss")
  private val now = LocalDateTime.now

  /** Метод создает представления таблиц calls и dates
   *
   * @param spark объект спарк-сессии
   */
  def initTables(spark: SparkSession): Unit = {
    val callsTable = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/sberbank_tests")
      .option("dbtable", "calls")
      .option("user", "postgres")
      .option("driver", "org.postgresql.Driver")
      .load()

    callsTable.createOrReplaceTempView("calls_jdbc")

    val datesTable = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/sberbank_tests")
      .option("dbtable", "dates")
      .option("user", "postgres")
      .option("driver", "org.postgresql.Driver")
      .load()

    datesTable.createOrReplaceTempView("dates_jdbc")
  }

  /** Метод обновляет данные в таблице stat_total
   *
   * @param spark объект спарк-сессии
   */
  def updateStatTotalTable(spark: SparkSession): Unit = {
    val countQuery =
      """
        |SELECT calls_jdbc.date, COUNT(calls_jdbc.date) as count
        |FROM calls_jdbc, dates_jdbc
        |WHERE calls_jdbc.date = dates_jdbc.date
        |GROUP BY calls_jdbc.date
        |ORDER BY calls_jdbc.date DESC;
        |""".stripMargin

    val countDates = spark.sql(countQuery)
    val finalTable = countDates.withColumn("stat_date_time", typedlit(dtf.format(now)))

    finalTable.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/sberbank_tests")
      .option("dbtable", "stat_total")
      .option("user", "postgres")
      .mode(SaveMode.Overwrite)
      .save()
  }

  /** Метод обновляет данные в таблице stat_oper
   *
   * @param spark объект спарк-сессии
   */
  def updateStatOperTable(spark: SparkSession): Unit = {
    val countQuery =
      """
        |SELECT calls_jdbc.date, oper_id, COUNT(*) AS count
        |FROM calls_jdbc, dates_jdbc
        |WHERE calls_jdbc.date = dates_jdbc.date
        |GROUP BY calls_jdbc.date, oper_id
        |ORDER BY calls_jdbc.date DESC, oper_id DESC;
        |""".stripMargin

    val countDatesForOpers = spark.sql(countQuery)
    val finalTable = countDatesForOpers.withColumn("stat_date_time", typedlit(dtf.format(now)))

    finalTable.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/sberbank_tests")
      .option("dbtable", "stat_oper")
      .option("user", "postgres")
      .mode(SaveMode.Overwrite)
      .save()
  }

  /** Метод очищает таблицу dates после создания таблиц stat_total и stat_oper
   *
   * @param spark объект спарк-сессии
   */
  def clearDatesTable(spark: SparkSession): Unit = {
    val datesTable = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/sberbank_tests")
      .option("dbtable", "dates")
      .option("user", "postgres")
      .option("driver", "org.postgresql.Driver")
      .load()

    val schema = datesTable.schema
    val emptyDatesTable = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    emptyDatesTable.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/sberbank_tests")
      .option("dbtable", "dates")
      .option("user", "postgres")
      .mode(SaveMode.Overwrite)
      .save()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Create New Tables JDBC")
      .master("local[*]")
      .getOrCreate()

    initTables(spark)
    updateStatTotalTable(spark)
    updateStatOperTable(spark)
    clearDatesTable(spark)

    spark.close()
  }
}
