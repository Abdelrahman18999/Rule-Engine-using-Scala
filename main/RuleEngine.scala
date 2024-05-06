import java.sql.Date
import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import scala.io.{BufferedSource, Source}

object RuleEngine extends App {

  // Load the Oracle JDBC driver
  Class.forName("oracle.jdbc.driver.OracleDriver")
  // Connection URL
  val url = "jdbc:oracle:thin:@//localhost:1521/XE"
  // Connect to the database
  val connection: Connection = DriverManager.getConnection(url, "BI", "123")


  // ######################## Logging #####################################
  // Open log file for writing
  val logFile = new File("rules_engine.log")
  val writer = new BufferedWriter(new FileWriter(logFile, true))

  def logEvent(logLevel: String, message: String): Unit = {
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val logMessage = s"$timestamp $logLevel $message"
    writer.write(logMessage + "\n")
    writer.flush()
  }

  // Log the start of the application
  logEvent("INFO", "RuleEngine started.")

  logEvent("INFO", "Start Reading the retail_store csv file ...")
  val source: BufferedSource = Source.fromFile("src/main/resources/retail_store.csv")
  val lines: List[String] = source.getLines().drop(1).toList // drop header
  logEvent("INFO", "Successfully Reading the retail_store csv file.")

  case class Order(timestamp: String, product_name: String, expire_date: String, quantity: Int,
                   unit_price: Double, channel: String, payment_method: String)


  // 1. Day Remaining Qualifying Rule
  // Day Remaining Qualifier
  def days_remaining_qualifier(timestamp: String, expireDate: String): Int = {
    // get only the date part from timestamp column
    val timeStampDate = timestamp.take(10)

    // Specifying the format of sold date and expire date as they are in the csv file
    val soldDate_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val expireDate_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    // convert them to actual date so I can perform date function on them
    val soldDate = LocalDate.parse(timeStampDate, soldDate_formatter)
    val expireDateParsed = LocalDate.parse(expireDate, expireDate_formatter)

    // Calculate the days remaining from the current date to the expiration date
    val currentDate = LocalDate.now()
    val daysRemaining = ChronoUnit.DAYS.between(soldDate, expireDateParsed)

    // Now let's apply the quality check (if the days between them less than 30 days or not)
    if (daysRemaining < 30) daysRemaining.toInt else 0
  }

  // Day Remaining Calculator of Discount Value
  def days_remaining_discount_calculator(days_remaining_qualifier: Int): Double = {
    if (days_remaining_qualifier == 0) 0.0 else (30.0 - days_remaining_qualifier) / 100.0
  }

  // Days Remaining Calculator of Discount Percentage
  def days_remaining_discount_percentage(days_remaining_qualifier: Int): Int = {
    if (days_remaining_qualifier == 0) 0 else (30 - days_remaining_qualifier)
  }

  // ******************************************************************************************
  // ******************************************************************************************

  // 2. On-Sale Products Qualifying Rule
  // On-Sale Qualifier and calculator
  def onSaleProducts_qualifier(productName: String): Double = {
    // prepare the pattern of wine and cheese products names
    val wine_pattern = """Wine - .+""".r
    val cheese_pattern = """Cheese - .+""".r

    // check if the product is a wine or cheese using regex pattern
    if (wine_pattern.matches(productName)) 0.1 // Discount for wine products
    else if (cheese_pattern.matches(productName)) 0.05 // Discount for cheese products
    else 0.0 // otherwise products have no discount
  }

  // ******************************************************************************************
  // ******************************************************************************************

  // 3. Special Discount for products which sold in 23th of March
  def marchSpecialDiscount(soldTimestamp: String): Double = {
    val timeStampDate = soldTimestamp.take(10)
    val soldDate_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val soldDate = LocalDate.parse(timeStampDate, soldDate_formatter)

    if (soldDate.getDayOfMonth == 23 && soldDate.getMonthValue == 3) 0.5
    else 0.0
  }

  // ******************************************************************************************
  // ******************************************************************************************

  // 4. Quatity of the product sold
  def quantitySoldDiscount(quantitySold: Int): Double = {
    if ((quantitySold >= 6) & (quantitySold <= 9)) 0.05
    else if ((quantitySold >= 10) & (quantitySold <= 14)) 0.07
    else if (quantitySold >= 15) 0.1
    else 0.0
  }

  // ******************************************************************************************
  // ******************************************************************************************


  val resultsList: List[List[Double]] = lines.map { line =>
    val Array(timestamp, product_name, expireDate, quantity, unit_price, channel, payment_method) = line.split(",")

    // Apply your discount functions to get a list of discounts for this line
    logEvent("INFO", "RuleEngine started checking days remaining qualification ...")
    val daysRemaining = days_remaining_qualifier(timestamp, expireDate)
    logEvent("INFO", "RuleEngine completed checking days remaining qualification.")

    logEvent("INFO", "RuleEngine started applying days remaining discount calculation ...")
    val daysRemainingDiscount = days_remaining_discount_calculator(daysRemaining)
    logEvent("INFO", "RuleEngine completed days remaining discount calculation.")

    logEvent("INFO", "RuleEngine started checking the on-sale products (wine/cheese) qualification ...")
    logEvent("INFO", "RuleEngine started checking the on-sale products (wine/cheese) calculation.")
    val onSaleDiscount = onSaleProducts_qualifier(product_name)
    logEvent("INFO", "RuleEngine completed checking the on-sale products (wine/cheese) calculation.")

    logEvent("INFO", "RuleEngine started checking the 23th of March qualification ...")
    logEvent("INFO", "RuleEngine started checking the 23th of March calculation.")
    val marchDiscount = marchSpecialDiscount(timestamp)
    logEvent("INFO", "RuleEngine completed checking the 23th of March calculation.")

    logEvent("INFO", "RuleEngine started checking the quantity sold qualification ...")
    logEvent("INFO", "RuleEngine started checking the quantity sold calculation.")
    val quantitySoldDiscountValue = quantitySoldDiscount(quantity.toInt)
    logEvent("INFO", "RuleEngine completed checking the quantity sold calculation.")

    List(daysRemainingDiscount, onSaleDiscount, marchDiscount, quantitySoldDiscountValue)
  }
  logEvent("INFO", "RuleEngine created resultList of the available discount qualifications for each product.")

  // Filter the lists of each product to include only the products that meet any qualifiying rule
  logEvent("INFO", "RuleEngine started calculating the average of the top two discounts for each product discount list ...")
  val avgResultList = resultsList.map { discounts =>
    val nonZeroDiscounts = discounts.filter(_ != 0.0)
    nonZeroDiscounts.sorted match {
      case x :: y :: _ => (x + y) / 2
      case x :: Nil => x
      case Nil => 0.0
    }
  }
  logEvent("INFO", "RuleEngine completed calculating the average of the top two discounts for each product discount list.")


  // Define SQL query to insert data
  val insertDataQuery = """
    |INSERT INTO RETAILSTORE (sold_date, product_name, expiry_date, quantity,
    |                   unit_price, channel, payment_method, final_price)
    |VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    |""".stripMargin


  val insertDataStatement = connection.prepareStatement(insertDataQuery)

  lines.zip(avgResultList).foreach { case (line, finalDiscount) =>
    val Array(timestamp, product_name, expireDate, quantity, unit_price, channel, payment_method) = line.split(",")

    // Ensure finalDiscount is not 0 before calculating finalPrice
    val finalPrice = (unit_price.toDouble * quantity.toInt) - (unit_price.toDouble * quantity.toInt * finalDiscount)

    val Date_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val timeStampDate = timestamp.take(10)
    val expireDate_ = expireDate.take(10)

    val soldDate = LocalDate.parse(timeStampDate, Date_formatter)
    val expireDateParsed = LocalDate.parse(expireDate_, Date_formatter)

    insertDataStatement.setDate(1, java.sql.Date.valueOf(soldDate.toString))
    insertDataStatement.setString(2, product_name)
    insertDataStatement.setDate(3, java.sql.Date.valueOf(expireDateParsed.toString))
    insertDataStatement.setInt(4, quantity.toInt)
    insertDataStatement.setDouble(5, unit_price.toDouble)
    insertDataStatement.setString(6, channel)
    insertDataStatement.setString(7, payment_method)
    insertDataStatement.setDouble(8, finalPrice)

    insertDataStatement.executeUpdate()
    logEvent("INFO", s"Inserted record for $product_name with final price $finalPrice")
  }


  // Log the end of the application
  logEvent("INFO", "RuleEngine completed")

  insertDataStatement.close()
  source.close()
  connection.close()
  writer.close()

}
