// preliminaries
import java.text.SimpleDateFormat
import java.util.regex.Matcher
import java.util.regex.Pattern
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.parsing.json.JSONObject
import java.util.Calendar
import com.typesafe.config.ConfigFactory


object chatEvent {

 def main(args: Array[String]) {

  // class for our chat
  case class chat (
   ticket_id: String,
   chat_time: String,
   body: String
  )

  // fire up spark context
  val conf = new SparkConf().setAppName("Chat Events")
  val sc = new SparkContext(conf)
  val config = new Settings(ConfigFactory.load())

  // load AWS credentials and S3 locations
  val aws_key = config.key
  val aws_secret = config.secret
  val aws_source_bucket = config.source_bucket
  val aws_destination_bucket = config.destination_bucket

  // regex to match line objects
  val chatMatch = "([a-zA-Z0-9]{18,20}).*([0-9]{4}-[0-9]{2}-[0-9]{2}\\s[0-9]{2}:[0-9]{2}:[0-9]{2}).*(\"(?: Location|Chat|Conversation).*)".r.unanchored

  // regex to match embedded chat times
  val chatTimePattern = """\([0-9]{2}:[0-9]{2}:[0-9]{2} [A-Z]{2}\)""".r

  // fire up calendar
  val cal = Calendar.getInstance()

  // input chat-time pattern
  val fromEventTimePattern = new java.text.SimpleDateFormat("(h:m:s a)")

  // output chat-time pattern
  val toEventTimePattern = new java.text.SimpleDateFormat("H:m:s")

  // regex to match the name of chat participants
  val nameMatch = "([A-Z]{1}[a-z]+\\s[A-Za-z-]+\\s[A-Z]{1}[a-z]+\\:|[A-Z]{1}[a-z]+\\s[A-Za-z]+\\:|[A-Z]{1}[a-z]+\\:|[A-Z]{2}\\:)".r.unanchored

  // input time pattern
  val inputDateFormat = new java.text.SimpleDateFormat("yyyy-M-d H:m:s")

  // output time pattern
  val outputDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")

  // function to parse raw chat lines and stuff components into string array
  def parseChat (line: String): Option[chat] = {
   line match { 
    case chatMatch(ticket_id, chat_time, body) => return Option(chat(ticket_id, chat_time, body))
    case _ => None
   }
  }

  def incrementDay(chatStartTime: String, chatTimes: Array[String]): Array[String] = {
    var timeDifferences = chatTimes.toList.sliding(2).map { case Seq(x, y, _*) => toEventTimePattern.parse(y).getTime() - toEventTimePattern.parse(x).getTime() }
    var incrementPosition = timeDifferences.indexWhere(x => x < 0) + 1
    if (incrementPosition > 0) {
      cal.setTime(outputDateFormat.parse(chatStartTime))
      cal.add(Calendar.DATE, 1)
      var incrementedDate = outputDateFormat.format(cal.getTime())
      var remainingFill = chatTimes.length - incrementPosition
      var days = (List.fill(incrementPosition)(chatStartTime)) ++ List.fill(remainingFill)(incrementedDate)
      return List.map2(days, chatTimes.toList) (_ + " " + _).toArray
    } else {
      var days = List.fill(chatTimes.length)(chatStartTime)
      return List.map2(days, chatTimes.toList) (_ + " " + _).toArray
    }
  }

  // function to parse embedded times from chat dialogue
  def parseTimes (line: chat): Array[String] = {
   var chatStartTime = outputDateFormat.format(inputDateFormat.parse(line.chat_time))
   var rawTimes = chatTimePattern.findAllIn(line.body)
   var needDateMath = rawTimes.toArray.map(e => toEventTimePattern.format(fromEventTimePattern.parse(e)))
   return incrementDay(chatStartTime, needDateMath)
  }


  // function to split the raw chat dialong into a sequence of events
  def splitChat (line: chat): Array[String] = {
   var rawChats = line.body.split(chatTimePattern.toString).drop(1)
   return rawChats
  }

  def parseNames (line: String): Option[String] = {
   line match { 
    case nameMatch(name) => return Some(name)
    case _ => None
   }
  }

  // function to repeat the ticket_id n times for n chat events
  def fanIds (line: chat): Array[String] = {
   var numIds = chatTimePattern.findAllIn(line.body).length
   return Array.fill(numIds)(line.ticket_id)
  }

  // function to take data and stuff it into a hash map
  def createMap (line: (((String, String), String), String)): Map[String, String] = {
   return Map("ticket_id" -> line._1._1._1, "created_at" -> line._1._1._2, "participant" -> line._2, "body" -> line._1._2)
  }

  // go-do function
  def run (line: String): Option[Array[scala.util.parsing.json.JSONObject]] = {
   var parsedLine = parseChat(line)
   if (parsedLine.nonEmpty) {
    var times = parseTimes(parsedLine.get)
    var chats = splitChat(parsedLine.get)
    var ids = fanIds(parsedLine.get)
    var names = chats.map(x => parseNames(x)).map(x => x.getOrElse(":").replaceAll("\\:", ""))
    var cleanedChats = chats.map(x => x.replaceAll(nameMatch.toString, "")).map(y => y.replaceAll("(\\\\n|\\\n)", ""))
    var nestedInformation = ids zip times zip cleanedChats zip names
    var jsonValues = nestedInformation.map(x => createMap(x)).filter(x => !x.values.exists(_ == "")).map(y => scala.util.parsing.json.JSONObject(y))
    return Some(jsonValues)
   }
    None
  }

  // pull in the raw data
  val hadoopConf = sc.hadoopConfiguration
  hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
  hadoopConf.set("fs.s3n.awsAccessKeyId", aws_key)
  hadoopConf.set("fs.s3n.awsSecretAccessKey", aws_secret)
  val tickets = sc.textFile(aws_source_bucket)

  // parse tickets
  var parsedTickets = tickets.filter(line => line.nonEmpty).map(line => run(line))
  parsedTickets.filter(line => line.nonEmpty).flatMap(x => x.get).saveAsTextFile(aws_destination_bucket)
 
 }

}