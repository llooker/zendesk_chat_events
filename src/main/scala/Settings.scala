import com.typesafe.config.Config

class Settings(config: Config) {
  val key = config.getString("aws.key")
  val secret = config.getString("aws.secret")
  val source_bucket = config.getString("aws.source_bucket")
  val destination_bucket = config.getString("aws.destination_bucket")
}