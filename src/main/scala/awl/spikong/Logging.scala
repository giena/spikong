/* Copyright (C) Atos SE, Siemens AG 2015 All Rights Reserved Confidential */

package awl.spikong

import akka.actor.ActorSystem
import ch.qos.logback.classic.{Level, LoggerContext}
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

/**
 * Created with IntelliJ IDEA.
 * User: a203673
 * Date: 26/09/14
 * Time: 17:51
 */
/**
 * Play-like configuration of logback
 */
object Logging {

    def configure(configuration: Config): Unit = {
        import scala.collection.JavaConverters._

        if (configuration.hasPath("logger")) {
            val ctx = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
            val loggerConfig = configuration.getConfig("logger")
            loggerConfig.entrySet().asScala.foreach { entry ⇒
                ctx.getLogger(entry.getKey).setLevel(Level.toLevel(loggerConfig.getString(entry.getKey)))
            }
        }
    }

    // Give a chance (mainly for tests) to override classpath application
    // configuration before starting log system
    protected var logSystemConfig: Config = _

    // See: http://stackoverflow.com/a/18982346
    // Note: we want the unified logger to be daemonic
    // XXX - use a dedicated Config section ?
    lazy val logSystem = ActorSystem(
        "LogSystem",
        ConfigFactory.parseString("akka.daemonic = true").withFallback(
            logSystemConfig
        )
    )

    trait UnifiedLogger {
        val log = akka.event.Logging.getLogger(logSystem, this)
    }

}
