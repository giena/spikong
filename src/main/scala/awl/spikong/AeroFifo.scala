package awl.spikong

import com.typesafe.config.Config
import java.util.concurrent.Executors

import scala.concurrent.Await
import com.tapad.aerospike.AerospikeClient
import java.nio.ByteBuffer

import scala.concurrent.Future
import java.util.concurrent.ThreadFactory

import scala.concurrent.duration.Duration
import scala.collection.JavaConverters._
import com.tapad.aerospike.DefaultValueMappings._

import scala.concurrent.ExecutionContext.Implicits.global
import com.tapad.aerospike.ClientSettings
import com.tapad.aerospike.WriteSettings
import awl.spikong.Logging.UnifiedLogger

import scala.util.Success

/*
 * A class for the metadata:
 * The queue name (qName) is the key
 * There's a head where we can read data
 * 		It is increased when we remove an element
 * There's a tail where we can write data
 * 		It is decreased when we add an element
 * There's potentially a maximum size. by default there's no limit (0)
 * Creation of the Fifo
 * Last Update of the Fifo
 */
case class Metadata(qName: String, head: Long = 0, tail: Long = 0, maxSize: Int = 0, tsCreation: Long, tsLastUpdate: Long, tsLastRead: Long, ttl: Int)

/*
 * Some specific exceptions
 */
class AlreadyExistingQueueException(qName: String) extends Exception
class NotExistingQueueException(qName: String) extends Exception
class NotDataFoundException(qName: String) extends Exception
class ReachMaxSizeException(qName: String) extends Exception
class EmptyQueueException(qName: String) extends Exception

/**
 * @author a140168
 * Singleton to use only one connections pool to access Aerospike
 */
object AsClient {
    private var instance: AerospikeClient = null

    def getInstance(implicit config: AeroFifoConfig) = {
        synchronized {
            if (instance == null) {
                val asyncTaskThreadPool = Executors.newCachedThreadPool(new ThreadFactory() {
                    override def newThread(runnable: Runnable) = {
                        val thread = new Thread(runnable)
                        thread.setDaemon(true)
                        thread
                    }
                })
                instance = AerospikeClient(config.urls.asScala, new ClientSettings(maxCommandsOutstanding = config.maxCommandsOutstanding, blockingMode = true, selectorThreads = config.selectorThreads, taskThreadPool = asyncTaskThreadPool))
            }
            instance
        }
    }

    def long2bytearray(lng: Long): Array[Byte] = {
        val bb = ByteBuffer.allocate(8) //8, In java Long is 64bits
        bb.putLong(lng)
        bb.array()
    }

    def int2bytearray(i: Int): Array[Byte] = {
        val bb = ByteBuffer.allocate(4) //8, In java Long is 32bits
        bb.putInt(i)
        bb.array()
    }

    def bytearray2long(ba: Array[Byte]): Long = {
        val bb = ByteBuffer.wrap(ba)
        bb.getLong
    }

    def bytearray2int(ba: Array[Byte]): Int = {
        val bb = ByteBuffer.wrap(ba)
        bb.getInt
    }
}

/**
 * Akka configuration
 */
class AeroFifoConfig(tsconfig: Config) {
    //AeroSpike urls to connect to
    val urls = tsconfig.getStringList("urls")
    //AeroSpike namespace containing the fifos
    val namespace = tsconfig.getString("namespace")
    //AeroSpike set containing the metadata
    val metadataset = tsconfig.getString("metadataset")
    //AeroSpike set containing the fifos
    val messageset = tsconfig.getString("messageset")
    //Aerospike selector number of threads
    val selectorThreads = tsconfig.getInt("selectorThreads")
    //Consistency level
    val commitAll = tsconfig.getBoolean("commitAll")
    //Producer and consumer are not on the same jvm.
    val localOnly = tsconfig.getBoolean("localOnly")
    //the maximum number of outstanding commands before rejections will happen
    val maxCommandsOutstanding = tsconfig.getInt("maxCommandsOutstanding")
    //the ttl of the polled data in seconds
    val pollTtl = tsconfig.getInt("pollTtl")
}

/**
 * @author a140168
 * Aerospike implementation of a Fifo
 */
class AeroFifo(qName: String, tsconfig: Config) extends UnifiedLogger {

    implicit val config = new AeroFifoConfig(tsconfig)

    var metacache: Metadata = null

    val wp = WriteSettings(commitAll = config.commitAll)
    val namespace = AsClient.getInstance.namespace(config.namespace, writeSettings = wp)
    val metadataset = namespace.set[String, Array[Byte]](config.metadataset) //We hope that this Set should always be in the buffer cache 
    val messageset = namespace.set[String, Array[Byte]](config.messageset)
    /* NB: The buffer cache does not work with the SSD aerospike driver. 
   * So we have to store data in memory and on disk if we use the SSD aerospike driver  
   */

    def initialize(): Unit = {
        Await.result(readMetadata(), Duration.Inf)
    }

    def destroy(): Unit = {}

    /**
     * create a queue asynchronously.
     * update the metadata in store and in cache
     */
    def createQueue(qSize: Int = 0, ttl: Option[Int] = None): Future[Unit] = {
        if (metacache != null) {
            Future.failed(new AlreadyExistingQueueException(qName))
        } else {
            val now = System.currentTimeMillis()
            val md = new Metadata(qName, 0, 0, qSize, now, now, 0L, ttl.getOrElse(0))
            saveMetadata(md)
        }
    }

    /**
     * save the metadata in store and in cache for a queue
     */
    private def saveMetadata(metadata: Metadata): Future[Unit] = {
        val bins: Map[String, Array[Byte]] = Map(
            "head" → AsClient.long2bytearray(metadata.head),
            "tail" → AsClient.long2bytearray(metadata.tail),
            "maxsize" → AsClient.int2bytearray(metadata.maxSize),
            "tsc" → AsClient.long2bytearray(metadata.tsCreation),
            "tslu" → AsClient.long2bytearray(metadata.tsLastUpdate),
            "tslr" → AsClient.long2bytearray(metadata.tsLastRead),
            "ttl" → AsClient.int2bytearray(metadata.ttl)
        )

        log.debug(s"Trying to save metadata :$metadata")
        metadataset.putBins(metadata.qName, bins, Option(-1)) map {
            case _ ⇒
                metacache = metadata
        }
    }

    /**
     * save the head metadata in store and in cache for a queue
     */
    private def saveMetadataHead(metadata: Metadata): Future[Unit] = {
        val bins: Map[String, Array[Byte]] = Map(
            "head" → AsClient.long2bytearray(metadata.head),
            "tslr" → AsClient.long2bytearray(metadata.tsLastRead)
        )

        log.debug(s"Trying to save metadata :$metadata")
        metadataset.putBins(metadata.qName, bins, Option(-1)) map {
            case _ ⇒
                metacache = metadata
        }
    }

    /**
     * save the tail metadata in store and in cache for a queue
     */
    private def saveMetadataTail(metadata: Metadata): Future[Unit] = {
        val bins: Map[String, Array[Byte]] = Map(
            "tail" → AsClient.long2bytearray(metadata.tail),
            "tslu" → AsClient.long2bytearray(metadata.tsLastUpdate)
        )

        log.debug(s"Trying to save metadata :$metadata")
        metadataset.putBins(metadata.qName, bins, Option(-1)) map {
            case _ ⇒
                metacache = metadata
        }
    }

    /**
     * update the metadata cache from the aerospike server
     */
    private def readMetadata(): Future[Unit] = {
        metadataset.getBins(qName, Seq("head", "tail", "maxsize", "tsc", "tslu", "tslr")) map {
            r ⇒
                {
                    if (r.contains("head") && r.contains("tail") && r.contains("maxsize")
                        && r.contains("tsc") && r.contains("tslu") && r.contains("tslr")) {
                        metacache = new Metadata(
                            qName,
                            ByteBuffer.wrap(r("head")).getLong(),
                            ByteBuffer.wrap(r("tail")).getLong(),
                            ByteBuffer.wrap(r("maxsize")).getInt(),
                            ByteBuffer.wrap(r("tsc")).getLong(),
                            ByteBuffer.wrap(r("tslu")).getLong(),
                            ByteBuffer.wrap(r("tslr")).getLong(),
                            ByteBuffer.wrap(r("ttl")).getInt()
                        )
                    }
                    log.debug(s"metacache has changed $metacache")
                    //println("metacache has changed "+metacache)
                }
        }
    }

    def exists(): Boolean = metacache != null

    /**
     * purge the queue
      * TODO: We have to remove all the messages (call removeMessage recursively)
     */
    def emptyQueue(): Future[Unit] = {
        val now = System.currentTimeMillis()
        val md = new Metadata(qName, 0, 0, metacache.maxSize, metacache.tsCreation, now, metacache.tsLastRead, metacache.ttl)
        saveMetadata(md)
    }

    /**
     * delete the metadata from store and cache for a queue
     * you have to re-create the queue if you want to re-use it.
      * TODO: We have to remove all the messages (call removeMessage recursively)
     */
    def dropQueue(): Future[Unit] = {
        if (metacache == null) {
            Future.failed(new NotExistingQueueException(qName))
        } else {
            metadataset.delete(qName) map {
                case _ ⇒
                    metacache = null
            }
        }
    }

    /**
     * retrieve the size of a queue from the metadata in cache
     */
    def getSize(): Long = {
        if (!config.localOnly)
            Await.result(readMetadata(), Duration.Inf)
        if (metacache == null) {
            throw new NotExistingQueueException(qName)
        } else {
            metacache.tail - metacache.head
        }
    }

    /**
     * add a message in store for a queue
     * update the tail counter in the metadata
     */
    def addMessage(message: Array[Byte]): Future[Unit] = {
        if (metacache == null) {
            Future.failed(new NotExistingQueueException(qName))
        } else {
            if (!config.localOnly)
                Await.result(readMetadata(), Duration.Inf)
            if (reachMaxSize(metacache))
                Future.failed(new ReachMaxSizeException(qName))
            else {
                val bins: Map[String, Array[Byte]] = Map("payload" → message)
                messageset.putBins(qName + "_" + (metacache.tail + 1), bins, if (metacache.ttl == 0) None else Some(metacache.ttl)).flatMap { _ ⇒
                    val now = System.currentTimeMillis()
                    val md2 = new Metadata(metacache.qName, metacache.head, metacache.tail + 1, metacache.maxSize, metacache.tsCreation, now, metacache.tsLastRead, metacache.ttl)
                    log.debug("just add message on" + qName + "_" + (metacache.tail + 1) + "...save metadata")
                    saveMetadataTail(md2)
                }
            }
        }
    }

    /**
     * poll a message (delete) from the store for a queue
     * update the head counter in the metadata
     */
    def pollMessage(): Future[Option[Array[Byte]]] = {
        if (metacache == null) {
            Future.failed(new NotExistingQueueException(qName))
        } else {
            if (emptyFifo(metacache)) {
                log.debug("Fifo is empty")
                Future.failed(new NotDataFoundException(metacache.qName))
            } else {
                val now = System.currentTimeMillis()
                val md2 = new Metadata(metacache.qName, metacache.head + 1, metacache.tail, metacache.maxSize, metacache.tsCreation, metacache.tsLastUpdate, now, metacache.ttl)
                log.debug("About to poll " + qName + "_" + md2.head)
                for {
                    record ← messageset.getandtouch(qName + "_" + md2.head, Some(config.pollTtl))
                    res ← saveMetadataHead(md2)
                } yield record.get("payload")
            }
        }
    }

    /**
     * peek a message (do not delete) from the store for a queue
     */
    def peekMessage(): Future[Option[Array[Byte]]] = {
        if (metacache == null) {
            Future.failed(new NotExistingQueueException(qName))
        } else {
            if (emptyFifo(metacache)) {
                log.debug("Fifo is empty")
                Future.failed(new NotDataFoundException(metacache.qName))
            } else {
                messageset.get(qName + "_" + (metacache.head + 1), "payload")
            }
        }
    }

    /**
     * delete a message from the store for a queue without read it
     * update the head counter in the metadata
     */
    def removeMessage(): Future[Unit] = {
        if (metacache == null) {
            Future.failed(new NotExistingQueueException(qName))
        } else {
            if (emptyFifo(metacache)) {
                log.debug("Fifo is empty")
                Future.failed(new EmptyQueueException(qName))
            } else {
                val now = System.currentTimeMillis()
                messageset.touch(qName + "_" + (metacache.head + 1), Some(config.pollTtl)) flatMap { _ ⇒
                    val md2 = new Metadata(metacache.qName, metacache.head + 1, metacache.tail, metacache.maxSize, metacache.tsCreation, now, metacache.tsLastRead, metacache.ttl)
                    saveMetadata(md2)
                }
            }
        }
    }

    private def emptyFifo(md: Metadata): Boolean = {
        md.tail - md.head == 0
    }

    private def reachMaxSize(md: Metadata): Boolean = {
        (md.maxSize > 0 && md.tail - md.head > md.maxSize)
    }
}
