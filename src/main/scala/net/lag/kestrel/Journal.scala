/*
 * Copyright 2009 Twitter, Inc.
 * Copyright 2009 Robey Pointer <robeypointer@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.lag.kestrel

import net.lag.logging.Logger
import java.io._
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.FileChannel
import net.lag.configgy.{Config, ConfigMap, Configgy}


// returned from journal replay
abstract case class JournalItem()
object JournalItem {
  case class Add(item: QItem) extends JournalItem
  case object Remove extends JournalItem
  case object RemoveTentative extends JournalItem
  case class SavedXid(xid: Int) extends JournalItem
  case class Unremove(xid: Int) extends JournalItem
  case class ConfirmRemove(xid: Int) extends JournalItem
  case object EndOfFile extends JournalItem
}


/**
 * Codes for working with the journal file for a PersistentQueue.
 */
class Journal(queuePath: String, syncJournal: => Boolean) {
  protected val log = Logger.get

  protected val rawQueuePath = queuePath
  protected val host = if (Configgy.config==null) ""
                     else Configgy.config.getString("host", "")

  protected val queueFile: Any = new File(queuePath)

  protected var writer: Any = null
  protected var reader: Option[Any] = None
  protected var replayer: Option[Any] = None

  var size: Long = 0

  // small temporary buffer for formatting operations into the journal:
  protected val buffer = new Array[Byte](16)
  protected val byteBuffer = ByteBuffer.wrap(buffer)
  byteBuffer.order(ByteOrder.LITTLE_ENDIAN)

  protected val CMD_ADD = 0
  protected val CMD_REMOVE = 1
  protected val CMD_ADDX = 2
  protected val CMD_REMOVE_TENTATIVE = 3
  protected val CMD_SAVE_XID = 4
  protected val CMD_UNREMOVE = 5
  protected val CMD_CONFIRM_REMOVE = 6
  protected val CMD_ADD_XID = 7

  def open(): Unit = {
    open(queueFile)
  }

  def roll(xid: Int, openItems: List[QItem], queue: Iterable[QItem]): Unit = {
    channelClose(writer)
    val tmpFile = createFileObject(queuePath + "~~" + Time.now)
    open(tmpFile)

    size = 0
    for (item <- openItems) {
      addWithXid(item)
      removeTentative(false)
    }
    saveXid(xid)
    for (item <- queue) {
      add(false, item)
    }

    if (syncJournal) writerForce(writer, false)
    channelClose(writer)
    fileRename(tmpFile, queueFile)
    open
  }

  def close(): Unit = {
    channelClose(writer)
    for (r <- reader) channelClose(r)
    reader = None
  }

  def erase(): Unit = {
    try {
      close()
      fileDelete(queueFile)
    } catch {
      case _ =>
    }
  }

  def inReadBehind(): Boolean = reader.isDefined

  def isReplaying(): Boolean = replayer.isDefined

  protected def add(allowSync: Boolean, item: QItem): Unit = {
    val blob = ByteBuffer.wrap(item.pack())
    size += write(false, CMD_ADDX.toByte, blob.limit)
    do {
      writerWrite(writer, blob)
    } while (blob.position < blob.limit)
    if (allowSync && syncJournal) writerForce(writer, false)
    size += blob.limit
  }

  def add(item: QItem): Unit = add(true, item)

  // used only to list pending transactions when recreating the journal.
  protected def addWithXid(item: QItem) = {
    val blob = ByteBuffer.wrap(item.pack())

    // only called from roll(), so the journal does not need to be synced after a write.
    size += write(false, CMD_ADD_XID.toByte, item.xid, blob.limit)
    do {
      writerWrite(writer, blob)
    } while (blob.position < blob.limit)
    size += blob.limit
  }

  def remove() = {
    size += write(true, CMD_REMOVE.toByte)
  }

  protected def removeTentative(allowSync: Boolean): Unit = {
    size += write(allowSync, CMD_REMOVE_TENTATIVE.toByte)
  }

  def removeTentative(): Unit = removeTentative(true)

  protected def saveXid(xid: Int) = {
    // only called from roll(), so the journal does not need to be synced after a write.
    size += write(false, CMD_SAVE_XID.toByte, xid)
  }

  def unremove(xid: Int) = {
    size += write(true, CMD_UNREMOVE.toByte, xid)
  }

  def confirmRemove(xid: Int) = {
    size += write(true, CMD_CONFIRM_REMOVE.toByte, xid)
  }

  def startReadBehind(): Unit = {
    val pos = if (replayer.isDefined) channelPosition(replayer.get) else channelPosition(writer)
    val rj = getInputChannel(queueFile)
    channelPosition(rj, pos)
    reader = Some(rj)
  }

  def fillReadBehind(f: QItem => Unit): Unit = {
    val pos = if (replayer.isDefined) channelPosition(replayer.get) else channelPosition(writer)
    for (rj <- reader) {
      if (channelPosition(rj) == pos) {
        // we've caught up.
        channelClose(rj)
        reader = None
      } else {
        readJournalEntry(rj) match {
          case (JournalItem.Add(item), _) => f(item)
          case (_, _) =>
        }
      }
    }
  }

  def replay(name: String)(f: JournalItem => Unit): Unit = {
    size = 0
    var lastUpdate = 0L
    val TEN_MB = 10L * 1024 * 1024
    try {
      val in = getInputChannel(queueFile) 
      try {
        replayer = Some(in)
        var done = false
        do {
          readJournalEntry(in) match {
            case (JournalItem.EndOfFile, _) => done = true
            case (x, itemsize) =>
              size += itemsize
              f(x)
              if (size / TEN_MB > lastUpdate) {
                lastUpdate = size / TEN_MB
                log.info("Continuing to read '%s' journal; %d MB so far...", name, lastUpdate * 10)
              }
          }
        } while (!done)
      } catch {
        case e: BrokenItemException =>
          log.error(e, "Exception replaying journal for '%s'", name)
          log.error("DATA MAY HAVE BEEN LOST! Truncated entry will be deleted.")
          truncateJournal(e.lastValidPosition)
      }
    } catch {
      case e: FileNotFoundException =>
        log.info("No transaction journal for '%s'; starting with empty queue.", name)
      case e: IOException =>
        log.error(e, "Exception replaying journal for '%s'", name)
        log.error("DATA MAY HAVE BEEN LOST!")
        // this can happen if the server hardware died abruptly in the middle
        // of writing a journal. not awesome but we should recover.
    }
    replayer = None
  }

  protected def truncateJournal(position: Long) {
    val trancateWriter = getOutputChannel(queueFile)
    try {
      truncateFile(trancateWriter, position)
    } finally {
      channelClose(trancateWriter)
    }
  }

  def readJournalEntry(in: Any): (JournalItem, Int) = {
    byteBuffer.rewind
    byteBuffer.limit(1)
    val lastPosition = channelPosition(in)
    var x: Int = 0
    do {
      x = journalRead(in, byteBuffer)
    } while (byteBuffer.position < byteBuffer.limit && x >= 0)

    if (x < 0) {
      (JournalItem.EndOfFile, 0)
    } else {
      try {
        buffer(0) match {
          case CMD_ADD =>
            val data = readBlock(in)
            (JournalItem.Add(QItem.unpackOldAdd(data)), 5 + data.length)
          case CMD_REMOVE =>
            (JournalItem.Remove, 1)
          case CMD_ADDX =>
            val data = readBlock(in)
            (JournalItem.Add(QItem.unpack(data)), 5 + data.length)
          case CMD_REMOVE_TENTATIVE =>
            (JournalItem.RemoveTentative, 1)
          case CMD_SAVE_XID =>
            val xid = readInt(in)
            (JournalItem.SavedXid(xid), 5)
          case CMD_UNREMOVE =>
            val xid = readInt(in)
            (JournalItem.Unremove(xid), 5)
          case CMD_CONFIRM_REMOVE =>
            val xid = readInt(in)
            (JournalItem.ConfirmRemove(xid), 5)
          case CMD_ADD_XID =>
            val xid = readInt(in)
            val data = readBlock(in)
            val item = QItem.unpack(data)
            item.xid = xid
            (JournalItem.Add(item), 9 + data.length)
          case n =>
            throw new BrokenItemException(lastPosition, new IOException("invalid opcode in journal: " + n.toInt + " at position " + channelPosition(in)))
        }
      } catch {
        case ex: IOException =>
          throw new BrokenItemException(lastPosition, ex)
      }
    }
  }

  def walk() = new Iterator[(JournalItem, Int)] {
    val in = getInputChannel(queueFile)
    var done = false
    var nextItem: Option[(JournalItem, Int)] = None

    def hasNext = {
      if (done) {
        false
      } else {
        nextItem = readJournalEntry(in) match {
          case (JournalItem.EndOfFile, _) =>
            done = true
            channelClose(in)
            None
          case x =>
            Some(x)
        }
        nextItem.isDefined
      }
    }

    def next() = nextItem.get
  }

  protected def readBlock(in: Any): Array[Byte] = {
    val size = readInt(in)
    val data = new Array[Byte](size)
    val dataBuffer = ByteBuffer.wrap(data)
    var x: Int = 0
    do {
      x = journalRead(in, dataBuffer)
    } while (dataBuffer.position < dataBuffer.limit && x >= 0)
    if (x < 0) {
      // we never expect EOF when reading a block.
      throw new IOException("Unexpected EOF")
    }
    data
  }

  protected def readInt(in: Any): Int = {
    byteBuffer.rewind
    byteBuffer.limit(4)
    var x: Int = 0
    do {
      x = journalRead(in, byteBuffer)
    } while (byteBuffer.position < byteBuffer.limit && x >= 0)
    if (x < 0) {
      // we never expect EOF when reading an int.
      throw new IOException("Unexpected EOF")
    }
    byteBuffer.rewind
    byteBuffer.getInt()
  }

  protected def write(allowSync: Boolean, items: Any*): Int = {
    byteBuffer.clear
    for (item <- items) item match {
      case b: Byte => byteBuffer.put(b)
      case i: Int => byteBuffer.putInt(i)
    }
    byteBuffer.flip
    while (byteBuffer.position < byteBuffer.limit) {
      writerWrite(writer, byteBuffer)
    }
    if (allowSync && syncJournal) writerForce(writer, false)
    byteBuffer.limit
  }

  /**
   * Refactor from original version.  The codes can be overriden
   * for journal to host on other storage system, such as HDFS, in 
   * cloud computing environments
   */
  protected def open(f: Any): Unit =  (f: @unchecked) match {
    case file: File => writer = new FileOutputStream(file, true).getChannel
  }

  protected def channelClose(c: Any): Unit = (c: @unchecked) match {
    case channel: FileChannel => channel.close
  }

  protected def createFileObject(s: String): Any = {
    new File(s)
  }

  protected def truncateFile(c: Any, pos: Long): Unit = (c: @unchecked) match {
    case channel: FileChannel => channel.truncate(pos)
  }

  protected def writerWrite(w: Any, blob: ByteBuffer): Unit = (w: @unchecked) match {
    case writer: FileChannel => writer.write(blob)
  }

  protected def writerForce(w: Any, bool: Boolean): Unit = (w: @unchecked) match {
    case writer: FileChannel => writer.force(bool)
  }

  protected def fileRename(s: Any, d: Any): Unit = ((s, d): @unchecked) match {
    case (src: File, dest: File) => src.renameTo(dest)
  }

  protected def fileDelete(f: Any): Unit = (f: @unchecked) match {
    case file: File => file.delete
  }
 
  protected def channelPosition(c: Any): Long = (c: @unchecked) match {
    case channel: FileChannel => channel.position
  }

  protected def channelPosition(c: Any, pos: Long): Unit = (c: @unchecked) match {
    case channel: FileChannel => channel.position(pos)
  }

  protected def getOutputChannel(f: Any): Any = (f: @unchecked)  match {
    case queueFile: File => new FileOutputStream(queueFile, true).getChannel
  }

  protected def getInputChannel(f: Any): Any = (f: @unchecked) match {
    case queueFile: File => new FileInputStream(queueFile).getChannel
  }

  protected def journalRead(channel: Any, buffer: ByteBuffer): Int = (channel: @unchecked) match {
    case in: FileChannel => in.read(buffer)
  } 
}
