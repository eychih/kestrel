/*
 * Copyright 2010 SocialMedia, Inc.
 * Copyright 2010 Ey-Chih Chow <eychih@hotmail.com>
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

import java.io._
import java.nio.ByteBuffer
import java.net.{InetAddress, URI}
import java.util.regex.{Pattern, Matcher}
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, FSDataOutputStream, Path}
import org.apache.hadoop.conf.Configuration
import scala.util.matching.Regex

trait Hadoop extends Journal {
  private val queueTag = "_" + InetAddress.getByName(host).getHostAddress.replace('.', '_')

  override protected val queueFile: Any = new Path(rawQueuePath + queueTag)

  protected val fs = FileSystem.get(URI.create(rawQueuePath.substring(0, rawQueuePath.indexOf('/',7))), new Configuration())
  
  override protected def open(f: Any): Unit = (f: @unchecked) match {
    case file: Path => if (fs.exists(file)) writer = fs.append(file)
                       else writer = fs.create(file)
  }

  override protected def channelClose(c: Any): Unit = (c: @unchecked) match {
    case rchannel: FSDataInputStream => rchannel.close
    case wchannel: FSDataOutputStream => wchannel.close
  }

  override protected def createFileObject(s: String): Any = {
    new Path(s + queueTag)
  }

  override def truncateFile(c: Any, pos: Long): Unit = (c: @unchecked) match {
    case writer: FSDataOutputStream => writer 
  }
 
  override protected def writerWrite(w: Any, blob: ByteBuffer): Unit = (w: @unchecked) match {
    case writer: FSDataOutputStream => writer.write(blob.array)
  }   

  override protected def writerForce(w: Any, bool: Boolean): Unit = (w: @unchecked) match {
    case writer: FSDataOutputStream => writer.flush; writer.sync
  }

  override protected def fileRename(s: Any, d: Any): Unit = ((s, d): @unchecked) match {
    case (src: Path, dest: Path) => fs.rename(src, dest)
  }

  override protected def fileDelete(f: Any): Unit = (f: @unchecked) match {
    case file: Path => fs.delete(file, false)
  }
    
  override protected def channelPosition(c: Any): Long = (c: @unchecked) match {
    case rchannel: FSDataInputStream => rchannel.getPos
  }

  override protected def channelPosition(c: Any, pos: Long): Unit = (c: @unchecked) match {
    case rchannel: FSDataInputStream => rchannel.seek(pos)
  }

  override protected def getOutputChannel(f: Any): Any = (f: @unchecked) match {
    case queueFile: Path => if (fs.exists(queueFile)) fs.append(queueFile)
                       else fs.create(queueFile)
  }

  override protected def getInputChannel(f: Any): Any = (f: @unchecked) match {
    case queueFile: Path => fs.open(queueFile)
  }

  override protected def journalRead(channel: Any, buffer: ByteBuffer): Int = (channel: @unchecked) match {
    case in: FSDataInputStream[InputStream] => 
        val nbytes = in.read(buffer.array)
        in.seek(in.getPos + nbytes)
        nbytes 
  }
    
}
  
