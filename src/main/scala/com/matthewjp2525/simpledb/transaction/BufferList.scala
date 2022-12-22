package com.matthewjp2525.simpledb.transaction

import com.matthewjp2525.simpledb.buffer.{Buffer, BufferManager}
import com.matthewjp2525.simpledb.filemanager.BlockId

import scala.collection.mutable
import scala.util.Try

class BufferList(bufferManager: BufferManager):
  private val buffers = new mutable.HashMap[BlockId, Buffer]()
  private val pins = new mutable.ArrayBuffer[BlockId]()

  def getBuffer(blockId: BlockId): Option[Buffer] =
    buffers.get(blockId)

  def pin(blockId: BlockId): Try[Unit] =
    bufferManager.pin(blockId).map { buffer =>
      buffers.put(blockId, buffer)
      pins += blockId
    }

  def unpin(blockId: BlockId): Unit =
    buffers.get(blockId) match
      case None => ()
      case Some(buffer) =>
        bufferManager.unpin(buffer)
        pins -= blockId
        if !pins.contains(blockId) then
          buffers.remove(blockId)

  def unpinAll(): Unit =
    pins.foreach { blockId =>
      buffers.get(blockId) match
        case None => ()
        case Some(buffer) =>
          bufferManager.unpin(buffer)
    }
    buffers.clear()
    pins.clear()