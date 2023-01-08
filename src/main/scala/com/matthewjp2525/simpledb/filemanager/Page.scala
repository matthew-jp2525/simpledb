package com.matthewjp2525.simpledb.filemanager

import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}

class Page private(byteBuffer: ByteBuffer):
  def getInt(offset: Int): Int = byteBuffer.getInt(offset)

  def setInt(offset: Int, value: Int): Unit = byteBuffer.putInt(offset, value)

  def getString(offset: Int): String =
    val bytes = getBytes(offset)
    new String(bytes, Page.charSet)

  def getBytes(offset: Int): Array[Byte] =
    byteBuffer.position(offset)
    val length = byteBuffer.getInt()
    val bytes = new Array[Byte](length)
    byteBuffer.get(bytes)
    bytes

  def setString(offset: Int, string: String): Unit =
    val bytes = string.getBytes(Page.charSet)
    setBytes(offset, bytes)

  def setBytes(offset: Int, bytes: Array[Byte]): Unit =
    byteBuffer.position(offset)
    byteBuffer.putInt(bytes.length)
    byteBuffer.put(bytes)

  private[filemanager] def contents: ByteBuffer =
    byteBuffer.position(0)
    byteBuffer


object Page:
  private val charSet: Charset = StandardCharsets.UTF_8

  def apply(blockSize: Int): Page = new Page(ByteBuffer.allocateDirect(blockSize))

  def apply(byteArray: Array[Byte]): Page = new Page(ByteBuffer.wrap(byteArray))

  def maxLength(strlen: Int): Int =
    val bytesPerChar = charSet.newEncoder().maxBytesPerChar()
    Integer.BYTES + (strlen * bytesPerChar.toInt)