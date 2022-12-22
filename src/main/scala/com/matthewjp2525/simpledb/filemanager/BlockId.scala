package com.matthewjp2525.simpledb.filemanager

case class BlockId(fileName: String, blockNumber: Int):
  override def toString: String = s"[file $fileName, block $blockNumber]"
