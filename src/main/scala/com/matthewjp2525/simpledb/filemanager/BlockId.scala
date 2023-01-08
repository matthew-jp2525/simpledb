package com.matthewjp2525.simpledb.filemanager

type FileName = String
type BlockNumber = Int
case class BlockId(fileName: FileName, blockNumber: BlockNumber):
  override def toString: String = s"[file $fileName, block $blockNumber]"
