package com.matthewjp2525.simpledb.record

import com.matthewjp2525.simpledb.filemanager.BlockNumber

case class RID(blockNumber: BlockNumber, slot: Slot)
