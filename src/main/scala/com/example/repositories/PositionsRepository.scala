package com.example.repositories

import java.{lang, util}
import java.nio.ByteBuffer.wrap

import com.example.HbaseConnection
import com.example.models.{AggregatedPosition, PageMetaData, PaginatedPositions, Position}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.coprocessor.{AggregationClient, DoubleColumnInterpreter}
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.util.Bytes.toBytes

import scala.collection.JavaConverters._
import org.apache.hadoop.hbase.{CompareOperator, HConstants, TableName}

import scala.collection.mutable

//TODO : Manage connection open & close
object PositionsRepository extends HbaseConnection {
  val table: Table = connection.getTable(TableName.valueOf(toBytes("positions")))
  private val columnFamily: Array[Byte] = toBytes("f")

  def list(): Seq[Position] = {
    var positions = Seq[Position]()
    val scan = new Scan()
    table.getScanner(scan).forEach(result => {
      positions = positions :+ toPosition(result)
    })
    positions
  }

  def add(position: Position): Position = {
    val positionRow = toPut(position)
    table.put(positionRow)
    position
  }

  def clearAll(): Unit = {
    val scan = new Scan()
    table.getScanner(scan).forEach { result: Result =>
      table.delete(new Delete(result.getRow))
    }
  }

  def findOneWith(key: String): Option[Position] = {
    val result = Option(table.get(new Get(toBytes(key))))
    result.map(toPosition)
  }

  def filterByAssetClass(assetClass: String): Seq[Position] = {
    var positions = Seq[Position]()
    val scan = new Scan()
    val columnValueFilter = new SingleColumnValueFilter(columnFamily, toBytes("assetClassL1"), CompareOperator.EQUAL, toBytes(assetClass))
    scan.setFilter(columnValueFilter)
    table.getScanner(scan).forEach((result: Result) => {
      val position = toPosition(result)
      positions = positions :+ position
    })
    positions
  }

  def filterByAssetAndSumOnAmount(assetClass: String): AggregatedPosition = {
    var positions = Seq[Position]()
    val scan = new Scan()

    val familyMap = new mutable.HashMap[Array[Byte], util.NavigableSet[Array[Byte]]]().asJava
    familyMap.put(columnFamily, null)
    scan.setFamilyMap(familyMap)

    val columnValueFilter = new SingleColumnValueFilter(columnFamily, toBytes("assetClassL1"), CompareOperator.EQUAL, toBytes(assetClass))
    scan.setFilter(columnValueFilter)

    table.getScanner(scan).forEach((result: Result) => {
      val position = toPosition(result)
      positions = positions :+ position
    })
    val doubleColumnInterpreter = new DoubleColumnInterpreter()
    val total = new AggregationClient(conf).sum(table, doubleColumnInterpreter, scan)
    AggregatedPosition(total, positions)
  }

  def groupBy[T](groupKey: Position => T): mutable.Map[T, List[Position]] = {
    val groupedResult: mutable.Map[T, List[Position]] = mutable.Map()

    table.getScanner(columnFamily).forEach((result: Result) => {
      val position = toPosition(result)
      val key = groupKey(position)

      val group = groupedResult.getOrElse(key, List())
      groupedResult(key) = position :: group
    })

    groupedResult
  }

  def nextPage(offset: Option[Array[Byte]] = None, size: Int): PaginatedPositions = {
    var paginatedPositions = List[Position]()

    val scan = new Scan()
    scan.withStartRow(offset.getOrElse(HConstants.EMPTY_START_ROW), false)
    scan.setLimit(size)

    var nextPageOffset: Option[Array[Byte]] = None
    var prevPageOffset: Option[Array[Byte]] = None

    table.getScanner(scan).forEach(result => {
      paginatedPositions = paginatedPositions :+ toPosition(result)
      nextPageOffset = new Some[Array[Byte]](result.getRow)
      prevPageOffset = prevPageOffset.orElse(Some(result.getRow))
    })

    if (paginatedPositions.size < size)
      nextPageOffset = None

    if (offset.isEmpty)
      prevPageOffset = None

    PaginatedPositions(PageMetaData(nextPageOffset, prevPageOffset), paginatedPositions)
  }

  def prevPage(offset: Array[Byte], size: Int): PaginatedPositions = {
    var paginatedPositions = List[Position]()

    val scan = new Scan()
    scan.setReversed(true)
    scan.withStartRow(offset, false)
    scan.setLimit(size)

    var nextPageOffset: Option[Array[Byte]] = None
    var prevPageOffset: Option[Array[Byte]] = None

    table.getScanner(scan).forEach(result => {
      paginatedPositions = toPosition(result) +: paginatedPositions
      prevPageOffset = new Some[Array[Byte]](result.getRow)
      nextPageOffset = nextPageOffset.orElse(Some(result.getRow))
    })

    if (paginatedPositions.size < size)
      prevPageOffset = None

    PaginatedPositions(PageMetaData(nextPageOffset, prevPageOffset), paginatedPositions)
  }

  private def toPosition(e: Result) = {

    val accountKey = wrap(e.getValue(columnFamily, toBytes("accountKey"))).getLong
    val accountType = new String(e.getValue(columnFamily, toBytes("accountType")))
    val accountNumber = new String(e.getValue(columnFamily, toBytes("accountNumber")))
    val assetClassL1 = new String(e.getValue(columnFamily, toBytes("assetClassL1")))
    val assetClassL2 = new String(e.getValue(columnFamily, toBytes("assetClassL2")))
    val accountGroupId = new String(e.getValue(columnFamily, toBytes("accountGroupId")))
    val balance = wrap(e.getValue(columnFamily, toBytes("balance"))).getDouble
    val currency = new String(e.getValue(columnFamily, toBytes("currency")))

    Position(accountKey, accountType, accountNumber, assetClassL1, assetClassL2, accountGroupId, balance, currency)
  }

  def toPut(position: Position): Put = {
    new Put(toBytes(position.buildKey()))
      .addColumn(columnFamily, toBytes("accountKey"), toBytes(position.accountKey))
      .addColumn(columnFamily, toBytes("accountType"), toBytes(position.accountType))
      .addColumn(columnFamily, toBytes("accountNumber"), toBytes(position.accountNumber))
      .addColumn(columnFamily, toBytes("assetClassL1"), toBytes(position.assetClassL1))
      .addColumn(columnFamily, toBytes("assetClassL2"), toBytes(position.assetClassL2))
      .addColumn(columnFamily, toBytes("accountGroupId"), toBytes(position.accountGroupId))
      .addColumn(columnFamily, toBytes("balance"), toBytes(position.balance))
      .addColumn(columnFamily, toBytes("currency"), toBytes(position.currency))
  }
}
