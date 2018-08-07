package com.example.repositories

import com.example.models.{AggregatedPosition, Position}
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

class PositionsRepositoryTest extends FunSuite with BeforeAndAfterEach with Matchers {

  override protected def beforeEach(): Unit = {
    seedData()
  }

  override protected def afterEach(): Unit = {
    clearData()
  }

  test("Should filter the positions by asset class") {
    val cashPositions = PositionsRepository.filterByAssetClass("CASH")

    cashPositions.size shouldBe 6
    cashPositions.forall(position => position.assetClassL1.eq("CASH"))
  }

  test("Should filter the positions by asset class and aggregate the amount") {
    val cashPositions: AggregatedPosition = PositionsRepository.filterByAssetAndSumOnAmount("CASH")

    cashPositions.total shouldBe 287.72
    cashPositions.positions.forall(position => position.assetClassL1.eq("CASH"))
  }

  test("Should do the forward pagination") {
    val paginatedPositions = PositionsRepository.nextPage(None, 2)
    paginatedPositions.positions.size shouldBe 2

    val expectedNextPageAccountKeysSeq = Seq(4343L, 6767L, 676767L)
    val nextPagePositions = PositionsRepository.nextPage(paginatedPositions.pageMetaData.nextPageOffset, 3)

    nextPagePositions.positions.size shouldBe 3
    val actualNextPageAccountKeysSeq = nextPagePositions.positions.map(_.accountKey)
    actualNextPageAccountKeysSeq shouldBe expectedNextPageAccountKeysSeq
  }

  test("should do backward pagination") {
    val paginatedPositions = PositionsRepository.nextPage(None, 2)
    val nextPagePositions = PositionsRepository.nextPage(paginatedPositions.pageMetaData.nextPageOffset, 3)

    val prevPagePositions = PositionsRepository.prevPage(nextPagePositions.pageMetaData.prevPageOffset.get, 2)
    prevPagePositions.positions shouldBe paginatedPositions.positions
  }

  private def seedData(): Unit = {
    PositionsRepository.add(Position(accountKey = 12345L, accountType = "SAVINGS", accountNumber = "1234567", assetClassL1 = "CASH", balance = 23.12))
    PositionsRepository.add(Position(accountKey = 12498L, accountType = "SAVINGS", accountNumber = "1234567", assetClassL1 = "CASH", balance = 83.12))
    PositionsRepository.add(Position(accountKey = 4343L, accountType = "SAVINGS", accountNumber = "1234567", assetClassL1 = "EQUITY", balance = 13.12))
    PositionsRepository.add(Position(accountKey = 6767L, accountType = "SAVINGS", accountNumber = "1234567", assetClassL1 = "CASH", balance = 32.12))
    PositionsRepository.add(Position(accountKey = 676767L, accountType = "SAVINGS", accountNumber = "1234567", assetClassL1 = "CASH", balance = 33.12))
    PositionsRepository.add(Position(accountKey = 76767L, accountType = "SAVINGS", accountNumber = "1234567", assetClassL1 = "CASH", balance = 53.12))
    PositionsRepository.add(Position(accountKey = 7676868L, accountType = "SAVINGS", accountNumber = "1234567", assetClassL1 = "CASH", balance = 63.12))
  }

  private def clearData(): Unit = {
    PositionsRepository.clearAll()
  }
}
