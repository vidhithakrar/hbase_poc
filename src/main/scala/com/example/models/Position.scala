package com.example.models
case class Position(accountKey: Long,
                    accountType: String,
                    accountNumber: String,
                    assetClassL1: String,
                    assetClassL2: String = "",
                    accountGroupId: String= "",
                    balance: Double,
                    currency: String = "USD") {

  def buildKey(): String = accountKey.toString
  override def toString: String = {
    s"""{"accountKey":$accountKey, "accountType": "$accountType", "accountNumber": "$accountNumber", "assetClassL1": "$assetClassL1", "assetClassL2": "$assetClassL2", "accountGroupId": "$accountGroupId", "balance": $balance, "currency": "$currency" }
    """.stripMargin
  }
}

