package com.example.models

case class PaginatedPositions(pageMetaData : PageMetaData, positions: Seq[Position])
case class PageMetaData(nextPageOffset : Option[Array[Byte]], prevPageOffset : Option[Array[Byte]])
