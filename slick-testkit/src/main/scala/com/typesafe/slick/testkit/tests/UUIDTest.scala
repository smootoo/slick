package com.typesafe.slick.testkit.tests

import java.util.UUID

import com.typesafe.slick.testkit.util.{AsyncTest, JdbcTestDB}

/**
  * Test of the UUID type in all the different places it can be used; queries, table create, defaults, indexes
  */
class UUIDTest extends AsyncTest[JdbcTestDB] {

  import tdb.profile.api._

  def testUUID = {
    val uuid0 = UUID.fromString("99123456-1234-4321-9876-123456789ABC")
    val uuidSampleData = uuid0 +: (0 to 3).map(x => UUID.randomUUID())
    val uuid1 = uuidSampleData(1)
    val uuid2 = uuidSampleData(2)
    val uuid3 = uuidSampleData(3)
    val uuid4 = uuidSampleData(4)
    class UUIDData(name: String)(tag: Tag) extends Table[(Int, UUID, Option[UUID])](tag, name) {
      def id = column[Int]("id")
      def data = column[UUID]("data", O Default uuid0)
      def optData = column[Option[UUID]]("optData", O Default None)
      def * = (id, data, optData)
    }
    val xs = TableQuery(new UUIDData("UUIDX")(_))
    val ys = TableQuery(new UUIDData("UUIDY")(_))
    for {
      // Literal column
      // This test currently commented out. It will fail on DB2/Hsql and Postgres. Still thinking of the best way to handle it.
//      _ <- LiteralColumn(uuid0).result.map(_ shouldBe uuid0)

      // insert values, some using default, and with some nulls
      _ <- (xs.schema ++ ys.schema).create
      _ <- xs ++= uuidSampleData.zipWithIndex.map { case (data, id) => (id + 10, data, if (id % 2 == 0) Some(data) else None) }
      _ <- ys ++= uuidSampleData.zipWithIndex.map { case (data, id) => (id + 11, data, if (id % 2 == 1) Some(data) else None) }

      _ <- xs.map(_.data).to[Set].result.map(_ shouldBe uuidSampleData.toSet)
      // UUID filter
      q1 = xs.filter(_.data === uuid0).to[Set]
      r1 <- q1.result
      _ = r1 shouldBe Set((10, uuid0, Some(uuid0)))
      _ <- ys.filter(_.data === uuid0).sortBy(_.data).result.map(_ shouldBe Seq((11, uuid0, None)))

      // optional UUID filtering
      _ <- ys.filter(_.optData === uuid0).map(_.id).result.map(_ shouldBe Seq())
      _ <- ys.filter(_.optData === uuid1).map(_.id).result.map(_ shouldBe Seq(12))
      _ <- ys.filter(_.optData.isEmpty).map(_.id).result.map(_ shouldBe Seq(11, 13, 15))

      // filter with a LiteralColumn
      _ = xs.filter(_.data === LiteralColumn(uuid0)).map(_.id).result.map(_ shouldBe Seq(10))

      //bind variables
      _ <- ys.filter(_.data === uuid0.bind).map(_.id).result.map(_ shouldBe Seq(11))
      _ <- ys.filter(_.optData === Option[UUID](null).bind).map(_.id).result.map(_ shouldBe Seq())

      // inner,left,right,outer joins with UUID
      r2 <- (xs.filter(_.data === uuid0) joinLeft ys on (_.optData === _.optData)).to[Set].result
      _ = r2 shouldBe Set(
        ((10, uuid0, Some(uuid0)), None))
      r3 <- (xs.filter(_.data === uuid0) joinRight ys on (_.optData === _.optData)).to[Set].result
      _ = r3 shouldBe Set(
        (None, (11, uuid0, None)),
        (None, (12, uuid1, Some(uuid1))),
        (None, (13, uuid2, None)),
        (None, (14, uuid3, Some(uuid3))),
        (None, (15, uuid4, None)))
      r4 <- (xs.filter(_.data === uuid0) join ys on (_.optData === _.optData)).to[Set].result
      _ = r4 shouldBe Set()
      r5 <- (xs.filter(_.data === uuid1) joinFull ys on (_.optData === _.optData)).to[Set].result
      _ = r5 shouldBe Set(
              (Some((11, uuid1, None)), None),
              (None, Some((11, uuid0, None))),
              (None, Some((12, uuid1, Some(uuid1)))),
              (None, Some((13, uuid2, None))),
              (None, Some((14, uuid3, Some(uuid3)))),
              (None, Some((15, uuid4, None))))

      // in clauses
      _ <- xs.filter(_.data inSetBind Seq()).map(_.id).result.map(_ shouldBe Nil)
      _ <- xs.filter(_.data inSetBind uuidSampleData.take(1)).map(_.id).result.map(_ shouldBe Seq(10))
      _ <- xs.filter(_.data inSetBind uuidSampleData.take(2)).map(_.id).result.map(_ shouldBe Seq(10, 11))
      _ <- xs.filter(_.data inSet Seq()).map(_.id).result.map(_ shouldBe Nil)
      _ <- xs.filter(_.data inSet uuidSampleData.take(1)).map(_.id).result.map(_ shouldBe Seq(10))
      _ <- xs.filter(_.data inSet uuidSampleData.take(2)).map(_.id).result.map(_ shouldBe Seq(10, 11))

      // exists type UUID operations
      _ <- xs.exists.result.map(_ shouldBe true)
      _ <- xs.filter(_.id === 23).exists.result.map(_ shouldBe false)
    } yield {}
  }

  def testUUIDPrimaryKey = {
    val uuid0 = UUID.randomUUID()
    val uuid1 = UUID.randomUUID()
    val uuid2 = UUID.randomUUID()
    val uuids = Seq(uuid0, uuid1, uuid2)
    class UUIDPK(name: String)(tag: Tag) extends Table[(UUID, String)](tag, name) {
      def id = column[UUID]("id")
      def data = column[String]("data")
      def * = (id, data)
      def pk = primaryKey("uuidpk_pk", id)
    }
    val us = TableQuery(new UUIDPK("UUIDPK")(_))
    for {
      _ <- us.schema.create
      _ <- us ++= uuids.map { uuid => (uuid, uuid.toString) }

      _ <- us.filter(_.id === uuid0).to[Set].result.map(_ shouldBe Set((uuid0, uuid0.toString)))
      _ <- us.filter(_.data === uuid0.toString).to[Set].result.map(_ shouldBe Set((uuid0, uuid0.toString)))
      _ <- us.filter(_.id === UUID.randomUUID()).to[Set].result.map(_ shouldBe Set.empty)
    } yield {}
  }
}
