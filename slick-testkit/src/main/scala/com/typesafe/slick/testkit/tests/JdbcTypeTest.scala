package com.typesafe.slick.testkit.tests

import com.typesafe.slick.testkit.util.{JdbcTestDB, AsyncTest}
import java.io.{ObjectInputStream, ObjectOutputStream, ByteArrayOutputStream}
import java.sql.{Blob, Date, Time, Timestamp}
import java.util.UUID
import java.time._
import javax.sql.rowset.serial.SerialBlob
import org.junit.Assert._

import scala.concurrent.Future

/** Data type related tests which are specific to JdbcProfile */
class JdbcTypeTest extends AsyncTest[JdbcTestDB] {
  import tdb.profile.api._

  def testByteArray = {
    class T(tag: Tag) extends Table[(Int, Array[Byte])](tag, "test_ba") {
      def id = column[Int]("id")
      def data = column[Array[Byte]]("data")
      def * = (id, data)
    }
    val ts = TableQuery[T]

    val as1 = for {
      _ <- ts.schema.create
      _ <- ts += (1, Array[Byte](1,2,3))
      _ <- ts += (2, Array[Byte](4,5))
      r1 <- ts.result.map(_.map{ case (id, data) => (id, data.mkString) }.toSet)
      _ = r1 shouldBe Set((1,"123"), (2,"45"))
    } yield ()
    if(implicitly[ColumnType[Array[Byte]]].hasLiteralForm) {
      as1 >> ts.filter(_.data === Array[Byte](4,5)).map(_.data).to[Set].result.map(_.map(_.mkString)).map(_ shouldBe Set("45"))
    } else as1
  }

  def testByteArrayOption = {
    class T(tag: Tag) extends Table[(Int, Option[Array[Byte]])](tag, "test_baopt") {
      def id = column[Int]("id")
      def data = column[Option[Array[Byte]]]("data")
      def * = (id, data)
    }
    val ts = TableQuery[T]

    seq(
      ts.schema.create,
      ts += (1, Some(Array[Byte](6,7))),
      ifCap(rcap.setByteArrayNull)(ts += (2, None)),
      ifNotCap(rcap.setByteArrayNull)(ts.map(_.id) += 2),
      ts.result.map(_.map { case (id, data) => (id, data.map(_.mkString).getOrElse("")) }.toSet).map(_ shouldBe Set((1,"67"), (2,"")))
    )
  }

  def testBlob = ifCapF(rcap.typeBlob) {
    class T(tag: Tag) extends Table[(Int, Blob)](tag, "test3") {
      def id = column[Int]("id")
      def data = column[Blob]("data")
      def * = (id, data)
    }
    val ts = TableQuery[T]

    val a1 = (
      ts.schema.create >>
      (ts += (1, new SerialBlob(Array[Byte](1,2,3)))) >>
      (ts += (2, new SerialBlob(Array[Byte](4,5)))) >>
      ts.result
    ).transactionally
    val p1 = db.stream(a1).mapResult { case (id, data) => (id, data.getBytes(1, data.length.toInt).mkString) }
    materialize(p1).map(_.toSet shouldBe Set((1,"123"), (2,"45"))) flatMap { _ =>
      val f = materializeAsync[(Int, Blob), (Int, String)](db.stream(ts.result.transactionally, bufferNext = false),
        { case (id, data) => db.io((id, data.getBytes(1, data.length.toInt).mkString)) })
      f.map(_.toSet shouldBe Set((1,"123"), (2,"45")))
    }
  }

  def testMappedBlob = ifCap(rcap.typeBlob) {
    case class Serialized[T](value: T)

    implicit def serializedType[T] = MappedColumnType.base[Serialized[T], Blob]({ s =>
      val b = new ByteArrayOutputStream
      val out = new ObjectOutputStream(b)
      out.writeObject(s.value)
      out.flush
      new SerialBlob(b.toByteArray)
    }, { b =>
      val in = new ObjectInputStream(b.getBinaryStream)
      Serialized[T](in.readObject().asInstanceOf[T])
    })

    class T(tag: Tag) extends Table[(Int, Serialized[List[Int]])](tag, "t") {
      def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
      def b = column[Serialized[List[Int]]]("b")
      def * = (id, b)
    }
    val ts = TableQuery[T]

    seq(
      ts.schema.create,
      ts.map(_.b) ++= Seq(Serialized(List(1,2,3)), Serialized(List(4,5))),
      ts.to[Set].result.map(_ shouldBe Set((1, Serialized(List(1,2,3))), (2, Serialized(List(4,5)))))
    ).transactionally
  }

  private def roundtrip[T : BaseColumnType](tn: String, v: T) = {
    class T1(tag: Tag) extends Table[(Int, T)](tag, tn) {
      def id = column[Int]("id")
      def data = column[T]("data")
      def * = (id, data)
    }
    val t1 = TableQuery[T1]

    seq(
      t1.schema.create,
      t1 += (1, v),
      t1.map(_.data).result.head.map(_ shouldBe v),
      t1.filter(_.data === v).map(_.id).result.headOption.map(_ shouldBe Some(1)),
      t1.filter(_.data =!= v).map(_.id).result.headOption.map(_ shouldBe None),
      t1.filter(_.data === v.bind).map(_.id).result.headOption.map(_ shouldBe Some(1)),
      t1.filter(_.data =!= v.bind).map(_.id).result.headOption.map(_ shouldBe None)
    )
  }

  def testDate =
    roundtrip("date_t1", Date.valueOf("2012-12-24"))

  def testTime =
    roundtrip("time_t1", Time.valueOf("17:53:48"))

  def testTimestamp = {
    roundtrip[Timestamp]("timestamp_t1", Timestamp.valueOf("2012-12-24 17:53:48.0")) >> {
      class T2(tag: Tag) extends Table[Option[Timestamp]](tag, "timestamp_t2") {
        def t = column[Option[Timestamp]]("t")
        def * = t
      }
      val t2 = TableQuery[T2]
      t2.schema.create >> (t2 += None) >> t2.result.head.map(_ shouldBe None)
    }
  }

  def testUUID =
    roundtrip[UUID]("uuid_t1", UUID.randomUUID())

  def testLocalDate = roundtrip[LocalDate]("local_date_t1", LocalDate.now())

  def testLocalTime = {
    roundtrip[LocalTime](
      "local_time_hour_greater_than_10",
      LocalTime.now().withHour(14)
    )
  }
  def testLocalTimeWithHourLesserThan10 = {
    roundtrip[LocalTime](
      "local_time_hour_lesser_than_10",
      LocalTime.now().withHour(5)
    )
  }

  def testInstant = {
    roundtrip[Instant](
      "instant_t1",
      LocalDateTime.now(ZoneId.of("UTC")).withHour(15).toInstant(ZoneOffset.of("-01:00"))
    )
  }
  def testInstantWithHourLesserThan10 = {
    roundtrip[Instant](
      "instant_with_hour_lesser_than 10",
      LocalDateTime.now(ZoneId.of("UTC")).withHour(5).toInstant(ZoneOffset.of("-01:00"))
    )
  }

  def testLocalDateTimeWithHourLesserThan10 = {
    roundtrip[LocalDateTime](
      "local_date_time_hour_lesser_than_10",
      LocalDateTime.now().withHour(2)
    )
  }

  def testLocalDateTimeWithHourGreaterThan10 = {
    roundtrip[LocalDateTime](
      "local_date_time_hour_greater_than_10",
      LocalDateTime.now().withHour(12)
    )
  }

  def testOffsetTime = {
    roundtrip[OffsetTime](
      s"offset_time_tz_utc",
      OffsetTime.now(ZoneId.of("UTC")).withHour(15)
    )
  }
  def testOffsetTimeHourLessThan10UTC = {
    roundtrip[OffsetTime](
      s"offset_time_tz_utc_hour_less_than_10",
      OffsetTime.now(ZoneId.of("UTC")).withHour(5)
    )
  }
  def testOffsetTimeNegativeOffsetGreaterThan10 = {
    // Offset -> -11:00 / -11:00
    roundtrip[OffsetTime](
      s"offset_time_tz_negative_greater_10",
      OffsetTime.now(ZoneId.of("Pacific/Samoa")).withHour(15)
    )
  }
  def testOffsetTimeNegativeOffsetLessThan10 = {
    // Offset -> -3:00 / -3:00
    roundtrip[OffsetTime](
      s"offset_time_tz_negative_less_10",
      OffsetTime.now(ZoneId.of("Antarctica/Rothera")).withHour(15)
    )
  }
  def testOffsetTimePositiveOffsetGreaterThan10 = {
    // Offset -> +12:00 / +12:00
    roundtrip[OffsetTime](
      s"offset_time_tz_positive_greater_10",
      OffsetTime.now(ZoneId.of("Pacific/Wallis")).withHour(15)
    )
  }
  def testOffsetTimePositiveOffsetLessThan10 = {
    // Offset -> +2:00 / +2:00
    roundtrip[OffsetTime](
      s"offset_time_tz_positive_less_10",
      OffsetTime.now(ZoneId.of("Africa/Johannesburg")).withHour(15)
    )
  }

  def testOffsetDateTime = {
    roundtrip[OffsetDateTime](
      s"offset_date_time_tz_utc",
      OffsetDateTime.now(ZoneId.of("UTC")).withHour(15)
    )
  }
  def testOffsetDateTimeWithHourLesserThan10 = {
    roundtrip[OffsetDateTime](
      s"offset_date_time_hour_lesser_than_10",
      OffsetDateTime.now(ZoneId.of("UTC")).withHour(5)
    )
  }
  def testOffsetDateTimeNegativeGreaterThan10 = {
    // Offset -> -11:00 / -11:00
    roundtrip[OffsetDateTime](
      s"offset_date_time_tz_negative_greater_10",
      OffsetDateTime.now(ZoneId.of("Pacific/Samoa")).withHour(15)
    )
  }
  def testOffsetDateTimeNegativeLessThan10 = {
    // Offset -> -3:00 / -3:00
    roundtrip[OffsetDateTime](
      s"offset_date_time_tz_negative_less_10",
      OffsetDateTime.now(ZoneId.of("Antarctica/Rothera")).withHour(15)
    )
  }
  def testOffsetDateTimePositiveGreaterThan10 = {
    // Offset -> +12:00 / +12:00
    roundtrip[OffsetDateTime](
      s"offset_date_time_tz_positive_greater_10",
      OffsetDateTime.now(ZoneId.of("Pacific/Wallis")).withHour(15)
    )
  }
  def testOffsetDateTimePositiveLessThan10 = {
    // Offset -> +2:00 / +2:00
    roundtrip[OffsetDateTime](
      s"offset_date_time_tz_positive_less_10",
      OffsetDateTime.now(ZoneId.of("Africa/Johannesburg")).withHour(15)
    )
  }

  def testZonedDateTime = {
    roundtrip[ZonedDateTime](
      s"zoned_date_time_tz_utc",
      ZonedDateTime.now(ZoneId.of("UTC")).withHour(15)
    )
  }
  def testZonedDateTimeWithHourLesserThan10 = {
    roundtrip[ZonedDateTime](
      s"zoned_date_time_hour_lesser_than_10",
      ZonedDateTime.now(ZoneId.of("UTC")).withHour(5)
    )
  }
  def testZonedDateTimeNegativeGreaterThan10 = {
    // Offset -> -11:00 / -11:00
    roundtrip[ZonedDateTime](
      s"zoned_date_time_tz_negative_greater_10",
      ZonedDateTime.now(ZoneId.of("Pacific/Samoa")).withHour(15)
    )
  }
  def testZonedDateTimeNegativeLessThan10 = {
    // Offset -> -3:00 / -3:00
    roundtrip[ZonedDateTime](
      s"zoned_date_time_tz_negative_less_10",
      ZonedDateTime.now(ZoneId.of("Antarctica/Rothera")).withHour(15)
    )
  }
  def testZonedDateTimePositiveGreaterThan10 = {
    // Offset -> +12:00 / +12:00
    roundtrip[ZonedDateTime](
      s"zoned_date_time_tz_positive_greater_10",
      ZonedDateTime.now(ZoneId.of("Pacific/Wallis")).withHour(15)
    )
  }
  def testZonedDateTimePositiveLessThan10 = {
    // Offset -> +2:00 / +2:00
    roundtrip[ZonedDateTime](
      s"zoned_date_time_tz_positive_less_10",
      ZonedDateTime.now(ZoneId.of("Africa/Johannesburg")).withHour(15)
    )
  }

  def testOverrideIdentityType = {
    class T1(tag: Tag) extends Table[Int](tag, "t1") {
      def id = column[Int]("id", O.PrimaryKey, O.AutoInc, O.SqlType("_FOO_BAR_"))
      def * = id
    }
    val t1 = TableQuery[T1]
    t1.schema.createStatements.mkString.should(_ contains "_FOO_BAR_")
    Future.successful(())
  }
}
