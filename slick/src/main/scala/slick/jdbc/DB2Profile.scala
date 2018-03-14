package slick.jdbc

import java.sql.{PreparedStatement, ResultSet, Timestamp}
import java.time._
import java.time.format.DateTimeFormatter

import scala.concurrent.ExecutionContext
import slick.ast._
import slick.compiler.{CompilerState, Phase}
import slick.dbio._
import slick.jdbc.meta.MTable
import slick.lifted._
import slick.relational.RelationalCapabilities
import slick.basic.Capability
import slick.util.MacroSupport.macroSupportInterpolation

/** Slick profile for IBM DB2 UDB.
  *
  * This profile implements [[slick.jdbc.JdbcProfile]]
  * ''without'' the following capabilities:
  *
  * <ul>
  *   <li>[[slick.relational.RelationalCapabilities.reverse]]:
  *     This String function is not available in DB2.</li>
  *   <li>[[slick.jdbc.JdbcCapabilities.insertOrUpdate]]:
  *     InsertOrUpdate operations are emulated on the client side if generated
  *     keys should be returned. Otherwise the operation is performmed
  *     natively on the server side.</li>
  *   <li>[[slick.jdbc.JdbcCapabilities.booleanMetaData]]:
  *     DB2 doesn't have booleans, so Slick maps to SMALLINT instead.
  *     Other JDBC drivers like MySQL map TINYINT(1) back to a Scala
  *     Boolean. DB2 maps SMALLINT to an Integer and that's how it shows
  *     up in the JDBC meta data, thus the original type is lost.</li>
  *   <li>[[slick.jdbc.JdbcCapabilities.supportsByte]]:
  *     DB2 does not have a BYTE type.</li>
  * </ul>
  *
  * Note: The DB2 JDBC driver has problems with quoted identifiers. Columns
  * which are returned from inserts must not require quoted names (in
  * particular, they must not contain lower-case characters or be equal to a
  * reserved word), otherwise a bug in the DB2 JDBC driver triggers a SQL
  * Error -206 (SQLState 42703).
  */
trait DB2Profile extends JdbcProfile {

  override protected def computeCapabilities: Set[Capability] = (super.computeCapabilities
    - RelationalCapabilities.reverse
    - JdbcCapabilities.insertOrUpdate
    - JdbcCapabilities.supportsByte
    - JdbcCapabilities.booleanMetaData
  )

  override protected lazy val useServerSideUpsert = true
  override protected lazy val useServerSideUpsertReturning = false
  override protected val invokerMutateType: ResultSetType = ResultSetType.ScrollSensitive

  override protected def computeQueryCompiler =
    (super.computeQueryCompiler.addAfter(Phase.removeTakeDrop, Phase.expandSums)
      + Phase.rewriteBooleans)
  override val columnTypes = new JdbcTypes
  override def createQueryBuilder(n: Node, state: CompilerState): QueryBuilder = new QueryBuilder(n, state)
  override def createTableDDLBuilder(table: Table[_]): TableDDLBuilder = new TableDDLBuilder(table)
  override def createColumnDDLBuilder(column: FieldSymbol, table: Table[_]): ColumnDDLBuilder = new ColumnDDLBuilder(column)
  override def createSequenceDDLBuilder(seq: Sequence[_]): SequenceDDLBuilder[_] = new SequenceDDLBuilder(seq)

  override def defaultTables(implicit ec: ExecutionContext): DBIO[Seq[MTable]] =
    MTable.getTables(None, None, None, Some(Seq("TABLE"))).map(_.filter(_.name.schema.filter(_ == "SYSTOOLS").isEmpty))

  override def defaultSqlTypeName(tmd: JdbcType[_], sym: Option[FieldSymbol]): String = tmd.sqlType match {
    case java.sql.Types.TINYINT => "SMALLINT" // DB2 has no smaller binary integer type
    case _ => super.defaultSqlTypeName(tmd, sym)
  }

  override val scalarFrom = Some("sysibm.sysdummy1")

  class QueryBuilder(tree: Node, state: CompilerState) extends super.QueryBuilder(tree, state) {

    override protected val hasPiFunction = false
    override protected val hasRadDegConversion = false
    override protected val pi = "decfloat(3.1415926535897932384626433832)"

    override def expr(c: Node, skipParens: Boolean = false): Unit = c match {
      case RowNumber(by) =>
        b += "row_number() over("
        if(!by.isEmpty) buildOrderByClause(by)
        b += ")"
      case Library.IfNull(l, r) =>
        /* DB2 does not support IFNULL so we use COALESCE instead */
        b += "coalesce("; expr(l, true); b += ","; expr(r, true); b += ")"
      case Library.NextValue(SequenceNode(name)) => b += "(next value for " += quoteIdentifier(name) += ")"
      case Library.CurrentValue(SequenceNode(name)) => b += "(prevval for " += quoteIdentifier(name) += ")"
      case Library.User() => b += "current user"
      case Library.Database() => b += "current server"
      case Library.CountAll(LiteralNode(1)) => b"count(*)"
      case _ => super.expr(c, skipParens)
    }

    override protected def buildOrdering(n: Node, o: Ordering) {
      /* DB2 does not have explicit NULLS FIST/LAST clauses. Nulls are
       * sorted after non-null values by default. */
      if(o.nulls.first && !o.direction.desc) {
        b += "case when ("
        expr(n)
        b += ") is null then 0 else 1 end,"
      } else if(o.nulls.last && o.direction.desc) {
        b += "case when ("
        expr(n)
        b += ") is null then 1 else 0 end,"
      }
      expr(n)
      if(o.direction.desc) b += " desc"
    }

    override protected def buildForUpdateClause(forUpdate: Boolean) = {
      super.buildForUpdateClause(forUpdate)
      if(forUpdate) {
        b" with RS "
      }
    }
  }

  class TableDDLBuilder(table: Table[_]) extends super.TableDDLBuilder(table) {
    override protected def createIndex(idx: Index) = {
      if(idx.unique) {
        /* Create a UNIQUE CONSTRAINT (with an automatically generated backing
         * index) because DB2 does not allow a FOREIGN KEY CONSTRAINT to
         * reference columns which have a UNIQUE INDEX but not a nominal UNIQUE
         * CONSTRAINT. */
        val sb = new StringBuilder append "ALTER TABLE " append quoteIdentifier(table.tableName) append " ADD "
        sb append "CONSTRAINT " append quoteIdentifier(idx.name) append " UNIQUE("
        addIndexColumnList(idx.on, sb, idx.table.tableName)
        sb append ")"
        sb.toString
      } else super.createIndex(idx)
    }

    //For compatibility with all versions of DB2 
    //http://stackoverflow.com/questions/3006999/sql-query-to-truncate-table-in-ibm-db2
    override def truncateTable = s"DELETE FROM ${quoteTableName(tableNode)}"
  }

  class ColumnDDLBuilder(column: FieldSymbol) extends super.ColumnDDLBuilder(column) {
    override def appendColumn(sb: StringBuilder) {
      val qname = quoteIdentifier(column.name)
      sb append qname append ' '
      appendType(sb)
      appendOptions(sb)
      if(jdbcType.isInstanceOf[JdbcTypes#BooleanJdbcType]) {
        sb append " constraint "+quoteIdentifier(column.name+"__bool")+" check (" append qname append " in (0, 1))"
      }
    }
  }

  class SequenceDDLBuilder[T](seq: Sequence[T]) extends super.SequenceDDLBuilder(seq) {
    override def buildDDL: DDL = {
      val b = new StringBuilder append "create sequence " append quoteIdentifier(seq.name)
      b append " as " append jdbcTypeFor(seq.tpe).sqlTypeName(None)
      seq._start.foreach { b append " start with " append _ }
      seq._increment.foreach { b append " increment by " append _ }
      seq._minValue.foreach { b append " minvalue " append _ }
      seq._maxValue.foreach { b append " maxvalue " append _ }
      if(seq._cycle) b append " cycle"
      DDL(b.toString, "drop sequence " + quoteIdentifier(seq.name))
    }
  }

  class JdbcTypes extends super.JdbcTypes {
    override val booleanJdbcType = new BooleanJdbcType
    override val uuidJdbcType = new UUIDJdbcType
    override val localDateType = new LocalDateJdbcType
    override val localDateTimeType = new LocalDateTimeJdbcType
    override val instantType = new InstantJdbcType
    override val offsetTimeType = new OffsetTimeJdbcType
    override val offsetDateTimeType = new OffsetDateTimeJdbcType
    override val zonedDateType = new ZonedDateTimeJdbcType


    class UUIDJdbcType extends super.UUIDJdbcType {
      override def sqlType = java.sql.Types.CHAR
      override def sqlTypeName(sym: Option[FieldSymbol]) = "CHAR(16) FOR BIT DATA"
    }

    /* DB2 does not have a proper BOOLEAN type. The suggested workaround is
     * a constrained CHAR with constants 1 and 0 for TRUE and FALSE. */
    class BooleanJdbcType extends super.BooleanJdbcType {
      override def sqlTypeName(sym: Option[FieldSymbol]) = "CHAR(1)"
      override def valueToSQLLiteral(value: Boolean) = if(value) "1" else "0"
    }
    class LocalDateJdbcType extends super.LocalDateJdbcType {
      override def hasLiteralForm: Boolean = true
      override def valueToSQLLiteral(value: LocalDate) : String = {
        s"TO_DATE('${value.toString}', 'SYYYY-MM-DD')"
      }
      override def getValue(r: ResultSet, idx: Int): LocalDate = {
        r.getString(idx) match {
          case null => null
          case dateStr => LocalDate.parse(dateStr.substring(0, 10))
        }
      }
    }

//    class LocalTimeJdbcType extends super.LocalTimeJdbcType {
//      @inline private[this] def timestampFromLocalTime(localTime : LocalTime) : Timestamp = {
//        Timestamp.valueOf(LocalDateTime.of(LocalDate.MIN, localTime))
//      }
//      override def sqlType = java.sql.Types.TIMESTAMP
//      override def sqlTypeName(sym: Option[FieldSymbol]) = "TIMESTAMP(6)"
//
//      override def getValue(r: ResultSet, idx: Int) : LocalTime = {
//        r.getTimestamp(idx) match {
//          case null => null
//          case timestamp => timestamp.toLocalDateTime.toLocalTime
//        }
//      }
//    }
//
//    // LocalDateTime and Instant are the 2 types which have no TZ component
//    // So, store them at UTC timestamps, otherwise the JDBC layer might attempt to map them
//    // and with DST changes, there are some times which will be unrepresentable during the switchover
//    class LocalDateTimeJdbcType extends super.LocalDateTimeJdbcType {
//      private[this] val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS x")
//      private[this] def serializeTime(v : LocalDateTime) : String = formatter.format(v.atOffset(ZoneOffset.UTC))
//      //      override def sqlType = java.sql.Types.TIMESTAMP_WITH_TIMEZONE
//      override def sqlTypeName(sym: Option[FieldSymbol]) = "TIMESTAMP(6) WITH TIME ZONE"
//      override def setValue(v: LocalDateTime, p: PreparedStatement, idx: Int) = {
//        p.setObject(idx, TimestamptzConverter.offsetDateTimeToTimestamptz(v.atOffset(ZoneOffset.UTC)), -101)
//      }
//      override def updateValue(v: LocalDateTime, r: ResultSet, idx: Int) = {
//        r.updateObject(idx, TimestamptzConverter.offsetDateTimeToTimestamptz(v.atOffset(ZoneOffset.UTC)), -101)
//      }
//      override def getValue(r: ResultSet, idx: Int): LocalDateTime = {
//        r.getObject(idx) match {
//          case null => null
//          case timestamptz => LocalDateTime.from(TimestamptzConverter.timestamptzToOffsetDateTime(timestamptz))
//
//        }
//      }
//      override def valueToSQLLiteral(value: LocalDateTime) = {
//        s"TO_TIMESTAMP_TZ('${serializeTime(value)}', 'YYYY-MM-DD HH24:MI:SS.FF3 TZH')"
//      }
//    }
//
//    class InstantJdbcType extends super.InstantJdbcType {
//      private[this] val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS x")
//      private[this] def serializeTime(v: Instant) : String = formatter.format(instantToUTC(v))
//      private[this] def instantToUTC(v: Instant): OffsetDateTime = v.atOffset(ZoneOffset.UTC)
//
//      override def sqlTypeName(sym: Option[FieldSymbol]) = "TIMESTAMP(6) WITH TIME ZONE"
//      override def setValue(v: Instant, p: PreparedStatement, idx: Int) = {
//        p.setObject(idx, TimestamptzConverter.offsetDateTimeToTimestamptz(instantToUTC(v)), -101)
//      }
//      override def updateValue(v: Instant, r: ResultSet, idx: Int) = {
//        r.updateObject(idx, TimestamptzConverter.offsetDateTimeToTimestamptz(instantToUTC(v)), -101)
//      }
//      override def getValue(r: ResultSet, idx: Int): Instant = {
//        r.getObject(idx) match {
//          case null => null
//          case timestamptz => Instant.from(TimestamptzConverter.timestamptzToOffsetDateTime(timestamptz))
//
//        }
//      }
//      override def hasLiteralForm: Boolean = true
//      override def valueToSQLLiteral(value: Instant) = {
//        s"TO_TIMESTAMP_TZ('${serializeTime(value)}', 'YYYY-MM-DD HH24:MI:SS.FF3 TZH')"
//      }
//    }
//
//    //TODO Sue Read this and make sure it works http://palashray.com/how-to-handle-oracle-timestamp-with-timezone-from-java/
//    //TODO Sue I don't think update is being tested
//
//    // No Oracle time type without date component. Add LocalDate.ofEpochDay(0), but ignore it.
//    class OffsetTimeJdbcType extends super.OffsetTimeJdbcType {
//      private[this] val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS x")
//      private[this] def serializeTime(v : OffsetTime) : String = formatter.format(v.atDate(LocalDate.ofEpochDay(0)))
//      override def sqlTypeName(sym: Option[FieldSymbol]) = "TIMESTAMP(6) WITH TIME ZONE"
//      override def setValue(v: OffsetTime, p: PreparedStatement, idx: Int) = {
//        p.setObject(idx, TimestamptzConverter.offsetTimeToTimestamptz(v), -101)
//      }
//      override def updateValue(v: OffsetTime, r: ResultSet, idx: Int) = {
//        r.updateObject(idx, TimestamptzConverter.offsetTimeToTimestamptz(v), -101)
//      }
//      override def getValue(r: ResultSet, idx: Int): OffsetTime = {
//        TimestamptzConverter.timestamptzToOffsetTime(r.getObject(idx))
//      }
//      override def hasLiteralForm: Boolean = true
//      override def valueToSQLLiteral(value: OffsetTime) = {
//        s"TO_TIMESTAMP_TZ('${serializeTime(value)}', 'YYYY-MM-DD HH24:MI:SS.FF3 TZH')"
//      }
//    }
//
//    class OffsetDateTimeJdbcType extends super.OffsetDateTimeJdbcType {
//      private[this] val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS x")
//      private[this] def serializeTime(v : OffsetDateTime) : String = formatter.format(v)
//      override def sqlTypeName(sym: Option[FieldSymbol]) = "TIMESTAMP(6) WITH TIME ZONE"
//      override def setValue(v: OffsetDateTime, p: PreparedStatement, idx: Int) = {
//        p.setObject(idx, TimestamptzConverter.offsetDateTimeToTimestamptz(v), -101)
//      }
//      override def updateValue(v: OffsetDateTime, r: ResultSet, idx: Int) = {
//        r.updateObject(idx, TimestamptzConverter.offsetDateTimeToTimestamptz(v), -101)
//      }
//      override def getValue(r: ResultSet, idx: Int): OffsetDateTime = {
//        TimestamptzConverter.timestamptzToOffsetDateTime(r.getObject(idx))
//      }
//      override def hasLiteralForm: Boolean = true
//      override def valueToSQLLiteral(value: OffsetDateTime) = {
//        s"TO_TIMESTAMP_TZ('${serializeTime(value)}', 'YYYY-MM-DD HH24:MI:SS.FF3 TZH')"
//      }
//    }
//
//    class ZonedDateTimeJdbcType extends super.ZonedDateTimeJdbcType {
//      private[this] val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS VV")
//      private[this] def serializeTime(v : ZonedDateTime) : String = formatter.format(v)
//      override def sqlTypeName(sym: Option[FieldSymbol]) = "TIMESTAMP(6) WITH TIME ZONE"
//      override def setValue(v: ZonedDateTime, p: PreparedStatement, idx: Int) = {
//        p.setObject(idx, TimestamptzConverter.zonedDateTimeToTimestamptz(v), -101)
//      }
//      override def updateValue(v: ZonedDateTime, r: ResultSet, idx: Int) = {
//        r.updateObject(idx, TimestamptzConverter.zonedDateTimeToTimestamptz(v), -101)
//      }
//      override def getValue(r: ResultSet, idx: Int): ZonedDateTime = {
//        TimestamptzConverter.timestamptzToZonedDateTime(r.getObject(idx))
//      }
//      override def hasLiteralForm: Boolean = true
//      override def valueToSQLLiteral(value: ZonedDateTime) = {
//        s"TO_TIMESTAMP_TZ('${serializeTime(value)}', 'YYYY-MM-DD HH24:MI:SS.FF3 TZR')"
//      }
//    }
//
  }

}

object DB2Profile extends DB2Profile
