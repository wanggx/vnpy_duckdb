from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any

import duckdb
import pandas as pd

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.database import (
    BarOverview,
    BaseDatabase,
    DB_TZ,
    TickOverview,
    convert_tz,
)
from vnpy.trader.object import BarData, TickData
from vnpy.trader.setting import SETTINGS
from vnpy.trader.utility import get_file_path


BAR_COLUMNS: list[str] = [
    "symbol",
    "exchange",
    "datetime",
    "interval",
    "volume",
    "turnover",
    "open_interest",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
]

TICK_COLUMNS: list[str] = [
    "symbol",
    "exchange",
    "datetime",
    "name",
    "volume",
    "turnover",
    "open_interest",
    "last_price",
    "last_volume",
    "limit_up",
    "limit_down",
    "open_price",
    "high_price",
    "low_price",
    "pre_close",
    "bid_price_1",
    "bid_price_2",
    "bid_price_3",
    "bid_price_4",
    "bid_price_5",
    "ask_price_1",
    "ask_price_2",
    "ask_price_3",
    "ask_price_4",
    "ask_price_5",
    "bid_volume_1",
    "bid_volume_2",
    "bid_volume_3",
    "bid_volume_4",
    "bid_volume_5",
    "ask_volume_1",
    "ask_volume_2",
    "ask_volume_3",
    "ask_volume_4",
    "ask_volume_5",
    "localtime",
]

BAR_KEY: list[str] = ["symbol", "exchange", "interval", "datetime"]
TICK_KEY: list[str] = ["symbol", "exchange", "datetime"]

# 价格列的定点精度：存 DECIMAL(18,2) 而不是 DOUBLE。
# DOUBLE 会把 QMT 的复权价/成交价原封不动存下来（如 10.123456789），库里到处是长
# 小数，SQL 查询/导出一片噪声；DECIMAL(18,2) 在写入时即按 2 位小数四舍五入，
# 读取端统一转回 float（见 load_bar_data / load_tick_data），下游算法不受影响。
# 注意：ETF/LOF/债券等 3 位小数报价的品种会被舍到 2 位。
PRICE_TYPE: str = "DECIMAL(18,2)"

PRICE_COLUMNS: dict[str, tuple[str, ...]] = {
    "dbbardata": ("open_price", "high_price", "low_price", "close_price"),
    "dbtickdata": (
        "last_price", "limit_up", "limit_down",
        "open_price", "high_price", "low_price", "pre_close",
        "bid_price_1", "bid_price_2", "bid_price_3", "bid_price_4", "bid_price_5",
        "ask_price_1", "ask_price_2", "ask_price_3", "ask_price_4", "ask_price_5",
    ),
}


def select_columns(table: str) -> str:
    """SELECT 列表：价格列在 SQL 里 CAST 回 DOUBLE，其余列原样。

    DECIMAL 列直接 fetch 回来是 Python ``Decimal``（实测 1 万行 0.156s，是 DOUBLE
    的 5 倍；再在 Python 里 float() 一次更慢，0.207s）。交给 DuckDB 在 C++ 层
    ``CAST(... AS DOUBLE)`` 后，取数耗时与 DOUBLE 列相同（0.030s），BarData /
    TickData 也直接拿到 float。
    """
    columns: list[str] = BAR_COLUMNS if table == "dbbardata" else TICK_COLUMNS
    prices: frozenset[str] = frozenset(PRICE_COLUMNS[table])
    return ", ".join(
        f"CAST({column} AS DOUBLE) AS {column}" if column in prices else column
        for column in columns
    )


class DuckdbDatabase(BaseDatabase):
    """DuckDB database interface."""

    def __init__(self, database: str | Path | None = None) -> None:
        """"""
        filename: str | Path = database or SETTINGS["database.database"] or "database.duckdb"
        self.path: Path = get_file_path(str(filename))
        self.lock: RLock = RLock()
        self.conn: duckdb.DuckDBPyConnection = duckdb.connect(str(self.path))

        self._init_tables()

    def _init_tables(self) -> None:
        """Create tables if they do not exist."""
        with self.lock:
            cursor: duckdb.DuckDBPyConnection = self.conn.cursor()
            try:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dbbardata (
                        symbol VARCHAR NOT NULL,
                        exchange VARCHAR NOT NULL,
                        datetime TIMESTAMP NOT NULL,
                        interval VARCHAR NOT NULL,
                        volume DOUBLE NOT NULL,
                        turnover DOUBLE NOT NULL,
                        open_interest DOUBLE NOT NULL,
                        open_price DECIMAL(18,2) NOT NULL,
                        high_price DECIMAL(18,2) NOT NULL,
                        low_price DECIMAL(18,2) NOT NULL,
                        close_price DECIMAL(18,2) NOT NULL,
                        PRIMARY KEY (symbol, exchange, interval, datetime)
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dbtickdata (
                        symbol VARCHAR NOT NULL,
                        exchange VARCHAR NOT NULL,
                        datetime TIMESTAMP NOT NULL,
                        name VARCHAR NOT NULL,
                        volume DOUBLE NOT NULL,
                        turnover DOUBLE NOT NULL,
                        open_interest DOUBLE NOT NULL,
                        last_price DECIMAL(18,2) NOT NULL,
                        last_volume DOUBLE NOT NULL,
                        limit_up DECIMAL(18,2) NOT NULL,
                        limit_down DECIMAL(18,2) NOT NULL,
                        open_price DECIMAL(18,2) NOT NULL,
                        high_price DECIMAL(18,2) NOT NULL,
                        low_price DECIMAL(18,2) NOT NULL,
                        pre_close DECIMAL(18,2) NOT NULL,
                        bid_price_1 DECIMAL(18,2) NOT NULL,
                        bid_price_2 DECIMAL(18,2) NOT NULL,
                        bid_price_3 DECIMAL(18,2) NOT NULL,
                        bid_price_4 DECIMAL(18,2) NOT NULL,
                        bid_price_5 DECIMAL(18,2) NOT NULL,
                        ask_price_1 DECIMAL(18,2) NOT NULL,
                        ask_price_2 DECIMAL(18,2) NOT NULL,
                        ask_price_3 DECIMAL(18,2) NOT NULL,
                        ask_price_4 DECIMAL(18,2) NOT NULL,
                        ask_price_5 DECIMAL(18,2) NOT NULL,
                        bid_volume_1 DOUBLE NOT NULL,
                        bid_volume_2 DOUBLE NOT NULL,
                        bid_volume_3 DOUBLE NOT NULL,
                        bid_volume_4 DOUBLE NOT NULL,
                        bid_volume_5 DOUBLE NOT NULL,
                        ask_volume_1 DOUBLE NOT NULL,
                        ask_volume_2 DOUBLE NOT NULL,
                        ask_volume_3 DOUBLE NOT NULL,
                        ask_volume_4 DOUBLE NOT NULL,
                        ask_volume_5 DOUBLE NOT NULL,
                        localtime TIMESTAMP,
                        PRIMARY KEY (symbol, exchange, datetime)
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dbbaroverview (
                        symbol VARCHAR NOT NULL,
                        exchange VARCHAR NOT NULL,
                        interval VARCHAR NOT NULL,
                        count INTEGER NOT NULL,
                        start TIMESTAMP NOT NULL,
                        "end" TIMESTAMP NOT NULL,
                        PRIMARY KEY (symbol, exchange, interval)
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dbtickoverview (
                        symbol VARCHAR NOT NULL,
                        exchange VARCHAR NOT NULL,
                        count INTEGER NOT NULL,
                        start TIMESTAMP NOT NULL,
                        "end" TIMESTAMP NOT NULL,
                        PRIMARY KEY (symbol, exchange)
                    )
                    """
                )
                self._migrate_price_types(cursor)
            finally:
                cursor.close()

    def _migrate_price_types(self, cursor: duckdb.DuckDBPyConnection) -> list[str]:
        """把旧库里 DOUBLE 的价格列原地改成 DECIMAL(18,2)（幂等）。

        新建的库在建表时已经是 DECIMAL；早期建的库那几列是 DOUBLE，`CREATE TABLE
        IF NOT EXISTS` 不会改已有表，所以这里按 information_schema 比对后 ALTER。
        ALTER 会重写该列，已存的长小数按列类型四舍五入到 2 位——只需要跑一次。
        返回被改动的 ``表.列`` 列表（无改动时为空）。
        """
        altered: list[str] = []
        for table, columns in PRICE_COLUMNS.items():
            placeholders: str = ", ".join("?" for _ in columns)
            rows: list[tuple] = cursor.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                f"WHERE table_name = ? AND column_name IN ({placeholders})",
                (table, *columns),
            ).fetchall()
            for column, data_type in rows:
                if str(data_type).upper() == PRICE_TYPE:
                    continue
                cursor.execute(
                    f"ALTER TABLE {table} ALTER COLUMN {column} TYPE {PRICE_TYPE}"
                )
                altered.append(f"{table}.{column}")
        return altered

    def save_bar_data(self, bars: list[BarData], stream: bool = False) -> bool:
        """Save bar data into database."""
        if not bars:
            return False

        bar: BarData = bars[0]
        symbol: str = bar.symbol
        exchange: Exchange = bar.exchange
        interval: Interval | None = bar.interval
        if not interval:
            return False

        rows: list[dict[str, Any]] = [self._bar_to_dict(bar) for bar in bars]
        df: pd.DataFrame = pd.DataFrame(rows, columns=BAR_COLUMNS)
        df = df.drop_duplicates(subset=BAR_KEY, keep="last")
        unique_count: int = len(df)
        start: datetime = df["datetime"].min().to_pydatetime()
        end: datetime = df["datetime"].max().to_pydatetime()

        with self.lock:
            cursor: duckdb.DuckDBPyConnection = self.conn.cursor()
            try:
                self._register_and_write_bar(
                    cursor,
                    df,
                    symbol,
                    exchange.value,
                    interval.value,
                    start,
                    end,
                    unique_count,
                    stream,
                )
            finally:
                cursor.close()

        return True

    def save_tick_data(self, ticks: list[TickData], stream: bool = False) -> bool:
        """Save tick data into database."""
        if not ticks:
            return False

        tick: TickData = ticks[0]
        symbol: str = tick.symbol
        exchange: Exchange = tick.exchange

        rows: list[dict[str, Any]] = [self._tick_to_dict(tick) for tick in ticks]
        df: pd.DataFrame = pd.DataFrame(rows, columns=TICK_COLUMNS)
        df = df.drop_duplicates(subset=TICK_KEY, keep="last")
        unique_count: int = len(df)
        start: datetime = df["datetime"].min().to_pydatetime()
        end: datetime = df["datetime"].max().to_pydatetime()

        with self.lock:
            cursor: duckdb.DuckDBPyConnection = self.conn.cursor()
            try:
                self._register_and_write_tick(
                    cursor,
                    df,
                    symbol,
                    exchange.value,
                    start,
                    end,
                    unique_count,
                    stream,
                )
            finally:
                cursor.close()

        return True

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime,
    ) -> list[BarData]:
        """Load bar data from database."""
        sql: str = f"""
            SELECT {select_columns("dbbardata")}
            FROM dbbardata
            WHERE symbol = ?
              AND exchange = ?
              AND interval = ?
              AND datetime >= ?
              AND datetime <= ?
            ORDER BY datetime
        """
        params: tuple = (
            symbol,
            exchange.value,
            interval.value,
            self._normalize_query_datetime(start),
            self._normalize_query_datetime(end),
        )

        with self.lock:
            cursor: duckdb.DuckDBPyConnection = self.conn.cursor()
            try:
                rows: list[tuple] = cursor.execute(sql, params).fetchall()
            finally:
                cursor.close()

        bars: list[BarData] = []
        for row in rows:
            bar: BarData = BarData(
                symbol=row[0],
                exchange=Exchange(row[1]),
                datetime=self._restore_datetime(row[2]),
                interval=Interval(row[3]),
                volume=row[4],
                turnover=row[5],
                open_interest=row[6],
                open_price=self._to_float(row[7]),
                high_price=self._to_float(row[8]),
                low_price=self._to_float(row[9]),
                close_price=self._to_float(row[10]),
                gateway_name="DB",
            )
            bars.append(bar)

        return bars

    def load_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime,
    ) -> list[TickData]:
        """Load tick data from database."""
        columns: str = select_columns("dbtickdata")
        sql: str = f"""
            SELECT {columns}
            FROM dbtickdata
            WHERE symbol = ?
              AND exchange = ?
              AND datetime >= ?
              AND datetime <= ?
            ORDER BY datetime
        """
        params: tuple = (
            symbol,
            exchange.value,
            self._normalize_query_datetime(start),
            self._normalize_query_datetime(end),
        )

        with self.lock:
            cursor: duckdb.DuckDBPyConnection = self.conn.cursor()
            try:
                rows: list[tuple] = cursor.execute(sql, params).fetchall()
            finally:
                cursor.close()

        ticks: list[TickData] = []
        for row in rows:
            data: dict[str, Any] = dict(zip(TICK_COLUMNS, row, strict=True))
            for column in PRICE_COLUMNS["dbtickdata"]:
                data[column] = self._to_float(data[column])
            tick: TickData = TickData(
                symbol=data["symbol"],
                exchange=Exchange(data["exchange"]),
                datetime=self._restore_datetime(data["datetime"]),
                name=data["name"],
                volume=data["volume"],
                turnover=data["turnover"],
                open_interest=data["open_interest"],
                last_price=data["last_price"],
                last_volume=data["last_volume"],
                limit_up=data["limit_up"],
                limit_down=data["limit_down"],
                open_price=data["open_price"],
                high_price=data["high_price"],
                low_price=data["low_price"],
                pre_close=data["pre_close"],
                bid_price_1=data["bid_price_1"],
                bid_price_2=data["bid_price_2"],
                bid_price_3=data["bid_price_3"],
                bid_price_4=data["bid_price_4"],
                bid_price_5=data["bid_price_5"],
                ask_price_1=data["ask_price_1"],
                ask_price_2=data["ask_price_2"],
                ask_price_3=data["ask_price_3"],
                ask_price_4=data["ask_price_4"],
                ask_price_5=data["ask_price_5"],
                bid_volume_1=data["bid_volume_1"],
                bid_volume_2=data["bid_volume_2"],
                bid_volume_3=data["bid_volume_3"],
                bid_volume_4=data["bid_volume_4"],
                bid_volume_5=data["bid_volume_5"],
                ask_volume_1=data["ask_volume_1"],
                ask_volume_2=data["ask_volume_2"],
                ask_volume_3=data["ask_volume_3"],
                ask_volume_4=data["ask_volume_4"],
                ask_volume_5=data["ask_volume_5"],
                localtime=data["localtime"],
                gateway_name="DB",
            )
            ticks.append(tick)

        return ticks

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
    ) -> int:
        """Delete all bar data with given symbol + exchange + interval."""
        with self.lock:
            cursor: duckdb.DuckDBPyConnection = self.conn.cursor()
            try:
                cursor.execute("BEGIN TRANSACTION")
                try:
                    count: int = self._fetch_scalar_int(
                        cursor,
                        """
                        SELECT count(*)
                        FROM dbbardata
                        WHERE symbol = ? AND exchange = ? AND interval = ?
                        """,
                        (symbol, exchange.value, interval.value),
                    )
                    cursor.execute(
                        """
                        DELETE FROM dbbardata
                        WHERE symbol = ? AND exchange = ? AND interval = ?
                        """,
                        (symbol, exchange.value, interval.value),
                    )
                    cursor.execute(
                        """
                        DELETE FROM dbbaroverview
                        WHERE symbol = ? AND exchange = ? AND interval = ?
                        """,
                        (symbol, exchange.value, interval.value),
                    )
                    cursor.execute("COMMIT")
                except Exception:
                    cursor.execute("ROLLBACK")
                    raise
            finally:
                cursor.close()

        return count

    def delete_tick_data(self, symbol: str, exchange: Exchange) -> int:
        """Delete all tick data with given symbol + exchange."""
        with self.lock:
            cursor: duckdb.DuckDBPyConnection = self.conn.cursor()
            try:
                cursor.execute("BEGIN TRANSACTION")
                try:
                    count: int = self._fetch_scalar_int(
                        cursor,
                        """
                        SELECT count(*)
                        FROM dbtickdata
                        WHERE symbol = ? AND exchange = ?
                        """,
                        (symbol, exchange.value),
                    )
                    cursor.execute(
                        """
                        DELETE FROM dbtickdata
                        WHERE symbol = ? AND exchange = ?
                        """,
                        (symbol, exchange.value),
                    )
                    cursor.execute(
                        """
                        DELETE FROM dbtickoverview
                        WHERE symbol = ? AND exchange = ?
                        """,
                        (symbol, exchange.value),
                    )
                    cursor.execute("COMMIT")
                except Exception:
                    cursor.execute("ROLLBACK")
                    raise
            finally:
                cursor.close()

        return count

    def get_bar_overview(self) -> list[BarOverview]:
        """Return bar data available in database."""
        with self.lock:
            cursor: duckdb.DuckDBPyConnection = self.conn.cursor()
            try:
                data_count: int = self._fetch_scalar_int(cursor, "SELECT count(*) FROM dbbardata")
                overview_count: int = self._fetch_scalar_int(cursor, "SELECT count(*) FROM dbbaroverview")
                if data_count and not overview_count:
                    self.init_bar_overview(cursor)

                rows: list[tuple] = cursor.execute(
                    """
                    SELECT symbol, exchange, interval, count, start, "end"
                    FROM dbbaroverview
                    ORDER BY symbol, exchange, interval
                    """
                ).fetchall()
            finally:
                cursor.close()

        return [
            BarOverview(
                symbol=row[0],
                exchange=Exchange(row[1]),
                interval=Interval(row[2]),
                count=row[3],
                start=self._restore_datetime(row[4]),
                end=self._restore_datetime(row[5]),
            )
            for row in rows
        ]

    def get_tick_overview(self) -> list[TickOverview]:
        """Return tick data available in database."""
        with self.lock:
            cursor: duckdb.DuckDBPyConnection = self.conn.cursor()
            try:
                data_count: int = self._fetch_scalar_int(cursor, "SELECT count(*) FROM dbtickdata")
                overview_count: int = self._fetch_scalar_int(cursor, "SELECT count(*) FROM dbtickoverview")
                if data_count and not overview_count:
                    self.init_tick_overview(cursor)

                rows: list[tuple] = cursor.execute(
                    """
                    SELECT symbol, exchange, count, start, "end"
                    FROM dbtickoverview
                    ORDER BY symbol, exchange
                    """
                ).fetchall()
            finally:
                cursor.close()

        return [
            TickOverview(
                symbol=row[0],
                exchange=Exchange(row[1]),
                count=row[2],
                start=self._restore_datetime(row[3]),
                end=self._restore_datetime(row[4]),
            )
            for row in rows
        ]

    def init_bar_overview(self, cursor: duckdb.DuckDBPyConnection) -> None:
        """Initialize bar overview from existing bar data."""
        with self.lock:
            cursor.execute(
                """
                INSERT OR REPLACE INTO dbbaroverview BY NAME
                SELECT
                    symbol,
                    exchange,
                    interval,
                    count(*)::INTEGER AS count,
                    min(datetime) AS start,
                    max(datetime) AS "end"
                FROM dbbardata
                GROUP BY symbol, exchange, interval
                """
            )

    def init_tick_overview(self, cursor: duckdb.DuckDBPyConnection) -> None:
        """Initialize tick overview from existing tick data."""
        with self.lock:
            cursor.execute(
                """
                INSERT OR REPLACE INTO dbtickoverview BY NAME
                SELECT
                    symbol,
                    exchange,
                    count(*)::INTEGER AS count,
                    min(datetime) AS start,
                    max(datetime) AS "end"
                FROM dbtickdata
                GROUP BY symbol, exchange
                """
            )

    def close(self) -> None:
        """Close database connection."""
        with self.lock:
            self.conn.close()

    def _register_and_write_bar(
        self,
        cursor: duckdb.DuckDBPyConnection,
        df: pd.DataFrame,
        symbol: str,
        exchange: str,
        interval: str,
        start: datetime,
        end: datetime,
        count: int,
        stream: bool,
    ) -> None:
        """Register bar DataFrame and upsert it with overview in one transaction."""
        cursor.register("bar_staging", df)
        cursor.execute("BEGIN TRANSACTION")
        try:
            cursor.execute("INSERT OR REPLACE INTO dbbardata BY NAME SELECT * FROM bar_staging")
            if stream:
                self._upsert_stream_bar_overview(cursor, symbol, exchange, interval, start, end, count)
            else:
                self._refresh_bar_overview(cursor, symbol, exchange, interval)
            cursor.execute("COMMIT")
        except Exception:
            cursor.execute("ROLLBACK")
            raise
        finally:
            cursor.unregister("bar_staging")

    def _register_and_write_tick(
        self,
        cursor: duckdb.DuckDBPyConnection,
        df: pd.DataFrame,
        symbol: str,
        exchange: str,
        start: datetime,
        end: datetime,
        count: int,
        stream: bool,
    ) -> None:
        """Register tick DataFrame and upsert it with overview in one transaction."""
        cursor.register("tick_staging", df)
        cursor.execute("BEGIN TRANSACTION")
        try:
            cursor.execute("INSERT OR REPLACE INTO dbtickdata BY NAME SELECT * FROM tick_staging")
            if stream:
                self._upsert_stream_tick_overview(cursor, symbol, exchange, start, end, count)
            else:
                self._refresh_tick_overview(cursor, symbol, exchange)
            cursor.execute("COMMIT")
        except Exception:
            cursor.execute("ROLLBACK")
            raise
        finally:
            cursor.unregister("tick_staging")

    def _upsert_stream_bar_overview(
        self,
        cursor: duckdb.DuckDBPyConnection,
        symbol: str,
        exchange: str,
        interval: str,
        start: datetime,
        end: datetime,
        count: int,
    ) -> None:
        """Incrementally update bar overview for streaming writes."""
        row: tuple | None = cursor.execute(
            """
            SELECT count, start, "end"
            FROM dbbaroverview
            WHERE symbol = ? AND exchange = ? AND interval = ?
            """,
            (symbol, exchange, interval),
        ).fetchone()

        if row:
            overview_count, overview_start, overview_end = row
            cursor.execute(
                """
                UPDATE dbbaroverview
                SET count = ?, start = ?, "end" = ?
                WHERE symbol = ? AND exchange = ? AND interval = ?
                """,
                (
                    overview_count + count,
                    min(overview_start, start),
                    max(overview_end, end),
                    symbol,
                    exchange,
                    interval,
                ),
            )
        else:
            cursor.execute(
                """
                INSERT INTO dbbaroverview
                    (symbol, exchange, interval, count, start, "end")
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (symbol, exchange, interval, count, start, end),
            )

    def _upsert_stream_tick_overview(
        self,
        cursor: duckdb.DuckDBPyConnection,
        symbol: str,
        exchange: str,
        start: datetime,
        end: datetime,
        count: int,
    ) -> None:
        """Incrementally update tick overview for streaming writes."""
        row: tuple | None = cursor.execute(
            """
            SELECT count, start, "end"
            FROM dbtickoverview
            WHERE symbol = ? AND exchange = ?
            """,
            (symbol, exchange),
        ).fetchone()

        if row:
            overview_count, overview_start, overview_end = row
            cursor.execute(
                """
                UPDATE dbtickoverview
                SET count = ?, start = ?, "end" = ?
                WHERE symbol = ? AND exchange = ?
                """,
                (
                    overview_count + count,
                    min(overview_start, start),
                    max(overview_end, end),
                    symbol,
                    exchange,
                ),
            )
        else:
            cursor.execute(
                """
                INSERT INTO dbtickoverview
                    (symbol, exchange, count, start, "end")
                VALUES (?, ?, ?, ?, ?)
                """,
                (symbol, exchange, count, start, end),
            )

    def _refresh_bar_overview(self, cursor: duckdb.DuckDBPyConnection, symbol: str, exchange: str, interval: str) -> None:
        """Refresh bar overview with an exact count."""
        row: tuple | None = cursor.execute(
            """
            SELECT count(*)::INTEGER, min(datetime), max(datetime)
            FROM dbbardata
            WHERE symbol = ? AND exchange = ? AND interval = ?
            """,
            (symbol, exchange, interval),
        ).fetchone()
        if row is None:
            raise RuntimeError("Expected bar overview query to return one row.")

        count, start, end = row
        if count:
            cursor.execute(
                """
                INSERT OR REPLACE INTO dbbaroverview
                    (symbol, exchange, interval, count, start, "end")
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (symbol, exchange, interval, count, start, end),
            )
        else:
            cursor.execute(
                """
                DELETE FROM dbbaroverview
                WHERE symbol = ? AND exchange = ? AND interval = ?
                """,
                (symbol, exchange, interval),
            )

    def _refresh_tick_overview(self, cursor: duckdb.DuckDBPyConnection, symbol: str, exchange: str) -> None:
        """Refresh tick overview with an exact count."""
        row: tuple | None = cursor.execute(
            """
            SELECT count(*)::INTEGER, min(datetime), max(datetime)
            FROM dbtickdata
            WHERE symbol = ? AND exchange = ?
            """,
            (symbol, exchange),
        ).fetchone()
        if row is None:
            raise RuntimeError("Expected tick overview query to return one row.")

        count, start, end = row
        if count:
            cursor.execute(
                """
                INSERT OR REPLACE INTO dbtickoverview
                    (symbol, exchange, count, start, "end")
                VALUES (?, ?, ?, ?, ?)
                """,
                (symbol, exchange, count, start, end),
            )
        else:
            cursor.execute(
                """
                DELETE FROM dbtickoverview
                WHERE symbol = ? AND exchange = ?
                """,
                (symbol, exchange),
            )

    @staticmethod
    def _bar_to_dict(bar: BarData) -> dict[str, Any]:
        """Convert BarData to a database row."""
        interval: Interval | None = bar.interval
        if interval is None:
            raise ValueError("BarData interval is required.")

        return {
            "symbol": bar.symbol,
            "exchange": bar.exchange.value,
            "datetime": convert_tz(bar.datetime),
            "interval": interval.value,
            "volume": bar.volume,
            "turnover": bar.turnover,
            "open_interest": bar.open_interest,
            "open_price": bar.open_price,
            "high_price": bar.high_price,
            "low_price": bar.low_price,
            "close_price": bar.close_price,
        }

    @staticmethod
    def _tick_to_dict(tick: TickData) -> dict[str, Any]:
        """Convert TickData to a database row."""
        return {
            "symbol": tick.symbol,
            "exchange": tick.exchange.value,
            "datetime": convert_tz(tick.datetime),
            "name": tick.name,
            "volume": tick.volume,
            "turnover": tick.turnover,
            "open_interest": tick.open_interest,
            "last_price": tick.last_price,
            "last_volume": tick.last_volume,
            "limit_up": tick.limit_up,
            "limit_down": tick.limit_down,
            "open_price": tick.open_price,
            "high_price": tick.high_price,
            "low_price": tick.low_price,
            "pre_close": tick.pre_close,
            "bid_price_1": tick.bid_price_1,
            "bid_price_2": tick.bid_price_2,
            "bid_price_3": tick.bid_price_3,
            "bid_price_4": tick.bid_price_4,
            "bid_price_5": tick.bid_price_5,
            "ask_price_1": tick.ask_price_1,
            "ask_price_2": tick.ask_price_2,
            "ask_price_3": tick.ask_price_3,
            "ask_price_4": tick.ask_price_4,
            "ask_price_5": tick.ask_price_5,
            "bid_volume_1": tick.bid_volume_1,
            "bid_volume_2": tick.bid_volume_2,
            "bid_volume_3": tick.bid_volume_3,
            "bid_volume_4": tick.bid_volume_4,
            "bid_volume_5": tick.bid_volume_5,
            "ask_volume_1": tick.ask_volume_1,
            "ask_volume_2": tick.ask_volume_2,
            "ask_volume_3": tick.ask_volume_3,
            "ask_volume_4": tick.ask_volume_4,
            "ask_volume_5": tick.ask_volume_5,
            "localtime": DuckdbDatabase._normalize_optional_datetime(tick.localtime),
        }

    @staticmethod
    def _normalize_query_datetime(dt: datetime) -> datetime:
        """Convert query datetime to the DB timezone's naive timestamp."""
        if dt.tzinfo:
            return convert_tz(dt)
        return dt

    @staticmethod
    def _normalize_optional_datetime(dt: datetime | None) -> datetime | None:
        """Normalize optional datetime values before storing."""
        if dt and dt.tzinfo:
            return convert_tz(dt)
        return dt

    def _fetch_scalar_int(self, cursor: duckdb.DuckDBPyConnection, sql: str, params: tuple[Any, ...] = ()) -> int:
        """Fetch an integer scalar from a query that must return one row."""
        row: tuple[Any, ...] | None = cursor.execute(sql, params).fetchone()
        if row is None:
            raise RuntimeError("Expected scalar query to return one row.")
        return int(row[0])

    @staticmethod
    def _restore_datetime(dt: datetime) -> datetime:
        """Restore a stored naive timestamp as a DB timezone-aware datetime."""
        return dt.replace(tzinfo=DB_TZ)

    @staticmethod
    def _to_float(value: Any) -> float:
        """把 DECIMAL 列读回来的 ``Decimal`` 转成 float。

        价格列是 DECIMAL(18,2)，驱动返回 ``decimal.Decimal``；vnpy 的 BarData /
        TickData 以及下游策略都按 float 计算，Decimal 混入 float 运算会抛
        TypeError，所以在读边界就地转掉。
        """
        if value is None:
            return 0.0
        if isinstance(value, float):
            return value
        return float(value)
