# 0.1.0版本

1. 基于DuckDB实现VeighNa数据库接口，支持K线和Tick数据的读取、写入、删除与汇总查询
2. 采用批量staging写入与INSERT OR REPLACE去重，并支持stream流式写入参数
3. 复用框架convert_tz/DB_TZ进行时区处理，与其他数据库接口的行为保持一致
4. **价格列改为 `DECIMAL(18,2)`**（原 `DOUBLE`）：QMT 返回的复权价/成交价有 6-10 位
   小数，直接存 DOUBLE 会让库里到处是长小数。改类型后写入即按 2 位小数四舍五入，
   SQL 查询/导出干净。涉及 `dbbardata` 的 open/high/low/close_price 与 `dbtickdata`
   的 last/limit_up/limit_down/open/high/low/pre_close、bid/ask_price_1..5；
   `_to_float()` 在读边界把 `Decimal` 转回 float，下游算法不受影响。
   已有库在打开时自动迁移（`_migrate_price_types()`：按 information_schema 比对，
   只对 DOUBLE 列执行一次 `ALTER COLUMN ... TYPE DECIMAL(18,2)`，幂等）。
   注意：ETF/LOF/债券等 3 位小数报价的品种会被舍到 2 位。
