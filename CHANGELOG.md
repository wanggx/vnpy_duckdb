# 0.1.0版本

1. 基于DuckDB实现VeighNa数据库接口，支持K线和Tick数据的读取、写入、删除与汇总查询
2. 采用批量staging写入与INSERT OR REPLACE去重，并支持stream流式写入参数
3. 复用框架convert_tz/DB_TZ进行时区处理，与其他数据库接口的行为保持一致
4. **数值列全部改为 `DECIMAL(18,2)`**（原 `DOUBLE`）：QMT 返回的值有长小数——
   既有前复权价的真实精度（`12.533842189676422`），也有二进制浮点尾巴
   （`11.540000000000001`）。DECIMAL 是定点数（整数 + 标度 2），写入即按 2 位小数
   四舍五入，任何输入都不会留下第 3 位。覆盖 `dbbardata` 的
   volume/turnover/open_interest/open·high·low·close_price，以及 `dbtickdata` 的同名
   数值列 + last_price/last_volume/limit_up/limit_down/pre_close/bid·ask_price_1..5/
   bid·ask_volume_1..5。
   读路径由 `select_columns()` 在 SQL 里 `CAST(... AS DOUBLE)`，交给 DuckDB 在 C++ 层
   转换：实测 1 万行 0.030s，与 DOUBLE 列同速；直接 fetch DECIMAL 会返回 Python
   `Decimal`（0.156s，5 倍慢），再在 Python 里 float() 更慢（0.207s）。
   已有库在打开时自动迁移（`_migrate_numeric_types()`：按 information_schema 比对，
   只对非 DECIMAL 的列执行一次 `ALTER COLUMN ... TYPE DECIMAL(18,2)`，幂等；
   实测 579 万行日线库迁移 0.7s）。
   精度影响（本机 579 万行实测）：74.1% 的行被舍入，最大绝对误差 0.004988，
   平均相对误差 0.0277%。注意：ETF/LOF/债券等 3 位小数报价会被舍到 2 位。
