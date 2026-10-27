"""Storage engines. SQL in mdm/sql is written in the subset shared by Spark SQL
and DuckDB, so the same checks run on Databricks and on a laptop."""

from pathlib import Path

import pandas as pd

from .config import ROOT

SQL_DIR = Path(__file__).parent / "sql"

# Explicit schemas for tables whose columns can be all-NULL on a given day
# (type inference would otherwise lock in a NULL/void column).
SCHEMAS = {
    "flags": "check_name STRING, severity STRING, ticker STRING, trade_date DATE, contract STRING, "
             "value DOUBLE, threshold DOUBLE, detail STRING, stage STRING, as_of DATE",
    "digests": "as_of DATE, source STRING, model STRING, attempts INT, verified BOOLEAN, "
               "rejected_errors STRING, posted BOOLEAN, text STRING, facts_json STRING, created_at TIMESTAMP",
}


def render_sql(text: str, **params) -> str:
    """Fill {placeholders} and drop -- comments (so ';' in a comment can't split a statement)."""
    lines = [ln for ln in text.splitlines() if not ln.lstrip().startswith("--")]
    return "\n".join(lines).format(**params)


class Engine:
    ns: str

    def t(self, name: str) -> str:
        return f"{self.ns}.{name}"

    def sql(self, query: str) -> pd.DataFrame:
        raise NotImplementedError

    def exists(self, table: str) -> bool:
        raise NotImplementedError

    def write(self, df: pd.DataFrame, table: str, replace_where: str | None = None) -> None:
        """Idempotent write: delete rows matching replace_where, then append.
        replace_where=None replaces the whole table."""
        raise NotImplementedError

    def render(self, sql_file: str, **params) -> str:
        return render_sql((SQL_DIR / sql_file).read_text(), ns=self.ns, **params)

    def run_file(self, sql_file: str, **params) -> pd.DataFrame:
        return self.sql(self.render(sql_file, **params))


class DuckDBEngine(Engine):
    def __init__(self, path: str, schema: str):
        import duckdb

        if path == ":memory:":
            self.con = duckdb.connect()
        else:
            p = Path(path)
            if not p.is_absolute():
                p = ROOT / p
            p.parent.mkdir(parents=True, exist_ok=True)
            self.con = duckdb.connect(str(p))
        self.ns = schema
        self._exec(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def _exec(self, q):
        # fetch so DuckDB doesn't keep a pending result open on the connection
        return self.con.execute(q).fetchall()

    def sql(self, query):
        return self.con.execute(query).df()

    def exists(self, table):
        n = self.con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
            [self.ns, table],
        ).fetchall()[0][0]
        return n > 0

    def write(self, df, table, replace_where=None):
        if df.empty and self.exists(table):
            if replace_where:
                self._exec(f"DELETE FROM {self.t(table)} WHERE {replace_where}")
            return
        if table in SCHEMAS and not self.exists(table):
            self._exec(f"CREATE TABLE {self.t(table)} ({SCHEMAS[table]})")
        self.con.register("_incoming", df)
        try:
            if not self.exists(table):
                self._exec(f"CREATE TABLE {self.t(table)} AS SELECT * FROM _incoming")
                return
            where = replace_where or "TRUE"
            self._exec(f"DELETE FROM {self.t(table)} WHERE {where}")
            self._exec(f"INSERT INTO {self.t(table)} BY NAME SELECT * FROM _incoming")
        finally:
            self.con.unregister("_incoming")


class SparkEngine(Engine):
    def __init__(self, spark, namespace: str):
        self.spark = spark
        self.ns = namespace
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")

    def sql(self, query):
        return self.spark.sql(query).toPandas()

    def exists(self, table):
        return self.spark.catalog.tableExists(self.t(table))

    def write(self, df, table, replace_where=None):
        if df.empty:
            if replace_where and self.exists(table):
                self.spark.sql(f"DELETE FROM {self.t(table)} WHERE {replace_where}")
            return
        if table in SCHEMAS:
            cols = [c.split()[0] for c in SCHEMAS[table].split(", ")]
            sdf = self.spark.createDataFrame(df[cols].astype(object).where(df[cols].notna(), None),
                                             schema=SCHEMAS[table])
        else:
            sdf = self.spark.createDataFrame(df)
        if self.exists(table):
            self.spark.sql(f"DELETE FROM {self.t(table)} WHERE {replace_where or 'TRUE'}")
        sdf.write.mode("append").option("mergeSchema", "true").saveAsTable(self.t(table))


def get_engine(cfg: dict) -> Engine:
    st = cfg["storage"]
    try:
        from pyspark.sql import SparkSession  # type: ignore

        spark = SparkSession.getActiveSession()
    except ImportError:
        spark = None
    if spark is not None or st["engine"] == "spark":
        if spark is None:
            from pyspark.sql import SparkSession  # type: ignore

            spark = SparkSession.builder.getOrCreate()
        return SparkEngine(spark, st["spark_namespace"])
    return DuckDBEngine(st["duckdb_path"], st["duckdb_schema"])
