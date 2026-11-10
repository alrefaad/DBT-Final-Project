import time
import duckdb


SCALE = 0.1
SAMPLE_SIZE = 2000


def loadData(con):
    con.execute("INSTALL tpch")
    con.execute("LOAD tpch")

    # Generate TPC-H tables
    con.execute(f"CALL dbgen(sf={SCALE})")

    # Create one table with the columns needed for our queries
    con.execute("""
        CREATE OR REPLACE TABLE tpch_data AS
        SELECT l.l_orderkey,
               l.l_linenumber,
               l.l_extendedprice,
               l.l_quantity,
               l.l_discount,
               l.l_shipmode,
               YEAR(o.o_orderdate) AS o_year,
               c.c_mktsegment,
               n.n_name AS c_nation
        FROM lineitem l
        JOIN orders o
        ON l.l_orderkey = o.o_orderkey
        JOIN customer c
        ON o.o_custkey = c.c_custkey
        JOIN nation n
        ON c.c_nationkey = n.n_nationkey
    """)


def createSample(con):
    # Keep at most SAMPLE_SIZE rows from each nation
    con.execute(f"""
        CREATE OR REPLACE TABLE sample AS
        SELECT * EXCLUDE (row_num)
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY c_nation
                       ORDER BY hash(l_orderkey, l_linenumber)
                   ) AS row_num
            FROM tpch_data
        )
        WHERE row_num <= {SAMPLE_SIZE}
    """)


def runQuery(con, table, where):
    start = time.perf_counter()

    result = con.execute(f"""
        SELECT c_nation,
               AVG(l_extendedprice) AS avg_price,
               COUNT(*) AS support
        FROM {table}
        WHERE {where}
        GROUP BY c_nation
        ORDER BY c_nation
    """).fetchall()

    runtime = (time.perf_counter() - start) * 1000

    return result, runtime


def compareResults(con, name, where):
    exact, exact_time = runQuery(con, "tpch_data", where)
    sample, sample_time = runQuery(con, "sample", where)

    sample_dict = {}

    for nation, avg_price, support in sample:
        sample_dict[nation] = (avg_price, support)

    missing = 0
    thin = 0
    total_error = 0

    for nation, exact_avg, exact_count in exact:
        if nation not in sample_dict:
            missing += 1
            total_error += 1
            continue

        sample_avg, support = sample_dict[nation]

        if support < 5:
            thin += 1

        error = abs(sample_avg - exact_avg) / exact_avg
        total_error += error

    if len(exact) > 0:
        average_error = total_error / len(exact)
    else:
        average_error = 0

    print(name)
    print("Groups:", len(exact))
    print("Missing groups:", missing)
    print("Thin groups (<5 sample rows):", thin)
    print("Average error:", f"{average_error:.2%}")
    print("Sample time:", f"{sample_time:.2f} ms")
    print("Exact time:", f"{exact_time:.2f} ms")
    print()


def main():
    con = duckdb.connect("project.duckdb")

    loadData(con)
    createSample(con)

    print("Full rows:",
          con.execute("SELECT COUNT(*) FROM tpch_data").fetchone()[0])

    print("Sample rows:",
          con.execute("SELECT COUNT(*) FROM sample").fetchone()[0])

    print()

    filters = [
        ("No Filter",
         "TRUE"),

        ("AIR",
         "l_shipmode = 'AIR'"),

        ("AIR + Quantity",
         "l_shipmode = 'AIR' AND l_quantity > 45"),

        ("AIR + Quantity + Year",
         "l_shipmode = 'AIR' AND l_quantity > 45 AND o_year = 1995"),

        ("AIR + Quantity + Year + Segment",
         """l_shipmode = 'AIR'
            AND l_quantity > 45
            AND o_year = 1995
            AND c_mktsegment = 'BUILDING'"""),

        ("Most Selective",
         """l_shipmode = 'AIR'
            AND l_quantity > 45
            AND o_year = 1995
            AND c_mktsegment = 'BUILDING'
            AND l_discount >= 0.08""")
    ]

    for name, where in filters:
        compareResults(con, name, where)

    con.close()


if __name__ == "__main__":
    main()