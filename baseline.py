import time
import duckdb

SCALE = 1
SAMPLE_SIZE = 2000
MIN_SUPPORT = 5
ALPHA = 0.20

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

def createGroupDirectory(con):
    return dict(con.execute("""
        SELECT c_nation, COUNT(*)
        FROM tpch_skewed
        GROUP BY c_nation
    """).fetchall())

def createSkewedData(con):
    con.execute("""
        CREATE OR REPLACE TABLE tpch_skewed AS
        WITH ranked AS (
            SELECT *,
                   DENSE_RANK() OVER (ORDER BY c_nation) AS nation_rank
            FROM tpch_data
        )
        SELECT * EXCLUDE (nation_rank)
        FROM ranked
        WHERE nation_rank <= 15

           OR (nation_rank BETWEEN 16 AND 20
               AND hash(l_orderkey, l_linenumber) % 4 = 0)

           OR (nation_rank BETWEEN 21 AND 23
               AND hash(l_orderkey, l_linenumber) % 100 = 0)

           OR (nation_rank >= 24
               AND hash(l_orderkey, l_linenumber) % 1000 = 0)

        ORDER BY c_nation
    """)

def createStratifiedSample(con):
    # Keep at most SAMPLE_SIZE rows from each nation
    con.execute(f"""
        CREATE OR REPLACE TABLE stratified_sample AS
        SELECT * EXCLUDE (row_num)
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY c_nation
                    ORDER BY hash(l_orderkey, l_linenumber)
                ) AS row_num
            FROM tpch_skewed
        )
        WHERE row_num <= {SAMPLE_SIZE}
    """)

def createUniformSample(con):
    sample_size = con.execute(
        "SELECT COUNT(*) FROM stratified_sample"
    ).fetchone()[0]

    con.execute(f"""
        CREATE OR REPLACE TABLE uniform_sample AS
        SELECT *
        FROM tpch_skewed
        USING SAMPLE {sample_size} ROWS
    """)

def detectAndRepair(con, where, group_directory):
    start = time.perf_counter()

    sample, _ = runQuery(con, "stratified_sample", where)
    results = {nation: [avg, support] for nation, avg, support in sample}

    # Flag thin or missing groups
    flagged = [
        nation for nation in group_directory
        if nation not in results or results[nation][1] < MIN_SUPPORT
    ]

    flagged_rows = sum(group_directory[nation] for nation in flagged)
    repair_ratio = flagged_rows / sum(group_directory.values())

    # Too much data needs repair, so just run exact
    if repair_ratio > ALPHA:
        result, _ = runQuery(con, "tpch_skewed", where)
        runtime = (time.perf_counter() - start) * 1000

        return result, runtime, len(flagged), repair_ratio, "Full Exact"

    # Repair flagged groups only
    if flagged:
        nations = ", ".join(f"'{nation}'" for nation in flagged)

        repaired = con.execute(f"""
            SELECT c_nation,
                   AVG(l_extendedprice),
                   COUNT(*)
            FROM tpch_skewed
            WHERE ({where})
              AND c_nation IN ({nations})
            GROUP BY c_nation
        """).fetchall()

        for nation, avg, support in repaired:
            results[nation] = [avg, support]

    result = [
        (nation, avg, support)
        for nation, (avg, support) in results.items()
    ]

    runtime = (time.perf_counter() - start) * 1000

    return result, runtime, len(flagged), repair_ratio, "Selective Repair"

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

def compareResults(con, name, where, table):
    exact, exact_time = runQuery(con, "tpch_skewed", where)
    sample, sample_time = runQuery(con, table, where)

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

        if support < MIN_SUPPORT:
            thin += 1

        error = abs(sample_avg - exact_avg) / exact_avg
        total_error += error

    if len(exact) > 0:
        average_error = total_error / len(exact)
    else:
        average_error = 0

    print(name, "-", table)
    print("Groups:", len(exact))
    print("Missing groups:", missing)
    print(f"Thin groups (<{MIN_SUPPORT} sample rows):", thin)
    print("Average error:", f"{average_error:.2%}")
    print("Sample time:", f"{sample_time:.2f} ms")
    print("Exact time:", f"{exact_time:.2f} ms")
    print()

def compareRepair(con, name, where, group_directory):
    exact, exact_time = runQuery(con, "tpch_skewed", where)

    repaired, repair_time, flagged, ratio, mode = detectAndRepair(
        con, where, group_directory
    )

    repaired_dict = {nation: avg for nation, avg, support in repaired}

    missing = 0
    total_error = 0

    for nation, exact_avg, support in exact:
        if nation not in repaired_dict:
            missing += 1
            total_error += 1
        else:
            total_error += abs(repaired_dict[nation] - exact_avg) / exact_avg

    average_error = total_error / len(exact)

    print(name, "- Detect-and-Repair")
    print("Mode:", mode)
    print("Flagged groups:", flagged)
    print("Repair ratio:", f"{ratio:.2%}")
    print("Missing groups:", missing)
    print("Average error:", f"{average_error:.2%}")
    print("Repair time:", f"{repair_time:.2f} ms")
    print("Exact time:", f"{exact_time:.2f} ms")
    print()

def main():
    con = duckdb.connect()

    loadData(con)
    createSkewedData(con)
    createStratifiedSample(con)
    createUniformSample(con)
    groupDirectory = createGroupDirectory(con)

    print("Full rows:",
          con.execute("SELECT COUNT(*) FROM tpch_skewed").fetchone()[0])

    print("Stratified Sample rows:",
          con.execute("SELECT COUNT(*) FROM stratified_sample").fetchone()[0])

    print("Uniform sample rows:",
      con.execute("SELECT COUNT(*) FROM uniform_sample").fetchone()[0])

    print()

    filters = [
        ("No Filter",
         "TRUE"),

        ("AIR",
         "l_shipmode = 'AIR'"),

        ("AIR + Quantity",
         "l_shipmode = 'AIR' AND l_quantity > 45"),

        ("AIR + Quantity + Year Range",
        """l_shipmode = 'AIR'
            AND l_quantity > 45
            AND o_year BETWEEN 1994 AND 1996"""),

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
        compareResults(con, name, where, "uniform_sample")
        compareResults(con, name, where, "stratified_sample")
        compareRepair(con, name, where, groupDirectory)

    con.close()

if __name__ == "__main__":
    main()