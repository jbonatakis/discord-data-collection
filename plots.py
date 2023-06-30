import duckdb
import pandas as pd
import seaborn as sns

from matplotlib import pyplot as plt


def connect(dbname: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(dbname)


def run_query(con: duckdb.DuckDBPyConnection, query_string) -> pd.DataFrame:
    return con.execute(query_string).df()


def barplot(data: pd.DataFrame, x: str, y: str, **kwargs):
    return sns.barplot(data=data, x=x, y=y, **kwargs)


if __name__ == "__main__":
    sql = """
    select
        strftime(datetrunc('month', created_at), '%Y-%m') as month,
        datepart('year', created_at) as year,
        count(*) as message_count
    from messages
    group by 1, 2
    order by 1;
    """
    con = connect("server.db")
    data = run_query(con, sql)
    plt.figure(figsize=(15, 8))
    plot = barplot(
        data, x="month", y="message_count", hue="year", width=0.8, dodge=False
    )
    avg_messages = run_query(
        con,
        "select count(*)/count(distinct datetrunc('month', created_at)) as avg from messages;",
    )
    plot.axhline(avg_messages["avg"][0])
    plot.tick_params(axis="x", rotation=90)
    fig = plot.get_figure()
    fig.savefig("images/messages-by-month.png")
