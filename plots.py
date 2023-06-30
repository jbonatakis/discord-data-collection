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
    con = connect("server.db")

    def plot_monthly_activity(con):
        plt.clf()
        messages_by_month_sql = """
        select
            strftime(datetrunc('month', created_at), '%Y-%m') as month,
            datepart('year', created_at) as year,
            count(*) as message_count
        from messages
        group by 1, 2
        order by 1;
        """

        messages_by_month = run_query(con, messages_by_month_sql)
        plt.figure(figsize=(15, 8))
        plot = barplot(
            messages_by_month,
            x="month",
            y="message_count",
            hue="year",
            width=0.8,
            dodge=False,
        )
        avg_messages = run_query(
            con,
            "select count(*)/count(distinct datetrunc('month', created_at)) as avg from messages;",
        )
        plot.axhline(avg_messages["avg"][0])
        plot.tick_params(axis="x", rotation=90)
        fig = plot.get_figure()
        fig.savefig("images/messages-by-month.png")

    def plot_hourly_activity(con):
        plt.clf()
        activity_by_hour_sql = """
        select
            datepart('hour', created_at) as hour_of_day,
            count(*) as message_count,
            round(count(*)/(select count(*) from messages)*100,2) as pct_total
        from messages
        group by 1
        order by 1;
        """
        activity_by_hour = run_query(con, activity_by_hour_sql)
        hourly_plot = barplot(
            activity_by_hour,
            x="hour_of_day",
            y="message_count",
            color="royalblue",
            width=0.8,
            dodge=False,
        )
        fig = hourly_plot.get_figure()
        fig.savefig("images/messages-by-hour.png")


plot_monthly_activity(con)
plot_hourly_activity(con)
