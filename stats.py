import argparse
import os
import time

from typing import Any

import discord
import duckdb


class Plumber(discord.Client):
    def __init__(
        self,
        *,
        intents: discord.Intents,
        server_id: int,
        backfill: bool,
        **options: Any,
    ) -> None:
        super().__init__(intents=intents, **options)
        self.server_id = server_id
        self.backfill = backfill

    async def on_ready(self) -> None:
        # Connected to the Discord server
        server = client.get_guild(self.server_id)
        print(f"Backfill: {self.backfill}")
        # Create a connection to a file called 'server.db'
        con = duckdb.connect("server.db")

        # CREATE IF NOT EXISTS
        self.create_table_users(con)
        self.create_table_channels(con)
        self.create_table_messages(con)

        # Get all users
        await self.get_users(con, server)

        # Get all channels, and for each channel get all messages
        await self.get_channels_and_messages(con, server, self.backfill)

        # Close connection to DuckDB and Discord
        con.close()
        await client.close()

    def create_table_users(self, con):
        con.sql(
            """
            CREATE TABLE IF NOT EXISTS 
            users(
                id INT64 PRIMARY KEY, 
                name VARCHAR, 
                joined_at TIMESTAMP,
                created_at TIMESTAMP
                )
            """
        )

    def create_table_channels(self, con):
        con.sql(
            """
            CREATE TABLE IF NOT EXISTS
            channels(
                id INT64 PRIMARY KEY,
                category VARCHAR,
                category_id INT64,
                created_at TIMESTAMP,
                mention VARCHAR,
                name VARCHAR,
                topic VARCHAR
                )
            """
        )

    def create_table_messages(self, con):
        con.sql(
            """
        CREATE TABLE IF NOT EXISTS
        messages(
            id INT64 PRIMARY KEY,
            channel_id INT64,
            user_id INT64,
            content VARCHAR,
            clean_content VARCHAR,
            jump_url VARCHAR,
            created_at TIMESTAMP
            )
        """
        )

    async def get_users(
        self, con: duckdb.DuckDBPyConnection, server: discord.Guild
    ) -> None:
        start = time.time()
        for member in server.members:
            sql = f"""
            INSERT OR REPLACE INTO users VALUES 
            ({member.id}, 
            '{member.name.replace("'", "''")}', 
            '{member.joined_at}',
            '{member.created_at}'
            );
            """
            try:
                con.sql(sql)
            except Exception as e:
                print(e)

        end = time.time()
        print(f"Got all users in {end-start} seconds")

    async def get_channels_and_messages(
        self,
        con: duckdb.DuckDBPyConnection,
        server: discord.Guild,
        backfill: bool = False,
    ) -> None:
        for channel in server.text_channels:
            print(f"Attempting to gather {channel.name}...")
            sql = f"""
            INSERT OR REPLACE INTO channels VALUES 
            ({channel.id}, 
            '{channel.category}',
            '{channel.category_id}',
            '{channel.created_at}',
            '{channel.mention}',
            '{channel.name}',
            '{channel.topic.replace("'", "''") if isinstance(channel.topic, str) else channel.topic}');
            """

            try:
                con.sql(sql)
            except Exception as e:
                print(e)

            try:
                await self._get_messages(con, channel, backfill)
            except Exception as e:
                print(e)

    async def _get_messages(
        self,
        con: duckdb.DuckDBPyConnection,
        channel: discord.TextChannel,
        backfill: bool,
    ) -> None:
        start = time.time()

        if not backfill:
            con.execute(
                # Coalesce with first message timestamp to handle new channels gracefully
                f"""
                SELECT 
                    coalesce(
                        MAX(created_at), 
                        (select MIN(created_at) FROM messages)
                    )
                FROM messages 
                where channel_id = {channel.id}
                """
            )
            max_created_at = con.fetchone()[0]
            print(
                f"Collecting messages with a created_at value greater than {max_created_at}"
            )
            messages = channel.history(
                limit=None, oldest_first=True, after=max_created_at
            )
        else:
            messages = channel.history(
                limit=None,
                oldest_first=True,
            )

        counter = 0
        async for message in messages:
            sql = f"""
            INSERT OR REPLACE INTO messages VALUES 
            ({message.id}, 
            '{message.channel.id}',
            '{message.author.id}',
            '{message.content.replace("'", "''") if isinstance(message.content, str) else message.content}',
            '{message.content.replace("'", "''") if isinstance(message.clean_content, str) else message.clean_content}',
            '{message.jump_url}',
            '{message.created_at}');
            """
            try:
                con.sql(sql)
                counter += 1
            except Exception as e:
                print(e)

        end = time.time()
        print(f"Inserted {counter} records")
        print(f"Elapsed time: {end-start} seconds\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", action="store_true")
    args = parser.parse_args()

    token = os.environ["DISCORD_TOKEN"]
    server_id = os.environ["DISCORD_SERVER_ID"]

    intents = discord.Intents.all()
    intents.message_content = True

    client = Plumber(intents=intents, server_id=int(server_id), backfill=args.backfill)
    client.run(token)
