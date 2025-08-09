import discord
from discord.ext import commands
from discord import app_commands
import json
import time
import os
import threading
import sqlite3
import sql
import asyncio
import requests
from main import main_function
from discord.ext import tasks
from discord import Embed
import re
from collections import defaultdict
import aiohttp

# Set up the bot with intents
intents = discord.Intents.default()
intents.messages = True
intents.guilds = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)
json_file = 'bucket_data.json'  # JSON file to store user bucket data

# Define the allowed roles using the provided role IDs
ALLOWED_ROLES = [
    1202412136698216458,
    1205975137795706942,
    1235821184332075049,
    1210946826652356618,
    571646216669233162,
    1253771406613938278
]

MANAGER_ROLE = [571646216669233162]

setup_message_id = None  # Store the message ID of the setup message

# Helper function to check if the user has an allowed role (by role ID)
def check_roles(member: discord.Member):
    return any(role.id in ALLOWED_ROLES for role in member.roles)

def check_manager(member: discord.Member):
    return any(role.id in MANAGER_ROLE for role in member.roles)

def format_points(points):
    if points >= 1000000000:
        return f"{points / 1000000000:.2f}B".rstrip('0').rstrip('.')
    elif points >= 1000000:
        return f"{points / 1000000:.2f}M".rstrip('0').rstrip('.')
    else:
        return str(points)

def get_battle_history(username, user_id):
    conn = sql.connect_db()
    sql.create_tables(conn)
    user_battles = sql.fetch_user_clan_battles(conn, user_id)
    sql.close_db(conn)

    if user_battles:
        battle_order = [
            "Christmas2023", "DecemberActiveHugePets", "IndexBattle", "AchBattle",
            "RaidBattle", "GoalBattleOne", "GoalBattleTwo", "GlitchBattle",
            "PrisonBattle", "HackerBattle", "GoodEvilBattle", "MillionaireRunBattle", "RngBattle"
        ]
        
        formatted_battles = []
        unknown_battles = []
        
        for battle in user_battles:
            clan_tag = f"[{battle['clan']}]"
            formatted_points = format_points(battle['points'])
            formatted_line = f"{clan_tag} {battle['battle_name']} 👤{battle['clan_contribution']} ⭐{formatted_points}"
            
            if battle['battle_name'] in battle_order:
                formatted_battles.append((battle_order.index(battle['battle_name']), formatted_line))
            else:
                unknown_battles.append(formatted_line)
        
        formatted_battles.sort(key=lambda x: x[0])
        formatted_battles = [battle[1] for battle in formatted_battles] + unknown_battles
        
        return "\n".join(formatted_battles)
    else:
        return f"No battles found for this user"

async def get_id(name):
    url = "https://users.roblox.com/v1/usernames/users"
    payload = {"usernames": [name], "excludeBannedUsers": True}
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.post(url, json=payload, timeout=30, ssl=False) as response:
                    response.raise_for_status()
                    data = await response.json()
                    data = data.get("data", [])
                    # print(data)
                    if not data:
                        return None
                    return data[0].get("id")
            except (aiohttp.ClientError, IndexError, KeyError):
                await asyncio.sleep(1)  # Wait for 1 second before retrying
                continue

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user}')
    try:
        synced = await bot.tree.sync()
        print(f'Synced {len(synced)} command(s)')
    except Exception as e:
        print(f'Failed to sync commands: {e}')

@bot.tree.command(name="stats", description="Get User's Clan Battle History")
@app_commands.describe(username="Username")
async def stats(interaction: discord.Interaction, username: str):
    await interaction.response.defer()
    try:
        # Fetch the full member object from the guild
        guild_member = await interaction.guild.fetch_member(interaction.user.id)
        
        # Check if the user has permission to use the command
        if not check_roles(guild_member):
            await interaction.followup.send("You don't have permission to use this command.", ephemeral=True)
            return

        # Proceed with the logic to fetch battle history
        id = await get_id(username)
        if id:
            text = get_battle_history(username, id)
            embed = discord.Embed(title=f"{username}'s Battle History", description=text)
        else:
            embed = discord.Embed(title="Error", description="Username or id is required")
        await interaction.followup.send(embed=embed)
    except Exception as e:
        print(f"An error occurred: {e}")
        await interaction.followup.send("An error occurred while processing the command.", ephemeral=True)

@stats.error
async def stats_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CheckFailure):
        await interaction.followup.send("You do not have the required roles to use this command.", ephemeral=True)
    else:
        await interaction.followup.send("An error occurred while processing the command.", ephemeral=True)


async def fetch_data(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()  # Await the asynchronous json() method
                return data.get('data', None)  # Access the 'data' key safely
            else:
                print(f'Failed to retrieve data, status code: {response.status}')
                return None


def is_user_in_members(user_id, members_list):
    for member in members_list:
        if member['UserID'] == user_id or user_id == 1824904833:
            return True
    return False

async def get_roblox_username(user_id):
    user_cache_file = 'usernames_cache.txt'  # Specify the cache file name

    # Try to read from the cache file
    try:
        with open(user_cache_file, 'r') as file:
            for line in file:
                cached_id, cached_name = line.strip().split(':')
                if str(user_id) == cached_id:
                    return cached_name
    except FileNotFoundError:
        # If the file doesn't exist, create it
        open(user_cache_file, 'w').close()

    # If the username isn't in the cache, fetch from the API asynchronously
    username_lookup_url = f'https://users.roblox.com/v1/users/{user_id}'

    async with aiohttp.ClientSession() as session:
        async with session.get(username_lookup_url) as response:
            if response.status == 200:
                data = await response.json()
                username = data.get('name', f'UnknownUser({user_id})')

                # Write the new username to the cache
                with open(user_cache_file, 'a') as file:
                    file.write(f'{user_id}:{username}\n')
                
                return username
            else:
                return f'UnknownUser({user_id})'

class ClanBattleButtons(discord.ui.View):
    def __init__(self, clan_name, clan_battle_names, conn):
        super().__init__(timeout=None)  # Disable timeout if you want buttons to last until manually removed
        self.clan_name = clan_name
        self.conn = conn  # Keep the DB connection open until interactions are done
        self.clan_battle_names = clan_battle_names

        # Dynamically create a button for each battle name
        for battle_name in clan_battle_names:
            button = discord.ui.Button(label=battle_name, custom_id=battle_name)
            button.callback = self.button_callback  # Assign the callback function to each button
            self.add_item(button)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Ensure that only the user who invoked the command can press the buttons
        return check_roles(interaction.user)

    async def button_callback(self, interaction: discord.Interaction):
        # Defer the interaction to prevent timeout errors
        await interaction.response.defer()  # This gives you more time to process the request

        battle_name = interaction.data['custom_id']  # Get the battle name from the button's custom_id

        # Fetch battle history for the specific clan and battle
        battle_history = sql.fetch_battle_history(self.conn, self.clan_name, battle_name)

        if battle_history:
            # Gather usernames asynchronously
            tasks = [get_roblox_username(entry['user_id']) for entry in battle_history]
            usernames = await asyncio.gather(*tasks)

            # Format the battle history to display in the embed
            description = ""
            for i, entry in enumerate(battle_history, start=1):
                formatted_points = format_points(entry['points'])
                description += f"{i}. {usernames[i-1]} {formatted_points}⭐ {entry['clan_contribution']:.2f}👥\n"

            index_of_last_newline = description[:4096].rfind("\n")
            embeds = []
            if len(description)<=4096:
                embed = discord.Embed(title=f"{self.clan_name} {battle_name} Leaderboard", description=description)
                embeds.append(embed)
            if len(description)>4096:
                embed = discord.Embed(title=f"{self.clan_name} {battle_name} Leaderboard", description=description[:index_of_last_newline])
                embeds.append(embed)
            if len(description)>6000:
                description = description
                embed = discord.Embed(title=f"{self.clan_name} {battle_name} Leaderboard", description=(description[index_of_last_newline:5097]+"..."))
                embeds.append(embed)
            # Send up to 2 embeds in a single message
            await interaction.edit_original_response(embeds=embeds)

        else:
            # If no history is found for the battle
            await interaction.edit_original_response(content=f"No battle history found for {battle_name}.")

    async def on_timeout(self):
        # Close the database connection when the view times out or buttons become inactive
        sql.close_db(self.conn)

    async def on_timeout(self):
        # Close the database connection when the view times out or buttons become inactive
        sql.close_db(self.conn)


@bot.tree.command(name="clan_stats", description="Get Clan's Battle Leaderboard")
@app_commands.describe(clan_name="Clan Name")
async def clan_stats(interaction: discord.Interaction, clan_name: str):
    if not check_roles(interaction.user):
        await interaction.response.send_message("You don't have permission to use this command.", ephemeral=True)
        return
    
    conn = sql.connect_db()  # Assuming sql.connect_db() is your function to connect to the database
    sql.create_tables(conn)  # Create the tables if not already created
    clan_battle_names = sql.fetch_clan_battle_names(conn, clan_name)  # Fetch the unique battle names for the clan
    
    if not clan_battle_names:
        await interaction.response.send_message(f"No battles found for clan {clan_name}.", ephemeral=True)
        sql.close_db(conn)  # Close the connection if no data is found
        return

    # Create an embed for the initial clan leaderboard prompt
    embed = discord.Embed(title="Clan Battles", description=f"Select a battle to view the leaderboard for {clan_name}")
    
    # Create a view with buttons for each battle name, passing the open DB connection
    view = ClanBattleButtons(clan_name, clan_battle_names, conn)

    # Send the initial message with buttons
    await interaction.response.send_message(embed=embed, view=view, ephemeral=False)

    # Do not close the connection here; the view will manage it
        
# Run the bot
def run_bot():
    bot.run("OTgzNjk3ODI5OTg4MDE2MTQ4.G6tklQ.xGDG_wyKjA_ixsrA9AO2f3yDhOn2Yj0nZO_ltY")

if __name__ == "__main__":  
    main_thread = threading.Thread(target=main_function)
    main_thread.start()
    run_bot()
