import json
import os
import sqlite3
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sql

def create_session_with_retries(retries=5, backoff_factor=0.3, status_forcelist=(500, 502, 504)):
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def get_top_clans(session, number_of_clans: int) -> list:
    response = session.get(f"https://biggamesapi.io/api/clans?page=1&pageSize={number_of_clans}&sort=Points&sortOrder=desc")
    response.raise_for_status()
    data = response.json()
    clan_names_list = [item['Name'] for item in data['data']]
    clan_names_list.reverse()
    return clan_names_list

def get_clan_json(session, clan: str) -> dict:
    response = session.get(f"https://biggamesapi.io/api/clan/{clan}")
    response.raise_for_status()
    return response.json()["data"]

def get_clan_battle(clan_json: dict) -> dict:
    return clan_json["Battles"]

def get_clan_capacity(clan_json: dict) -> int:
    return clan_json["MemberCapacity"]

def battle_breakdown(battles, clan, clan_size):
    a_list = []
    for name, info in battles.items():
        try:
            overall_points = info["Points"]
            contributions = info["PointContributions"]
            for item in contributions:
                user_id = item['UserID']
                points = item['Points']
                if overall_points == 0:
                    relative_average_clan_contribution = 0
                else:
                    relative_average_clan_contribution = round((points / overall_points) / (1 / clan_size), 2)
                a_list.append((user_id, name, points, clan, relative_average_clan_contribution))
        except:
            continue
    return a_list

def process_local_clan_files():
    clan_directories = ["clan_data", "clan_data01-6", "clan_data23-5"]
    all_batches = []
    for dir_name in clan_directories:
        if os.path.isdir(dir_name):
            for file_name in os.listdir(dir_name):
                file_path = os.path.join(dir_name, file_name)
                if os.path.isfile(file_path):
                    with open(file_path, 'r') as file:
                        try:
                            clan = file_name.split(".")[0]
                            clan_json = json.load(file)["data"]
                            clan_size = get_clan_capacity(clan_json)
                            battle_list = get_clan_battle(clan_json)
                            batch_list = battle_breakdown(battle_list, clan, clan_size)
                            all_batches.extend(batch_list)
                        except json.JSONDecodeError as e:
                            print(f"Error reading {file_path}: {e}")
                        except KeyError as e:
                            print(f"Missing key in {file_path}: {e}")

    conn = sql.connect_db()
    sql.create_tables(conn)
    sql.batch_insert_clan_battle_data(conn, all_batches)
    sql.close_db(conn)

def main_function():
    session = create_session_with_retries()
    while True:
        try:
            clan_placement_list = get_top_clans(session, 500)
            conn = sql.connect_db()
            sql.create_tables(conn)
            for clan in clan_placement_list:
                clan_json = get_clan_json(session, clan)
                clan_size = get_clan_capacity(clan_json)
                battle_list = get_clan_battle(clan_json)
                batch_list = battle_breakdown(battle_list, clan, clan_size)
                sql.batch_insert_clan_battle_data(conn, batch_list)
            sql.close_db(conn)
            time.sleep(600)  # Wait for 10 minutes before the next run
        except Exception as e:
            print(f"An error occurred: {e}. Retrying in 1 minute...")
            time.sleep(60)  # Wait for 1 minute before retrying

if __name__ == '__main__':
    main_function()
