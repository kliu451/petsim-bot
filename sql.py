import sqlite3

def connect_db():
    return sqlite3.connect('clan_battles.db')

def create_tables(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS clan_battles (
            user_id INTEGER,
            battle_name TEXT,
            points INTEGER,
            clan TEXT,
            clan_contribution FLOAT,
            PRIMARY KEY (user_id, battle_name)
        )
    ''')
    conn.commit()

def batch_insert_clan_battle_data(conn, battle_data):
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT OR REPLACE INTO clan_battles (user_id, battle_name, points, clan, clan_contribution)
        VALUES (?, ?, ?, ?, ?)
    ''', battle_data)
    conn.commit()

def fetch_user_clan_battles(conn, user_id):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM clan_battles WHERE user_id = ?', (user_id,))
    battles = cursor.fetchall()

    if battles:
        return [{"battle_name": battle[1], "points": battle[2], "clan": battle[3], "clan_contribution": battle[4]} for battle in battles]
    else:
        return []

def close_db(conn):
    conn.close()

def fetch_clan_battle_names(conn, clan):
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT battle_name FROM clan_battles WHERE clan = ?', (clan,))
    battle_names = cursor.fetchall()
    
    # Return the list of battle names as a simple list
    return [battle_name[0] for battle_name in battle_names]

def fetch_battle_history(conn, clan, battle_name):
    cursor = conn.cursor()
    cursor.execute('''
        SELECT user_id, battle_name, points, clan, clan_contribution 
        FROM clan_battles 
        WHERE clan = ? AND battle_name = ? 
        ORDER BY points DESC
    ''', (clan, battle_name))
    battle_history = cursor.fetchall()
    
    # Return the results as a list of dictionaries
    return [{"user_id": entry[0], "battle_name": entry[1], "points": entry[2], "clan": entry[3], "clan_contribution": entry[4]} for entry in battle_history]