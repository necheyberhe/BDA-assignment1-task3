import os
import sqlite3
import random
import zipfile
from pathlib import Path

import pandas as pd

DB_PATH = "pokemon_battle_arena.db"
DATASET_SLUG = "abcsds/pokemon"
DATA_DIR = Path("data")
CSV_PATH = DATA_DIR / "Pokemon.csv"

def ensure_dataset():
    if not CSV_PATH.exists():
        raise FileNotFoundError(
            "Dataset not found. Please include data/Pokemon.csv in the repo."
        )
    return CSV_PATH

def create_baseline_snapshot(conn):
    conn.execute("DROP TABLE IF EXISTS pokemon_baseline")
    conn.execute("""
        CREATE TABLE pokemon_baseline AS
        SELECT
            id,
            name,
            type1,
            type2,
            hp,
            attack,
            defense,
            sp_atk,
            sp_def,
            speed,
            generation,
            legendary,
            source_tag
        FROM pokemon
    """)
    conn.commit()


# ----------------------------
# Database setup
# ----------------------------
def get_conn(db_path=DB_PATH):
    return sqlite3.connect(db_path)

def create_schema(conn):
    cur = conn.cursor()

    cur.executescript("""
    DROP TABLE IF EXISTS battle_log;
    DROP TABLE IF EXISTS cheat_audit;
    DROP TABLE IF EXISTS player_team;
    DROP TABLE IF EXISTS type_effectiveness;
    DROP TABLE IF EXISTS pokemon;

    CREATE TABLE pokemon (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        type1 TEXT NOT NULL,
        type2 TEXT,
        hp INTEGER NOT NULL,
        attack INTEGER NOT NULL,
        defense INTEGER NOT NULL,
        sp_atk INTEGER NOT NULL,
        sp_def INTEGER NOT NULL,
        speed INTEGER NOT NULL,
        generation INTEGER NOT NULL,
        legendary INTEGER NOT NULL DEFAULT 0,
        current_hp INTEGER,
        source_tag TEXT DEFAULT 'dataset'
    );

    CREATE TABLE type_effectiveness (
        attacker_type TEXT NOT NULL,
        defender_type TEXT NOT NULL,
        multiplier REAL NOT NULL,
        PRIMARY KEY (attacker_type, defender_type)
    );

    CREATE TABLE player_team (
        team_slot_id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_name TEXT NOT NULL,
        pokemon_id INTEGER NOT NULL,
        team_order INTEGER NOT NULL,
        FOREIGN KEY (pokemon_id) REFERENCES pokemon(id)
    );

    CREATE TABLE battle_log (
        log_id INTEGER PRIMARY KEY AUTOINCREMENT,
        turn_no INTEGER NOT NULL,
        actor TEXT NOT NULL,
        target TEXT,
        action TEXT NOT NULL,
        damage INTEGER DEFAULT 0,
        target_hp_after INTEGER,
        note TEXT
    );

    CREATE TABLE cheat_audit (
        audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
        cheat_code TEXT NOT NULL,
        player_name TEXT NOT NULL,
        details TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.commit()

def load_dataset(conn, csv_path):
    df = pd.read_csv(csv_path)

    rename_map = {
        "Name": "name",
        "Type 1": "type1",
        "Type 2": "type2",
        "HP": "hp",
        "Attack": "attack",
        "Defense": "defense",
        "Sp. Atk": "sp_atk",
        "Sp. Def": "sp_def",
        "Speed": "speed",
        "Generation": "generation",
        "Legendary": "legendary",
    }
    df = df.rename(columns=rename_map)

    required_cols = [
        "name", "type1", "type2", "hp", "attack", "defense",
        "sp_atk", "sp_def", "speed", "generation", "legendary"
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Dataset is missing required columns: {missing}")

    df["legendary"] = df["legendary"].astype(int)
    df["current_hp"] = df["hp"]
    df["source_tag"] = "dataset"

    cols = [
        "name", "type1", "type2", "hp", "attack", "defense",
        "sp_atk", "sp_def", "speed", "generation", "legendary",
        "current_hp", "source_tag"
    ]
    df[cols].to_sql("pokemon", conn, if_exists="append", index=False)

def load_type_effectiveness(conn):
    cur = conn.cursor()

    types = [
        "Normal", "Fire", "Water", "Electric", "Grass", "Ice", "Fighting",
        "Poison", "Ground", "Flying", "Psychic", "Bug", "Rock", "Ghost",
        "Dragon", "Dark", "Steel", "Fairy"
    ]

    rows = []
    for atk in types:
        for dfn in types:
            rows.append((atk, dfn, 1.0))

    overrides = {
        ("Fire", "Grass"): 2.0, ("Fire", "Water"): 0.5, ("Fire", "Fire"): 0.5, ("Fire", "Rock"): 0.5, ("Fire", "Steel"): 2.0,
        ("Water", "Fire"): 2.0, ("Water", "Grass"): 0.5, ("Water", "Water"): 0.5, ("Water", "Ground"): 2.0, ("Water", "Rock"): 2.0,
        ("Grass", "Water"): 2.0, ("Grass", "Fire"): 0.5, ("Grass", "Grass"): 0.5, ("Grass", "Ground"): 2.0, ("Grass", "Rock"): 2.0, ("Grass", "Flying"): 0.5,
        ("Electric", "Water"): 2.0, ("Electric", "Grass"): 0.5, ("Electric", "Electric"): 0.5, ("Electric", "Ground"): 0.0, ("Electric", "Flying"): 2.0,
        ("Ground", "Electric"): 2.0, ("Ground", "Fire"): 2.0, ("Ground", "Grass"): 0.5, ("Ground", "Flying"): 0.0, ("Ground", "Rock"): 2.0, ("Ground", "Steel"): 2.0,
        ("Rock", "Fire"): 2.0, ("Rock", "Flying"): 2.0, ("Rock", "Bug"): 2.0, ("Rock", "Fighting"): 0.5, ("Rock", "Ground"): 0.5,
        ("Ice", "Grass"): 2.0, ("Ice", "Ground"): 2.0, ("Ice", "Flying"): 2.0, ("Ice", "Dragon"): 2.0, ("Ice", "Fire"): 0.5, ("Ice", "Water"): 0.5,
        ("Fighting", "Normal"): 2.0, ("Fighting", "Rock"): 2.0, ("Fighting", "Ice"): 2.0, ("Fighting", "Dark"): 2.0, ("Fighting", "Ghost"): 0.0,
        ("Psychic", "Fighting"): 2.0, ("Psychic", "Poison"): 2.0, ("Psychic", "Dark"): 0.0,
        ("Bug", "Grass"): 2.0, ("Bug", "Psychic"): 2.0, ("Bug", "Dark"): 2.0, ("Bug", "Fire"): 0.5,
        ("Ghost", "Ghost"): 2.0, ("Ghost", "Psychic"): 2.0, ("Ghost", "Normal"): 0.0,
        ("Dragon", "Dragon"): 2.0, ("Dragon", "Fairy"): 0.0,
        ("Dark", "Psychic"): 2.0, ("Dark", "Ghost"): 2.0,
        ("Steel", "Rock"): 2.0, ("Steel", "Ice"): 2.0, ("Steel", "Fairy"): 2.0, ("Steel", "Fire"): 0.5, ("Steel", "Water"): 0.5,
        ("Fairy", "Dragon"): 2.0, ("Fairy", "Dark"): 2.0, ("Fairy", "Fighting"): 2.0, ("Fairy", "Fire"): 0.5, ("Fairy", "Steel"): 0.5,
    }

    cur.executemany("""
        INSERT INTO type_effectiveness(attacker_type, defender_type, multiplier)
        VALUES (?, ?, ?)
    """, rows)

    for (atk, dfn), mult in overrides.items():
        cur.execute("""
            UPDATE type_effectiveness
            SET multiplier = ?
            WHERE attacker_type = ? AND defender_type = ?
        """, (mult, atk, dfn))

    conn.commit()

def reset_current_hp(conn):
    conn.execute("UPDATE pokemon SET current_hp = hp")
    conn.commit()

def setup_database(db_path=DB_PATH):
    csv_path = ensure_dataset()
    conn = get_conn(db_path)
    create_schema(conn)
    load_dataset(conn, csv_path)
    create_baseline_snapshot(conn)
    load_type_effectiveness(conn)
    reset_current_hp(conn)
    return conn
# ----------------------------
# Utility queries
# ----------------------------
def list_pokemon(conn, limit=20):
    return pd.read_sql_query(f"""
        SELECT id, name, type1, type2, hp, attack, defense, sp_atk, sp_def, speed, generation, legendary
        FROM pokemon
        ORDER BY name
        LIMIT {int(limit)}
    """, conn)

def search_pokemon(conn, name_fragment):
    return pd.read_sql_query("""
        SELECT id, name, type1, type2, hp, attack, defense, sp_atk, sp_def, speed, generation, legendary
        FROM pokemon
        WHERE name LIKE ?
        ORDER BY name
    """, conn, params=[f"%{name_fragment}%"])

def get_pokemon_row(conn, pokemon_id):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, type1, type2, hp, attack, defense, sp_atk, sp_def, speed, generation, legendary, current_hp
        FROM pokemon
        WHERE id = ?
    """, (pokemon_id,))
    row = cur.fetchone()
    if not row:
        return None
    cols = ["id", "name", "type1", "type2", "hp", "attack", "defense", "sp_atk", "sp_def", "speed", "generation", "legendary", "current_hp"]
    return dict(zip(cols, row))

def clear_teams_and_logs(conn):
    cur = conn.cursor()
    cur.execute("DELETE FROM player_team")
    cur.execute("DELETE FROM battle_log")
    cur.execute("DELETE FROM cheat_audit")
    conn.commit()

def assign_team(conn, player_name, pokemon_ids):
    cur = conn.cursor()
    cur.execute("DELETE FROM player_team WHERE player_name = ?", (player_name,))
    for idx, pid in enumerate(pokemon_ids, start=1):
        cur.execute("""
            INSERT INTO player_team(player_name, pokemon_id, team_order)
            VALUES (?, ?, ?)
        """, (player_name, pid, idx))
    conn.commit()

def get_team(conn, player_name):
    return pd.read_sql_query("""
        SELECT pt.team_order, p.id, p.name, p.type1, p.type2, p.hp, p.current_hp, p.attack, p.defense, p.sp_atk, p.sp_def, p.speed
        FROM player_team pt
        JOIN pokemon p ON pt.pokemon_id = p.id
        WHERE pt.player_name = ?
        ORDER BY pt.team_order
    """, conn, params=[player_name])

def get_active_pokemon(conn, player_name):
    cur = conn.cursor()
    cur.execute("""
        SELECT p.id, p.name, p.type1, p.type2, p.hp, p.current_hp, p.attack, p.defense, p.sp_atk, p.sp_def, p.speed
        FROM player_team pt
        JOIN pokemon p ON pt.pokemon_id = p.id
        WHERE pt.player_name = ?
          AND p.current_hp > 0
        ORDER BY pt.team_order
        LIMIT 1
    """, (player_name,))
    row = cur.fetchone()
    if not row:
        return None
    cols = ["id", "name", "type1", "type2", "hp", "current_hp", "attack", "defense", "sp_atk", "sp_def", "speed"]
    return dict(zip(cols, row))

def effectiveness_multiplier(conn, attacker_type, defender_type1, defender_type2=None):
    cur = conn.cursor()
    mult = 1.0
    for d_type in [defender_type1, defender_type2]:
        if d_type is None or pd.isna(d_type):
            continue
        cur.execute("""
            SELECT multiplier
            FROM type_effectiveness
            WHERE attacker_type = ? AND defender_type = ?
        """, (attacker_type, d_type))
        row = cur.fetchone()
        part = row[0] if row else 1.0
        mult *= part
    return mult

def log_event(conn, turn_no, actor, target, action, damage=0, target_hp_after=None, note=None):
    conn.execute("""
        INSERT INTO battle_log(turn_no, actor, target, action, damage, target_hp_after, note)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (turn_no, actor, target, action, damage, target_hp_after, note))
    conn.commit()

def calc_damage(conn, attacker, defender):
    base = max(1, attacker["attack"] - defender["defense"] / 2)
    mult = effectiveness_multiplier(conn, attacker["type1"], defender["type1"], defender["type2"])
    random_factor = random.uniform(0.85, 1.0)
    damage = max(1, int(base * mult * random_factor))
    return damage, mult

def apply_damage(conn, pokemon_id, damage):
    cur = conn.cursor()
    cur.execute("""
        UPDATE pokemon
        SET current_hp = MAX(0, current_hp - ?)
        WHERE id = ?
    """, (damage, pokemon_id))
    conn.commit()
    cur.execute("SELECT current_hp FROM pokemon WHERE id = ?", (pokemon_id,))
    return cur.fetchone()[0]

# ----------------------------
# Cheat system: real SQL writes
# ----------------------------
def record_cheat(conn, cheat_code, player_name, details):
    conn.execute("""
        INSERT INTO cheat_audit(cheat_code, player_name, details)
        VALUES (?, ?, ?)
    """, (cheat_code, player_name, details))
    conn.commit()

def apply_cheat(conn, cheat_code, player_name, opponent_name):
    cur = conn.cursor()
    cheat_code = cheat_code.upper().strip()

    if cheat_code == "UPUPDOWNDOWN":
        cur.execute("""
            UPDATE pokemon
            SET hp = hp * 2,
                current_hp = current_hp * 2
            WHERE id IN (
                SELECT pokemon_id FROM player_team WHERE player_name = ?
            )
        """, (player_name,))
        conn.commit()
        record_cheat(conn, cheat_code, player_name, "Doubled HP and current HP of player's team.")

    elif cheat_code == "GODMODE":
        cur.execute("""
            UPDATE pokemon
            SET defense = 999,
                sp_def = 999
            WHERE id IN (
                SELECT pokemon_id FROM player_team WHERE player_name = ?
            )
        """, (player_name,))
        conn.commit()
        record_cheat(conn, cheat_code, player_name, "Set Defense and Sp.Def to 999 for player's team.")

    elif cheat_code == "NERF":
        cur.execute("""
            UPDATE pokemon
            SET attack = CAST(attack * 0.5 AS INT),
                defense = CAST(defense * 0.5 AS INT),
                sp_atk = CAST(sp_atk * 0.5 AS INT),
                sp_def = CAST(sp_def * 0.5 AS INT),
                speed = CAST(speed * 0.5 AS INT),
                hp = CAST(hp * 0.5 AS INT),
                current_hp = MIN(current_hp, CAST(hp * 0.5 AS INT))
            WHERE id IN (
                SELECT pokemon_id FROM player_team WHERE player_name = ?
            )
        """, (opponent_name,))
        conn.commit()
        record_cheat(conn, cheat_code, player_name, f"Reduced opponent ({opponent_name}) stats by 50%.")

    elif cheat_code == "STEAL":
        cur.execute("""
            SELECT p.id, p.name, p.type1, p.type2, p.hp, p.attack, p.defense, p.sp_atk, p.sp_def, p.speed, p.generation, p.legendary
            FROM player_team pt
            JOIN pokemon p ON pt.pokemon_id = p.id
            WHERE pt.player_name = ?
            ORDER BY (p.attack + p.defense + p.sp_atk + p.sp_def + p.speed + p.hp) DESC
            LIMIT 1
        """, (opponent_name,))
        row = cur.fetchone()
        if row:
            name = f"{row[1]}_stolen_{player_name}"
            cur.execute("""
                INSERT INTO pokemon(name, type1, type2, hp, attack, defense, sp_atk, sp_def, speed, generation, legendary, current_hp, source_tag)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                name, row[2], row[3], row[4], row[5], row[6], row[7], row[8],
                row[9], row[10], row[11], row[4], "cheat_steal"
            ))
            new_id = cur.lastrowid
            cur.execute("SELECT COALESCE(MAX(team_order), 0) + 1 FROM player_team WHERE player_name = ?", (player_name,))
            next_order = cur.fetchone()[0]
            cur.execute("""
                INSERT INTO player_team(player_name, pokemon_id, team_order)
                VALUES (?, ?, ?)
            """, (player_name, new_id, next_order))
            conn.commit()
            record_cheat(conn, cheat_code, player_name, f"Copied opponent's strongest team Pokémon ({row[1]}) into player's team.")
        else:
            record_cheat(conn, cheat_code, player_name, "STEAL attempted but opponent had no available Pokémon.")

    elif cheat_code == "LEGENDARY":
        cur.execute("""
            INSERT INTO pokemon(name, type1, type2, hp, attack, defense, sp_atk, sp_def, speed, generation, legendary, current_hp, source_tag)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            f"OmegaPhoenix_{player_name}",
            "Fire", "Dragon",
            500, 300, 300, 300, 300, 250,
            99, 1, 500, "cheat_legendary"
        ))
        new_id = cur.lastrowid
        cur.execute("SELECT COALESCE(MAX(team_order), 0) + 1 FROM player_team WHERE player_name = ?", (player_name,))
        next_order = cur.fetchone()[0]
        cur.execute("""
            INSERT INTO player_team(player_name, pokemon_id, team_order)
            VALUES (?, ?, ?)
        """, (player_name, new_id, next_order))
        conn.commit()
        record_cheat(conn, cheat_code, player_name, "Inserted custom overpowered Pokémon into player's team.")

    else:
        raise ValueError(f"Unknown cheat code: {cheat_code}")

# ----------------------------
# Battle engine
# ----------------------------
def perform_attack(conn, turn_no, attacker_name, attacker, defender_name, defender):
    damage, mult = calc_damage(conn, attacker, defender)
    hp_after = apply_damage(conn, defender["id"], damage)

    if mult == 0:
        note = "No effect"
    elif mult > 1:
        note = "Super effective"
    elif mult < 1:
        note = "Not very effective"
    else:
        note = "Normal effectiveness"

    log_event(
        conn,
        turn_no=turn_no,
        actor=attacker["name"],
        target=defender["name"],
        action=f"{attacker_name} attacks {defender_name}",
        damage=damage,
        target_hp_after=hp_after,
        note=note
    )
    return hp_after, note, damage

def battle(conn, player1="Player 1", player2="Player 2", verbose=True):
    turn_no = 1

    while True:
        p1 = get_active_pokemon(conn, player1)
        p2 = get_active_pokemon(conn, player2)

        if p1 is None:
            if verbose:
                print(f"{player2} wins. {player1} has no Pokémon left.")
            return player2

        if p2 is None:
            if verbose:
                print(f"{player1} wins. {player2} has no Pokémon left.")
            return player1

        order = [(player1, p1), (player2, p2)]
        if p2["speed"] > p1["speed"]:
            order = [(player2, p2), (player1, p1)]
        elif p2["speed"] == p1["speed"] and random.random() < 0.5:
            order = [(player2, p2), (player1, p1)]

        if verbose:
            print(f"\nTurn {turn_no}")
            print(f"{player1}: {p1['name']} ({p1['current_hp']}/{p1['hp']} HP)")
            print(f"{player2}: {p2['name']} ({p2['current_hp']}/{p2['hp']} HP)")

        first_name, first_poke = order[0]
        second_name, second_poke = order[1]

        hp_after, note, damage = perform_attack(conn, turn_no, first_name, first_poke, second_name, second_poke)
        if verbose:
            print(f"{first_poke['name']} hits {second_poke['name']} for {damage} damage. {note}. Target HP: {hp_after}")

        second_poke = get_pokemon_row(conn, second_poke["id"])
        second_poke = {
            "id": second_poke["id"], "name": second_poke["name"], "type1": second_poke["type1"], "type2": second_poke["type2"],
            "hp": second_poke["hp"], "current_hp": second_poke["current_hp"], "attack": second_poke["attack"],
            "defense": second_poke["defense"], "sp_atk": second_poke["sp_atk"], "sp_def": second_poke["sp_def"], "speed": second_poke["speed"]
        }

        if second_poke["current_hp"] <= 0:
            log_event(conn, turn_no, second_poke["name"], None, f"{second_name} Pokémon fainted", note="Fainted")
            if verbose:
                print(f"{second_poke['name']} fainted.")
            turn_no += 1
            continue

        hp_after, note, damage = perform_attack(conn, turn_no, second_name, second_poke, first_name, first_poke)
        if verbose:
            print(f"{second_poke['name']} hits {first_poke['name']} for {damage} damage. {note}. Target HP: {hp_after}")

        first_poke = get_pokemon_row(conn, first_poke["id"])
        if first_poke["current_hp"] <= 0:
            log_event(conn, turn_no, first_poke["name"], None, f"{first_name} Pokémon fainted", note="Fainted")
            if verbose:
                print(f"{first_poke['name']} fainted.")

        turn_no += 1

def show_battle_log(conn):
    return pd.read_sql_query("""
        SELECT log_id, turn_no, actor, target, action, damage, target_hp_after, note
        FROM battle_log
        ORDER BY log_id
    """, conn)

def cheat_audit_report(conn):
    suspicious_changes = pd.read_sql_query("""
        SELECT
            p.name,
            p.source_tag,
            b.hp AS original_hp,
            p.hp AS current_hp,
            b.attack AS original_attack,
            p.attack AS current_attack,
            b.defense AS original_defense,
            p.defense AS current_defense,
            b.sp_atk AS original_sp_atk,
            p.sp_atk AS current_sp_atk,
            b.sp_def AS original_sp_def,
            p.sp_def AS current_sp_def,
            b.speed AS original_speed,
            p.speed AS current_speed,
            CASE
                WHEN b.id IS NULL AND p.source_tag = 'cheat_legendary' THEN 'LEGENDARY cheat'
                WHEN b.id IS NULL AND p.source_tag = 'cheat_steal' THEN 'STEAL cheat'
                WHEN p.hp = b.hp * 2 THEN 'UPUPDOWNDOWN cheat (HP doubled)'
                WHEN p.defense = 999 AND p.sp_def = 999 THEN 'GODMODE cheat'
                WHEN p.attack = CAST(b.attack * 0.5 AS INT)
                OR p.defense = CAST(b.defense * 0.5 AS INT)
                OR p.sp_atk = CAST(b.sp_atk * 0.5 AS INT)
                OR p.sp_def = CAST(b.sp_def * 0.5 AS INT)
                OR p.speed = CAST(b.speed * 0.5 AS INT)
                OR p.hp = CAST(b.hp * 0.5 AS INT) THEN 'NERF cheat'
                ELSE 'Modified from baseline'
            END AS anomaly_reason
                                           
        FROM pokemon p
        LEFT JOIN pokemon_baseline b
            ON p.name = b.name
        WHERE
            b.id IS NULL
            OR p.hp != b.hp
            OR p.attack != b.attack
            OR p.defense != b.defense
            OR p.sp_atk != b.sp_atk
            OR p.sp_def != b.sp_def
            OR p.speed != b.speed
        ORDER BY p.name
    """, conn)

    deleted_rows = pd.read_sql_query("""
        SELECT
            b.name,
            'Missing from current pokemon table' AS anomaly_reason
        FROM pokemon_baseline b
        LEFT JOIN pokemon p
            ON p.name = b.name
        WHERE p.id IS NULL
        ORDER BY b.name
    """, conn)

    cheat_log = pd.read_sql_query("""
        SELECT audit_id, cheat_code, player_name, details, created_at
        FROM cheat_audit
        ORDER BY audit_id
    """, conn)

    return suspicious_changes, deleted_rows, cheat_log
# ----------------------------
# Analysis queries
# ----------------------------
def strongest_type_combos(conn, top_n=10):
    return pd.read_sql_query(f"""
        SELECT
            type1,
            COALESCE(type2, 'None') AS type2,
            ROUND(AVG(hp + attack + defense + sp_atk + sp_def + speed), 2) AS avg_total_stats,
            COUNT(*) AS pokemon_count
        FROM pokemon
        WHERE source_tag = 'dataset'
        GROUP BY type1, COALESCE(type2, 'None')
        HAVING COUNT(*) >= 2
        ORDER BY avg_total_stats DESC, pokemon_count DESC
        LIMIT {int(top_n)}
    """, conn)

def power_creep_by_generation(conn):
    return pd.read_sql_query("""
        SELECT
            generation,
            ROUND(AVG(hp + attack + defense + sp_atk + sp_def + speed), 2) AS avg_total_stats,
            COUNT(*) AS pokemon_count,
            ROUND(AVG(CASE WHEN legendary = 1 THEN hp + attack + defense + sp_atk + sp_def + speed END), 2) AS avg_legendary_total,
            ROUND(AVG(CASE WHEN legendary = 0 THEN hp + attack + defense + sp_atk + sp_def + speed END), 2) AS avg_nonlegendary_total
        FROM pokemon
        WHERE source_tag = 'dataset'
        GROUP BY generation
        ORDER BY generation
    """, conn)

def best_three_pokemon_team(conn):
    return pd.read_sql_query("""
        SELECT
            name, type1, type2, hp, attack, defense, sp_atk, sp_def, speed,
            (hp + attack + defense + sp_atk + sp_def + speed) AS total_stats
        FROM pokemon
        WHERE source_tag = 'dataset'
        ORDER BY total_stats DESC, speed DESC
        LIMIT 3
    """, conn)
if __name__ == "__main__":
    conn = setup_database(DB_PATH)
    clear_teams_and_logs(conn)

    print("Sample Pokémon:")
    print(list_pokemon(conn, 10).to_string(index=False))

    team1 = search_pokemon(conn, "Charizard")["id"].tolist()[:1] + search_pokemon(conn, "Pikachu")["id"].tolist()[:1]
    team2 = search_pokemon(conn, "Blastoise")["id"].tolist()[:1] + search_pokemon(conn, "Venusaur")["id"].tolist()[:1]

    if len(team1) < 2 or len(team2) < 2:
        raise RuntimeError("Example Pokémon not found in dataset. Search names manually and assign teams.")

    assign_team(conn, "Player 1", team1)
    assign_team(conn, "Player 2", team2)

    apply_cheat(conn, "UPUPDOWNDOWN", "Player 1", "Player 2")
    apply_cheat(conn, "NERF", "Player 1", "Player 2")

    winner = battle(conn, "Player 1", "Player 2", verbose=True)
    print("\nWinner:", winner)

    print("\nBattle log:")
    print(show_battle_log(conn).to_string(index=False))

    changes, deleted_rows, audit = cheat_audit_report(conn)

    print("\nCheat audit - modified or inserted Pokémon:")
    print(changes.to_string(index=False))

    print("\nCheat audit - deleted Pokémon:")
    print(deleted_rows.to_string(index=False))

    print("\nCheat audit log:")
    print(audit.to_string(index=False))

    print("\nAnalysis - strongest type combos:")
    print(strongest_type_combos(conn).to_string(index=False))

    print("\nAnalysis - power creep by generation:")
    print(power_creep_by_generation(conn).to_string(index=False))

    print("\nAnalysis - best raw-stat team:")
    print(best_three_pokemon_team(conn).to_string(index=False))

    conn.close()