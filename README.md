# BDA-assignment1-task3
# Pokemon Battle Arena

A data-driven Pokemon battle simulation built using **Python, SQLite, and Streamlit**.  
All game mechanics, cheat codes, and analytics are powered entirely by database operations.

===========================================================================================

## Features
###  Battle System
- Turn-based Pokemon battles
- Speed determines turn order
- Damage calculated using Attack vs Defense
- Type effectiveness (Fire > Grass > Water, etc.)
- Full battle log stored in database
==============================================================================================
### Team Selection
- Each player selects **1–3 Pokemon**
- All Pokémon stats are loaded from SQLite (no hardcoding)
===============================================================================================
### Cheat Code System (SQL-driven)
All cheats are implemented using **real SQL operations**:
| Cheat Code | Operation | Effect |
|----------|----------|--------|
| `UPUPDOWNDOWN` | UPDATE | Doubles HP |
| `GODMODE` | UPDATE | Sets Defense & Sp.Def to 999 |
| `STEAL` | INSERT | Copies opponent’s strongest Pokémon |
| `LEGENDARY` | INSERT | Adds custom overpowered Pokémon |
| `NERF` | UPDATE | Reduces opponent stats by 50% |
=====================================================================================================
### Cheat Audit System
- Tracks all cheats in a dedicated audit table
- Detects:
  - Modified Pokemon
  - Inserted Pokemon
  - Anomalies vs original stats
- Uses a **baseline snapshot** for accurate comparison
========================================================================================================
### Data Analysis (SQL)

Includes analytical queries such as:
-  Strongest Pokemon type combinations
- Power creep across generations
- Best 3-Pokemon team based on total stats
===========================================================================================================
## Database Schema
Main tables:

- `pokemon` → Pokemon stats dataset  
- `player_team` → Player-selected teams  
- `battle_log` → Turn-by-turn battle actions  
- `type_effectiveness` → Type multipliers  
- `cheat_audit` → Cheat tracking  
- `pokemon_baseline` → Original stats snapshot  
================================================================================================================
## Technologies Used
- Python  
- SQLite (sqlite3)  
- Pandas  
- Streamlit  
====================================================================================================================

## How to Run
### 1. Clone the repository

```bash
git clone https://github.com/your-username/bda-assignment1-task3.git
cd bda-assignment1-task3
