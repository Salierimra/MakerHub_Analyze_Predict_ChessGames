# MakerHub_Analyze_Predict_ChessGames

## Project Description

In a world where data analysis plays an increasingly central role in solving real-world problems, this project applies that same rigor to the game of chess.

The goal is to analyze **past and future chess games** to answer concrete questions and surface meaningful insights for the end user — whether you're a casual player looking to improve, a coach studying patterns, or a data enthusiast fascinated by the intersection of strategy and statistics.

<img width="835" height="335" alt="image" src="https://github.com/user-attachments/assets/80e574db-070b-4158-a156-4b64cc250457" />

## Project Goal

- Analyze historical chess game data to identify patterns and trends
- Answer concrete, user-relevant questions (e.g. *"Which openings win most at my ELO?"*, *"What is the strenghts and weakness of a player"*,*"which player would be the most likely to win"*)
- Build a foundation for predictive analysis of future game outcomes
- Present findings in a clear, visual, and accessible format

## Key Questions

- Which openings perform best at a given ELO range? | Descriptive |
- What are a specific player's strengths and weaknesses? | Profiling |
- Which player is most likely to win a given matchup? | Predictive |
- Does playing White vs. Black significantly affect win rate? | Statistical |

## Technologies Used

- Python (Flask,ORM)
- SQL
- SSIS
- Power BI
  
## Project phases

### Data Collection

Data collection is automated daily via **Windows Task Scheduler**, which triggers a Python script at a defined interval.

<img width="787" height="602" alt="image" src="https://github.com/user-attachments/assets/a18a2c5b-2421-418f-a750-51584a3badf5" />

Data collection comes from 3 sources :

- [http://](https://theweekinchess.com/) : the script is extracting the last 2 weeks
- Chess.com API : the script is extracting top 50 from all type of games (classical,rapid,blitz and bullet) and check if each player is in DataBase
    - If not : extract player games from 5 lasts months
    - if yes : extract player games from last date in the database
 
<img width="828" height="26" alt="image" src="https://github.com/user-attachments/assets/a0c23662-8e27-4e8d-9efd-c862d3706869" />

- Lichess API : the script is extracting top 50 from all type of games (classical,rapid,blitz and bullet) and check if each player is in DataBase
    - If not : extract player games from 5 lasts months
    - if yes : extract player games from last date in the database
 
<img width="312" height="22" alt="image" src="https://github.com/user-attachments/assets/f17c9b91-d99d-43f6-ba4d-87671582f41f" />

Games extracted are placed in a single concatened dataframe

### Data Cleaning

The following transformations are applied to ensure data quality and consistency:

- **Result normalization** — The result column is mapped to 3 standardized outcomes (`White wins`, `Black wins`, `Draw`) to reduce cardinality
- **Unranked game removal** — Games without a rating context are dropped as they are not analytically useful
- **Game type mapping** — Game types are standardized to `Classical`, `Rapid`, `Blitz`, or `Bullet`
- **Invalid row removal** — Rows where ELO, opening name, or opening code is unknown are removed, as they cannot be used in analysis
- **ELO type conversion** — ELO columns are cast to numerical values to enable quantitative analysis

At the end of cleaning phase, dataframe is exported to a `.csv`

### Database creation

Relational database was created and is continuously updated from data collection phase using ORM (**Windows Task Scheduler**)

<img width="322" height="395" alt="image" src="https://github.com/user-attachments/assets/2a62b7c3-1a84-4d11-82f9-3866969f52f8" />

### Datawarehouse 

Data warehouse was created using SQL and automated daily by SQL agent 

<img width="1208" height="285" alt="image" src="https://github.com/user-attachments/assets/b2d0e500-eec3-4917-8885-e56fcebb4606" />

Data transfer between the relational database and the data warehouse is handled by **SSIS**, with **historization** included to track changes over time.

<img width="427" height="336" alt="image" src="https://github.com/user-attachments/assets/196481f7-69bc-497c-b58b-a9bf3364529d" />

<img width="467" height="375" alt="image" src="https://github.com/user-attachments/assets/03725a99-13cd-4439-9ed3-36edd2e444b3" />

### Deep Learning

#### Feature engineering

For each games the following features are calculated
-  Games number played by each player to assure that his/her rating is accurate
-  Games number played against each other to assure match-up consideration
-  Winrate against each other
-  Momentum (number of games won on the last 10 games played)

#### Feature selection

<img width="772" height="455" alt="image" src="https://github.com/user-attachments/assets/f4cae3a7-1252-4914-b715-eb5758a3904d" />

- ELO white/Black
- Delta ELO (adjusted)
- Number of games played by each player
- Momentum
- Game type (`Classical`, `Rapid`, `Blitz`, `Bullet`)
- is white stronger ? (boolean)

#### Results

The model achieved a **ROC-AUC score of 75%**, demonstrating solid discriminative power for predicting match outcomes.
  
<img width="782" height="55" alt="image" src="https://github.com/user-attachments/assets/c25025f9-f350-4e3a-ad81-db5e1d87fffe" />

### DashBoard

This Dashboard was deployed into service (one month trial) and a on premises gateway was set

<img width="452" height="640" alt="image" src="https://github.com/user-attachments/assets/b7d8aa88-d3af-45e9-93b9-ac43b97bdfe6" />

## Findings /insights

- Chess data reveals a truth that applies even to the world's greatest players: **no one is invincible**.

Take **Magnus Carlsen** — widely regarded as the Greatest Of All Time. Analysis of his game history uncovers a striking vulnerability:

When playing openings of type **A01** (Nimzovich-Larsen Attack), Carlsen loses **48% of his games** — a remarkable weakness for a player of his caliber.

<img width="448" height="250" alt="image" src="https://github.com/user-attachments/assets/c69c1873-5ccb-43ce-aa72-b80755ac87a3" />

This kind of insight is precisely what this project is designed to surface. By aggregating thousands of games and breaking down performance by opening type, player, ELO range, and game format, the analysis goes beyond raw win rates to expose **the specific conditions under which even elite players are most vulnerable**.

- Winning with white pieces is easier (WR Magnus : White : 70% / Black : 62.7 %)
  
<img width="558" height="198" alt="image" src="https://github.com/user-attachments/assets/f0ea5faf-e34c-46a3-a86f-df16b9f567bc" />

## Flask -  Using DL Model with local html page

### Use

1. Launch the ORM_Creation_fill_BDD_V4 script using terminal :

- python ORM_Creation_Fill_BDD_Rela_V4.py --mode manuel

2. open browser and go to 127.0.0.1
3. Follow instructions

<img width="455" height="777" alt="image" src="https://github.com/user-attachments/assets/f211d5f1-677f-46b1-8f1b-b885c9c0fc2a" />

