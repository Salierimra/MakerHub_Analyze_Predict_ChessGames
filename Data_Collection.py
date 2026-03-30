import time
import pandas as pd
import requests
from chessdotcom import ChessDotComClient #https://chesscom.readthedocs.io/en/latest/
import re
from datetime import datetime,timedelta,date
from dateutil.relativedelta import relativedelta
import berserk
import pyodbc

import zipfile
import io
import os
import chess.pgn
import glob




def get_active_players(client):
    """
    Fonction permettant de recuperer le top 50 des joueurs en bullet, blitz, rapide, classical en evitant les doublons
    Args : 
        client : object de l'API ChessDotComClient
    Returns:
        liste de joueurs uniques representant le top level chess.com
    """
    players = set() # structure de données n'acceptant pas les doublons
    
    try:
        leaderboard = client.get_leaderboards()#utilisation de la methode get_leaderbords de l'API retrouve le top 50 de chacun des types de parties (blitz, tactic, classical)
        leaderboard_data = leaderboard.json #recupere la reponse json de la requete 
        #{'leaderboards': {'daily': [{'player_id': 32506052,'@id': 'https://api.chess.com/pub/player/jolintsai',......

        # Accéder aux leaderboards
        if "leaderboards" in leaderboard_data:#check si l clef existe dans le Json
            boards = leaderboard_data["leaderboards"] # on filtre sur leaderboards {'daily': [{'player_id': 32506052,'@id': 'https://api.chess.com/pub/player/jolintsai',......
            
            # Pour chaque type de jeu désiré 
            for board_type in ["daily", "live_rapid", "live_blitz", "live_bullet"]:
                if board_type in boards:#si l'entrée daily existe (exemple)

                    print(f"\nTraitement {board_type}...")
                    
                    # on filtre boards sur le type de jeu en cours (daily par exemple) mais on en prend que les 50 premieres entrées puis on parcours chaque joueur
                    #boards[board_type][:50]  :  [{'player_id': 32506052,  '@id': 'https://api.chess.com/pub/player/jolintsai',  'url': 'https://www.chess.com/member/JolinTsai',  'username': 'JolinTsai',
                    for player in boards[board_type][:50]: #player : {'player_id': 32506052,  '@id': 'https://api.chess.com/pub/player/jolintsai',  'url': 'https://www.chess.com/member/JolinTsai',  'username': 'JolinTsai',
                        
                        # test si l'element est un dictionnaire ( on sait jamais)et si username est une clef de ce dictionnaie
                        if isinstance(player, dict) and 'username' in player:
                            players.add(player['username'])#on recupere seulement l'username du joueur  {'JolinTsai'}
        
        print(f"\nTotal final: {len(players)} joueurs uniques")
        
    except Exception as e:
        print(f"Erreur leaderboard: {e}")
       
    return list(players) #retourne la liste des joueurs ['JolinTsai']

def recovering_players_from_DB():
    '''
    Fonction permettant de retrouver tous les joueurs de la DB
    '''
    conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=MAX;"
    "DATABASE=CHESS_MAKERHUB_DW;"
    "Trusted_Connection=yes;"
)

    cursor = conn.cursor()

    cursor.execute(f""" 
        select * from D_Players
        """)
    fetch = [row[2] for row in cursor.fetchall()] #le 3ieme attribut est le nom du joueur

    conn.close()
    return fetch #retourne une liste avec les noms des joueurs

def get_current_and_n_lasts_month(n):
    """Retourne YYYY/MM pour le mois actuel et les n precedents sous forme de list"""
    now = datetime.now()
    current = now.strftime("%Y/%m")#formatage en 2000/12
    
    months_ago = []
    for _ in range(n):
        # Mois précédent
        if now.month == 1: #si mois = 2000/01, le mois precedent est 1999/12
            months_ago.append(f"{now.year - 1}/12")
        else:
            months_ago.append(f"{now.year}/{str(now.month - 1).zfill(2)}")#zfill(2) rajoute des 0 jusqu'a avoir 2 chiffres -> si 10 ne fait rien, si 1 -> 01
        now = now - relativedelta(months=1) #on decale la date de 1 moi
    
    return current, months_ago

def recovering_SK_from_name_in_DB(namePlayer):
    '''
    FOnction permettant de retourner l SK d'un joueur à partir du nom en interogeant la DB DW'''

    conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=MAX;"
    "DATABASE=CHESS_MAKERHUB_DW;"
    "Trusted_Connection=yes;")

    cursor = conn.cursor()
    
    # EVD_Players is NULL car il nous faut l'information à jour 
    cursor.execute("""
    SELECT SK_Players
    FROM D_Players
    WHERE Name_Player = ? AND EVD_Players IS NULL
    """, (namePlayer,))

    fetch = cursor.fetchone()
    conn.close()
    return fetch[0] #on retourne un entier representant la SK du joueur passé en paramètre

def recovering_last_played_date_from_DB(SKPlayer):
    '''
    Fonction permettant de retrouver la partie la plus recente d'un joueur à partir de sa SK dans la BDD
    retourne le nombre de mois depuis la derniere patie,la date de la derniere partie et le nombre de jours depuis la derniere partie
    '''

    conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=MAX;"
    "DATABASE=CHESS_MAKERHUB_DW;"
    "Trusted_Connection=yes;"
)

    cursor = conn.cursor()

    cursor.execute(f""" 
        select MAX(DATEKEY) from F_Games
    where SK_Players = {SKPlayer} or SK_Players_1 = {SKPlayer}
        """)

    fetchd = cursor.fetchone()
    conn.close()
    fetch_str = str(fetchd[0])#le premier element seulement

    my_date = pd.to_datetime(datetime(int(fetch_str[:4]),int(fetch_str[4:6]),int(fetch_str[6:9]))) #transform to date
    my_date

    diff = relativedelta(date.today(),my_date) #dictionnaire avec nombre d'année, de mois et de jours d'ecart
    nombre_mois = diff.years * 12 + diff.months
    nombre_jours = diff.years *365 +diff.months*30 +diff.days
    return nombre_mois,my_date,nombre_jours

def get_player_recent_games_chesscom(username,players_DB):
    """
    Fonction permettant de recuperer les parties des n derniers mois du joueur passé en paramètre
    Args : 
        username du joueur
        liste des joueurs dèjà dans la DB
    Returns:
        
    """
    if username not in players_DB :#si le joueur est nouveau :)
        print("nouveau joueur",username)
        current_month, last_month = get_current_and_n_lasts_month(5)
        all_target_months = [current_month] + last_month #on va concatener le mois actuel avec la liste des n derniers mois [2026/03,2026/02,2026/01,2025/12,2025/11]
        print (all_target_months)
        try:
            archives = clientchesscom.get_player_game_archives(username=username) #utilisation de la methode get_player_game_archives de l'API chessdotcom 
            #retourne : <chessdotcom.endpoints.player_game_archives.GetPlayerGameArchivesResponse at 0x1fc0b17e0d0>
            recent_games = []
            # Headers pour éviter le 403
            headers = {
                'User-Agent': 'MyChessApp/1.0 (contact@example.com)',  
                'Accept': 'application/json'
            }
            games= []
            

            #archives.archives ['https://api.chess.com/pub/player/jolintsai/games/2017/01', 'https://api.chess.com/pub/player/jolintsai/games/2017/02', 'https://api.chess.com/pub/player/jolintsai/games/2017/06',

            for archive_url in archives.archives: #archive_url : 'https://api.chess.com/pub/player/jolintsai/games/2017/01'

                # VÉRIFIER D'ABORD si l'URL correspond aux mois ciblés , c'est une facon d'importer uniquement les 5 derniers mois
                is_target_month = any(month in archive_url for month in all_target_months)
                
                if is_target_month:
                    
                    try:
                        response = requests.get(archive_url, headers=headers, timeout=10)#timeout : combien de ms pour attendre reponse client
                        
                        if response.status_code == 200: #si tout va bien
                            games_data = response.json()#transforme la reponse en json
                                #    {'games': [{'url': 'https://www.chess.com/game/live/1908943303',
                                #    'pgn': '[Event "Live Chess"]\n[Site "Chess.com"]\n[Date "2017.01.19"]\n[Round "-"]\n[White "JolinTsai"]\n[Black "rapadoodle"]\n[Result "1-0"]\n[CurrentPosition "7Q/8/P6k/8/6R1/4PP2/1B5P/6K1 b - -"]\n[Timezone "UTC"]\n[ECO "A01"]\n[ECOUrl "https://www.chess.com/openings/Nimzowitsch-Larsen-Attack-Modern-Variation-2.Bb2-d6-3.e3-Nf6"]\n[UTCDate "2017.01.19"]\n[UTCTime "09:05:40"]\n[WhiteElo "978"]\n[BlackElo "692"]\n[TimeControl "60"]\n[Termination "JolinTsai won by checkmate"]\n[StartTime "09:05:40"]\n[EndDate "2017.01.19"]\n[EndTime "09:07:48"]\n[Link "https://www.chess.com/game/live/1908943303"]\n\n1. b3 {[%clk 0:00:59.3]} 1... e5 {[%clk 0:00:58.4]} 2. Bb2 {[%clk 0:00:59.2]} 2... d6 {[%clk 0:00:57.9]} 3. e3 {[%clk 0:00:58.7]} 3... Nf6 {[%clk 0:00:57.5]} 4. Ne2 {[%clk 0:00:58.6]} 4... e4 {[%clk 0:00:56.8]} 5. d4 {[%clk 0:00:58.4]} 5... Be6 {[%clk 0:00:56]} 6. c4 {[%clk 0:00:57.3]} 6... c6 {[%clk 0:00:54.9]} 7. Nbc3 {[%clk 0:00:57.2]} 7... Be7 {[%clk 0:00:54.3]} 8. Nf4 {[%clk 0:00:56.5]} 8... Kf8 {[%clk 0:00:53.1]} 9. Nxe6+ {[%clk 0:00:55.8]} 9... fxe6 {[%clk 0:00:52.6]} 10. d5 {[%clk 0:00:55.6]} 10... exd5 {[%clk 0:00:51.6]} 11. cxd5 {[%clk 0:00:55.5]} 11... Nxd5 {[%clk 0:00:50.6]} 12. Nxd5 {[%clk 0:00:52.4]} 12... cxd5 {[%clk 0:00:49.8]} 13. Qxd5 {[%clk 0:00:52.3]} 13... Nd7 {[%clk 0:00:49.3]} 14. Bc4 {[%clk 0:00:49.8]} 14... Qe8 {[%clk 0:00:46.2]} 15. Qxe4 {[%clk 0:00:45.7]} 15... Nf6 {[%clk 0:00:44.2]} 16. Qxb7 {[%clk 0:00:44]} 16... d5 {[%clk 0:00:40.1]} 17. Bb5 {[%clk 0:00:32.1]} 17... Qb8 {[%clk 0:00:35.8]} 18. Qxb8+ {[%clk 0:00:31]} 18... Rxb8 {[%clk 0:00:34.9]} 19. Bd3 {[%clk 0:00:30.9]} 19... Kf7 {[%clk 0:00:33]} 20. O-O {[%clk 0:00:29.5]} 20... Rhe8 {[%clk 0:00:32.1]} 21. Rfd1 {[%clk 0:00:28.3]} 21... Rbd8 {[%clk 0:00:31.1]} 22. Rac1 {[%clk 0:00:26.9]} 22... Ne4 {[%clk 0:00:30.3]} 23. Bxe4 {[%clk 0:00:26.1]} 23... dxe4 {[%clk 0:00:29.4]} 24. Rd4 {[%clk 0:00:25.7]} 24... Rxd4 {[%clk 0:00:28.1]} 25. Bxd4 {[%clk 0:00:25.6]} 25... a5 {[%clk 0:00:26.6]} 26. g4 {[%clk 0:00:24.7]} 26... Ba3 {[%clk 0:00:25.2]} 27. Rc7+ {[%clk 0:00:23.3]} 27... Kg8 {[%clk 0:00:24.3]} 28. Ra7 {[%clk 0:00:21.8]} 28... Bf8 {[%clk 0:00:18.9]} 29. Rxa5 {[%clk 0:00:20.7]} 29... Rc8 {[%clk 0:00:17.8]} 30. Re5 {[%clk 0:00:19.8]} 30... Rc2 {[%clk 0:00:17]} 31. a4 {[%clk 0:00:17.8]} 31... Rb2 {[%clk 0:00:16.2]} 32. Bxb2 {[%clk 0:00:16.3]} 32... h6 {[%clk 0:00:14.7]} 33. Rxe4 {[%clk 0:00:16.2]} 33... Kh7 {[%clk 0:00:14.2]} 34. a5 {[%clk 0:00:15.8]} 34... Bb4 {[%clk 0:00:13.2]} 35. Rxb4 {[%clk 0:00:14.4]} 35... h5 {[%clk 0:00:11.6]} 36. Rb7 {[%clk 0:00:14.3]} 36... hxg4 {[%clk 0:00:11.2]} 37. Rxg7+ {[%clk 0:00:13.8]} 37... Kh6 {[%clk 0:00:10]} 38. Rxg4 {[%clk 0:00:13.7]} 38... Kh5 {[%clk 0:00:09.5]} 39. f3 {[%clk 0:00:12.2]} 39... Kh6 {[%clk 0:00:07.6]} 40. a6 {[%clk 0:00:12.1]} 40... Kh7 {[%clk 0:00:07.1]} 41. b4 {[%clk 0:00:12]} 41... Kh6 {[%clk 0:00:06.5]} 42. b5 {[%clk 0:00:11.9]} 42... Kh7 {[%clk 0:00:06.3]} 43. b6 {[%clk 0:00:11.8]} 43... Kh6 {[%clk 0:00:06]} 44. b7 {[%clk 0:00:11.5]} 44... Kh5 {[%clk 0:00:05.9]} 45. b8=Q {[%clk 0:00:10.5]} 45... Kh6 {[%clk 0:00:05.1]} 46. Qh8# {[%clk 0:00:10.4]} 1-0\n',
                                #    'time_control': '60',
                                #    'end_time': 1484816868,
                                #    'rated': True,
                                #    'tcn': 'jr0KcjZRmu!TgmKClB6SkAYQbs90mD89DS1SBJSJAJTJsJQJdJ5ZfA78JCZTCXRJAH85X545Ht91eg?8fd57acTCtCJCdB7BjBWGoE0qcY1!YWq9WG86GK6kiykjBj3VKC!3yG9zCzVNzXNEX23V2EVNnvNVGOV3rz3VzHV3HP3VPXVNX~NV5?',
                                #    'uuid': 'e2e9f646-4541-11e1-8000-000000010001',
                                #    'initial_setup': 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1',

                            games = games_data.get('games', [])#on recupere la clef 'games' et on recupere [] si la clef n'existe pas
                           
                            
                            recent_games.extend(games) #on extend l'element en cours -> [{{'url': 'https://www.chess.com/game/live/1908943303','pgn': '[Event "Live Chess"]\n[Si
                            
                            
                            
                        elif response.status_code == 403:
                            print(f"    403 Forbidden - Attente...")
                            time.sleep(2)
                            
                        else:
                            print(f"   Erreur {response.status_code}")
                            
                    except requests.exceptions.RequestException as e:
                        print(f"    Erreur requête: {e}")
                    
                    # Rate limiting entre chaque requête
                    time.sleep(0.5)
                else:
                    # Mois non ciblé, on skip
                    pass
            
            print(f"    {len(recent_games)} parties récupérées")
            return recent_games
        
        except Exception as e:
            print(f"Erreur {username}: {e}")
            return []
    
    else: #si le joueur existe dans la BDD
        recov_SK = recovering_SK_from_name_in_DB(username)
        nb_mois,last_date,_ = recovering_last_played_date_from_DB(recov_SK)
        current_month, last_month = get_current_and_n_lasts_month(nb_mois)
        all_target_months = [current_month] + last_month #on va concatener le mois actuel avec la liste des n derniers mois
        
        try:
            archives = clientchesscom.get_player_game_archives(username=username) #utilisation de la methode get_player_game_archives de l'API chessdotcom 
            #retourne : <chessdotcom.endpoints.player_game_archives.GetPlayerGameArchivesResponse at 0x1fc0b17e0d0>

            recent_games = []
            # Headers pour éviter le 403
            headers = {
                'User-Agent': 'MyChessApp/1.0 (contact@example.com)',  # Votre vrai email
                'Accept': 'application/json'
            }
            games= []

            #archives.archives ['https://api.chess.com/pub/player/jolintsai/games/2017/01', 'https://api.chess.com/pub/player/jolintsai/games/2017/02', 'https://api.chess.com/pub/player/jolintsai/games/2017/06',

            for archive_url in archives.archives: #archive_url : 'https://api.chess.com/pub/player/jolintsai/games/2017/01'

                # VÉRIFIER D'ABORD si l'URL correspond aux mois ciblés , c'est une facon d'importer uniquement les n derniers mois
                is_target_month = any(month in archive_url for month in all_target_months)
                
                if is_target_month:
                    
                    try:
                        response = requests.get(archive_url, headers=headers, timeout=10)#timeout : combien de ms pour attendre reponse client
                        
                        if response.status_code == 200: #si tout va bien
                            games_data = response.json()#transforme la reponse en json
                                #    {'games': [{'url': 'https://www.chess.com/game/live/1908943303',
                                #    'pgn': '[Event "Live Chess"]\n[Site "Chess.com"]\n[Date "2017.01.19"]\n[Round "-"]\n[White "JolinTsai"]\n[Black "rapadoodle"]\n[Result "1-0"]\n[CurrentPosition "7Q/8/P6k/8/6R1/4PP2/1B5P/6K1 b - -"]\n[Timezone "UTC"]\n[ECO "A01"]\n[ECOUrl "https://www.chess.com/openings/Nimzowitsch-Larsen-Attack-Modern-Variation-2.Bb2-d6-3.e3-Nf6"]\n[UTCDate "2017.01.19"]\n[UTCTime "09:05:40"]\n[WhiteElo "978"]\n[BlackElo "692"]\n[TimeControl "60"]\n[Termination "JolinTsai won by checkmate"]\n[StartTime "09:05:40"]\n[EndDate "2017.01.19"]\n[EndTime "09:07:48"]\n[Link "https://www.chess.com/game/live/1908943303"]\n\n1. b3 {[%clk 0:00:59.3]} 1... e5 {[%clk 0:00:58.4]} 2. Bb2 {[%clk 0:00:59.2]} 2... d6 {[%clk 0:00:57.9]} 3. e3 {[%clk 0:00:58.7]} 3... Nf6 {[%clk 0:00:57.5]} 4. Ne2 {[%clk 0:00:58.6]} 4... e4 {[%clk 0:00:56.8]} 5. d4 {[%clk 0:00:58.4]} 5... Be6 {[%clk 0:00:56]} 6. c4 {[%clk 0:00:57.3]} 6... c6 {[%clk 0:00:54.9]} 7. Nbc3 {[%clk 0:00:57.2]} 7... Be7 {[%clk 0:00:54.3]} 8. Nf4 {[%clk 0:00:56.5]} 8... Kf8 {[%clk 0:00:53.1]} 9. Nxe6+ {[%clk 0:00:55.8]} 9... fxe6 {[%clk 0:00:52.6]} 10. d5 {[%clk 0:00:55.6]} 10... exd5 {[%clk 0:00:51.6]} 11. cxd5 {[%clk 0:00:55.5]} 11... Nxd5 {[%clk 0:00:50.6]} 12. Nxd5 {[%clk 0:00:52.4]} 12... cxd5 {[%clk 0:00:49.8]} 13. Qxd5 {[%clk 0:00:52.3]} 13... Nd7 {[%clk 0:00:49.3]} 14. Bc4 {[%clk 0:00:49.8]} 14... Qe8 {[%clk 0:00:46.2]} 15. Qxe4 {[%clk 0:00:45.7]} 15... Nf6 {[%clk 0:00:44.2]} 16. Qxb7 {[%clk 0:00:44]} 16... d5 {[%clk 0:00:40.1]} 17. Bb5 {[%clk 0:00:32.1]} 17... Qb8 {[%clk 0:00:35.8]} 18. Qxb8+ {[%clk 0:00:31]} 18... Rxb8 {[%clk 0:00:34.9]} 19. Bd3 {[%clk 0:00:30.9]} 19... Kf7 {[%clk 0:00:33]} 20. O-O {[%clk 0:00:29.5]} 20... Rhe8 {[%clk 0:00:32.1]} 21. Rfd1 {[%clk 0:00:28.3]} 21... Rbd8 {[%clk 0:00:31.1]} 22. Rac1 {[%clk 0:00:26.9]} 22... Ne4 {[%clk 0:00:30.3]} 23. Bxe4 {[%clk 0:00:26.1]} 23... dxe4 {[%clk 0:00:29.4]} 24. Rd4 {[%clk 0:00:25.7]} 24... Rxd4 {[%clk 0:00:28.1]} 25. Bxd4 {[%clk 0:00:25.6]} 25... a5 {[%clk 0:00:26.6]} 26. g4 {[%clk 0:00:24.7]} 26... Ba3 {[%clk 0:00:25.2]} 27. Rc7+ {[%clk 0:00:23.3]} 27... Kg8 {[%clk 0:00:24.3]} 28. Ra7 {[%clk 0:00:21.8]} 28... Bf8 {[%clk 0:00:18.9]} 29. Rxa5 {[%clk 0:00:20.7]} 29... Rc8 {[%clk 0:00:17.8]} 30. Re5 {[%clk 0:00:19.8]} 30... Rc2 {[%clk 0:00:17]} 31. a4 {[%clk 0:00:17.8]} 31... Rb2 {[%clk 0:00:16.2]} 32. Bxb2 {[%clk 0:00:16.3]} 32... h6 {[%clk 0:00:14.7]} 33. Rxe4 {[%clk 0:00:16.2]} 33... Kh7 {[%clk 0:00:14.2]} 34. a5 {[%clk 0:00:15.8]} 34... Bb4 {[%clk 0:00:13.2]} 35. Rxb4 {[%clk 0:00:14.4]} 35... h5 {[%clk 0:00:11.6]} 36. Rb7 {[%clk 0:00:14.3]} 36... hxg4 {[%clk 0:00:11.2]} 37. Rxg7+ {[%clk 0:00:13.8]} 37... Kh6 {[%clk 0:00:10]} 38. Rxg4 {[%clk 0:00:13.7]} 38... Kh5 {[%clk 0:00:09.5]} 39. f3 {[%clk 0:00:12.2]} 39... Kh6 {[%clk 0:00:07.6]} 40. a6 {[%clk 0:00:12.1]} 40... Kh7 {[%clk 0:00:07.1]} 41. b4 {[%clk 0:00:12]} 41... Kh6 {[%clk 0:00:06.5]} 42. b5 {[%clk 0:00:11.9]} 42... Kh7 {[%clk 0:00:06.3]} 43. b6 {[%clk 0:00:11.8]} 43... Kh6 {[%clk 0:00:06]} 44. b7 {[%clk 0:00:11.5]} 44... Kh5 {[%clk 0:00:05.9]} 45. b8=Q {[%clk 0:00:10.5]} 45... Kh6 {[%clk 0:00:05.1]} 46. Qh8# {[%clk 0:00:10.4]} 1-0\n',
                                #    'time_control': '60',
                                #    'end_time': 1484816868,
                                #    'rated': True,
                                #    'tcn': 'jr0KcjZRmu!TgmKClB6SkAYQbs90mD89DS1SBJSJAJTJsJQJdJ5ZfA78JCZTCXRJAH85X545Ht91eg?8fd57acTCtCJCdB7BjBWGoE0qcY1!YWq9WG86GK6kiykjBj3VKC!3yG9zCzVNzXNEX23V2EVNnvNVGOV3rz3VzHV3HP3VPXVNX~NV5?',
                                #    'uuid': 'e2e9f646-4541-11e1-8000-000000010001',
                                #    'initial_setup': 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1',

                            games = games_data.get('games', [])#on recupere la clef 'games' et on recupere [] si la clef n'existe pas
                            
                            for i in range(len(games)):
                                indice_date = games[i]['pgn'].find("Date") #trouver l'indice de la position DATE dans la rubrique pdn du json
                                date_str = games[i]['pgn'][indice_date+6:indice_date+6+10]
                                date_partie = pd.to_datetime(datetime(int(date_str[:4]),int(date_str[5:7]),int(date_str[8:10])))
                                if date_partie > last_date :
                                    
                                    recent_games.append(games[i]) 
                                    '''
                                    [{'url': 'https://www.chess.com/game/live/1918278650',
                                    'pgn': '[Event "Live Chess"]\n[Site "Chess.com"]\n[Date "2017.01.25"]\n[Round "-"]\n[White "JolinTsai"]\n[Black "abhijit235"]\n[Result "1-0"]\n[CurrentPosition "r2k3r/1b2qp2/3Np1p1/p1np2P1/3Q1BPp/P3P2R/1PP1BK2/R7 b - -"]\n[Timezone "UTC"]\n[ECO "D00"]\n[ECOUrl "https://www.chess.com/openings/Queens-Pawn-Opening-Chigorin-Variation-2...Nf6-3.Bf4-c6-4.e3"]\n[UTCDate "2017.01.25"]\n[UTCTime "05:27:37"]\n[WhiteElo "2026"]\n[BlackElo "1931"]\n[TimeControl "180"]\n[Termination "JolinTsai won by resignation"]\n[StartTime "05:27:37"]\n[EndDate "2017.01.25"]\n[EndTime "05:30:56"]\n[Link "https://www.chess.com/game/live/1918278650"]\n\n1. d4 {[%clk 0:03:00]} 1... Nf6 {[%clk 0:03:00]} 2. Nc3 {[%clk 0:02:58.6]} 2... d5 {[%clk 0:02:57.3]} 3. Bf4 {[%clk 0:02:57.4]} 3... c6 {[%clk 0:02:54.2]} 4. e3 {[%clk 0:02:50.6]} 4... e6 {[%clk 0:02:49.4]} 5. Bd3 {[%clk 0:02:49.4]} 5... Be7 {[%clk 0:02:46.4]} 6. Nge2 {[%clk 0:02:47.2]} 6... a5 {[%clk 0:02:44.5]} 7. h4 {[%clk 0:02:45.9]} 7... h5 {[%clk 0:02:41.7]} 8. Ng3 {[%clk 0:02:43]} 8... g6 {[%clk 0:02:39.6]} 9. Nf1 {[%clk 0:02:29.8]} 9... b5 {[%clk 0:02:37.1]} 10. Nh2 {[%clk 0:02:28.9]} 10... Na6 {[%clk 0:02:23.4]} 11. Nf3 {[%clk 0:02:27.5]} 11... Nb4 {[%clk 0:02:15.2]} 12. Be2 {[%clk 0:02:26.4]} 12... Ng4 {[%clk 0:02:07.9]} 13. Ng5 {[%clk 0:02:22.6]} 13... Bxg5 {[%clk 0:02:02.2]} 14. hxg5 {[%clk 0:02:22.5]} 14... Nxf2 {[%clk 0:01:38.4]} 15. Kxf2 {[%clk 0:02:20.6]} 15... Qe7 {[%clk 0:01:25.9]} 16. a3 {[%clk 0:02:17.9]} 16... Na6 {[%clk 0:01:23.6]} 17. g4 {[%clk 0:02:15.9]} 17... h4 {[%clk 0:01:22.2]} 18. Rh3 {[%clk 0:02:13]} 18... c5 {[%clk 0:01:17.5]} 19. Nxb5 {[%clk 0:02:09]} 19... Bb7 {[%clk 0:01:09.5]} 20. Nd6+ {[%clk 0:02:03.1]} 20... Kd8 {[%clk 0:01:08.5]} 21. dxc5 {[%clk 0:01:59.2]} 21... Nxc5 {[%clk 0:01:06.9]} 22. Qd4 {[%clk 0:01:58.4]} 1-0\n',
                                    'time_control': '180',
                                    'end_time': 1485322256,
                                    'rated': True},
                                    {.....}'''
                                
                                    
                            
                            
                        elif response.status_code == 403:
                            print(f"   ✗ 403 Forbidden - Attente...")
                            time.sleep(2)
                            
                        else:
                            print(f"   ✗ Erreur {response.status_code}")
                            
                    except requests.exceptions.RequestException as e:
                        print(f"   ✗ Erreur requête: {e}")
                    
                    # Rate limiting entre chaque requête
                    time.sleep(0.5)
                else:
                    # Mois non ciblé, on skip
                    pass
            
            print(f"   ✓ {len(games)} parties récupérées")
            return recent_games
        
        except Exception as e:
            print(f"Erreur {username}: {e}")
            return []

def extraire_opening_robuste(pgn):
    """
    Extrait le nom de l'ouverture de manière robuste
    Args :
        - champ 'pgn' du fichier json

    output : extract l'opening ou N/A
    """
    # Chercher l'ECOUrl avec regex
    match = re.search(r'\[ECOUrl "https://www\.chess\.com/openings/([^"]+)"\]', pgn)
    if match:
        return match.group(1)
    
    # Fallback si regex échoue
    idx = pgn.find("https://www.chess.com/openings/")
    if idx != -1:
        start = idx + 33 #definis manuelement en general il commence la...
        end = pgn.find('"', start) #-1 si n'existe pas
        if end != -1:
            return pgn[start:end]
    
    return 'N/A'

def extraire_code_opening(pgn):
    
    """
    Extrait le code opening de manière robuste
    Args :
        - champ pgn du fichier json

    output : extract l'opening ou N/A
    """
    # Chercher l'ECOUrl avec regex
    match = re.search(r'\[ECO "([^"]+)"\]', pgn)
    
    if match:
        return match.group(1)
    
    # Fallback si regex échoue
    idx = pgn.find('[ECO "')
    if idx != -1:
        start = idx + 6#definie manuelemment , en general il est la....
        end = pgn.find('"', start) #-1 si n'existe pas
        if end != -1:
            return pgn[start:end]
    
    return 'N/A'

def Creating_df_chesscom(all_games):
    """
    :param all_games: liste de json reprenant les parties de tous les joueurs considerés
    return un df avec toutes les infos des parties des joueurs considérés
    """

    dictionnaire_partie = {'Player white' : [],'Player black' : [],'Result white':[],'Result black':[],'Date partie':[],'Heure partie':[],'ELO white': [], 'ELO black':[],'Opening':[],'Code Opening':[],'Rated':[],'Time control':[],'Type de partie':[],'Liste des coups':[]}


    for j in range(len(all_games)):
        try :
            indice_date = all_games[j]['pgn'].find("Date") #trouver l'indice de la position DATE dans la rubrique pdn du json
            dictionnaire_partie['Date partie'].append(all_games[j]['pgn'][indice_date+6:indice_date+6+10])

            indice_heure = all_games[j]['pgn'].find("UTCTime") #trouver l'indice de la position UTCDate dans la rubrique pdn du json (pour trouver l'heure)
            dictionnaire_partie['Heure partie'].append(all_games[j]['pgn'][indice_heure+9:indice_heure+9+8])

            opening = extraire_opening_robuste(all_games[j]['pgn'])
            dictionnaire_partie['Opening'].append(opening)

            opening_code = extraire_code_opening(all_games[j]['pgn'])
            dictionnaire_partie['Code Opening'].append(opening_code)
            
            startlistecoups = all_games[j]['pgn'].find("]\n\n1.")#trouver l'endroit ou commence la liste des coups dans pgn
            dictionnaire_partie['Liste des coups'].append(all_games[j]['pgn'][startlistecoups+2:])

            dictionnaire_partie['Player white'].append(all_games[j]['white']['username'])
            dictionnaire_partie['Player black'].append(all_games[j]['black']['username'])
            
            dictionnaire_partie['Result white'].append(all_games[j]['white']['result'])
            dictionnaire_partie['Result black'].append(all_games[j]['black']['result'])

            dictionnaire_partie['ELO white'].append(all_games[j]['white']['rating'])
            dictionnaire_partie['ELO black'].append(all_games[j]['black']['rating'])

            dictionnaire_partie['Rated'].append(all_games[j]['rated'])

            dictionnaire_partie['Time control'].append(all_games[j]['time_control'])

            dictionnaire_partie['Type de partie'].append(all_games[j]['time_class'])

        except Exception as e:
            continue
            
    df = pd.DataFrame(dictionnaire_partie)
    return df

def get_top_players(perf_type='blitz', count=50):
    """
    Récupère les top joueurs pour un type de jeu
    perf_type: 'bullet', 'blitz', 'rapid', 'classical', 'correspondence', 'chess960', etc.
    """
    print(f"\n Récupération top {count} joueurs {perf_type}...")
    
    try:
        # Récupérer le leaderboard
        leaderboard = clientlichess.users.get_leaderboard(perf_type, count=count) #utilisation de la methode get_leadboard de l'API pour recuperation du top
        
        usernames = [player['username'] for player in leaderboard]
        print(f"    {len(usernames)} joueurs récupérés")
        
        return usernames
    
    except Exception as e:
        print(f"    Erreur: {e}")
        return []

def get_player_recent_games(username, max_games=10000, perf_type=None, since_days=150):
    """
    Récupère les parties récentes d'un joueur
    
    Args:
        username: nom du joueur
        max_games: nombre max de parties à récupérer
        perf_type: 'bullet', 'blitz', 'rapid', 'classical' ou None pour tous
        since_days: récupérer les parties des X derniers jours
    """
    print(f"Récupération parties de {username}...", end=" ")
    
    try:
        # Calculer la date de début (timestamp en millisecondes) que l'on transforme en secondes
        since = int((datetime.now() - timedelta(days=since_days)).timestamp() * 1000)
        
        # Récupérer les parties en utilisant la methode export_by_player de l'API
        games = clientlichess.games.export_by_player(
            username,
            since=since,
            max=max_games,
            perf_type=perf_type,
            evals=False,  # Pas besoin des évaluations
            opening=True,  # Inclure les informations d'ouverture
            moves=True,    # Inclure les coups
            tags=True,     # Inclure les métadonnées
            clocks=False   # Pas besoin des temps de réflexion
        )
        #games : <generator object Games.export_by_player at 0x0000021AEF631D80>
        # Convertir le générateur en liste
        games_list = list(games)
        #games_list
        # [{'id': 'fnrnxUYd',
        # 'rated': True,
        # 'variant': 'standard',
        # 'speed': 'blitz',
        # 'perf': 'blitz',
        # 'createdAt': datetime.datetime(2026, 2, 26, 14, 24, 3, 598000, tzinfo=datetime.timezone.utc),
        # 'lastMoveAt': datetime.datetime(2026, 2, 26, 14, 30, 22, 800000, tzinfo=datetime.timezone.utc),
        # 'status': 'resign',
        # 'source': 'friend',
        # 'players': {'white': {'user': {'name': 'shby-i-ro-la-ysahl','id': 'shby-i-ro-la-ysahl'},'rating': 1998,'ratingDiff': -5},
        # 'black': {'user': {'name': 'BoozyGuy', 'id': 'boozyguy'},'rating': 2200,'ratingDiff': 122,'provisional': True}},
        # 'winner': 'black',
        # 'opening': {'eco': 'B52',
        # 'name': 'Sicilian Defense: Moscow Variation, Sokolsky Variation',
        # 'ply': 9},
        # 'moves': 'e4 c5 Nf3 d6 Bb5+ Bd7 Bxd7+ Qxd7 c4 Nf6 e5 dxe5 Nxe5 Qd4 Qa4+ Nbd7 Nxd7 Nxd7 O-O a6 Nc3 Rd8 b3 e6 Re1 Bd6 Ne4 O-O Bb2 Qxb2 Nxd6 Nf6 Nxb7 Rxd2 Rf1 Rxf2 Rxf2 Qxa1+ Rf1 Qd4+ Kh1 e5 Qc6 e4 Nxc5 e3 b4 Ng4 Qf3 Nf2+ Kg1 Re8 Rxf2 exf2+',
        # 'clock': {'initial': 180, 'increment': 2, 'totalTime': 260}},
        # {'id': 'JNYwdv4P',
        # 'rated': True,
        # 'variant': 'standard',
        # 'speed': 'blitz',
        print(f"{len(games_list)} parties recupérées")
        
        return games_list
    
    except Exception as e:
        print(f"✗ Erreur: {e}")
        return []

def extract_all_top_players_games(players_DB,
                                  categories=['blitz', 'rapid', 'classical', 'bullet'], 
                                  top_n=50, 
                                  max_games_per_player=10000,
                                  since_days=150):
    """
    Extrait les parties des meilleurs joueurs pour chaque categorie 
    args: 
        Player_DB : liste joueurs deja present dans la DB
        categories : liste representant toutes les categories de partie
        top_n : nombre de joueur par categories à recuperer (les meilleurs)
        max_games_per_player : nombre max de parties à recuperer par joueur
        since_days : durée à considerer pour extraire les parties, par defaut 150 -> on va extraire les parties des 150 derniers joueurs pour un joueur
    retourne un dictionnaire avec toutes les parties
    """
    all_data = {} #init dicp
    
    for category in categories: #pour chaque cat
        print(f"\n{'='*70}")
        print(f"CATÉGORIE: {category.upper()}")
        print(f"{'='*70}")
        
        # Récupérer les top joueurs pour la categorie en cours
        top_players = get_top_players(category, count=top_n)
        
        if not top_players:
            continue
        
        category_games = {}
        
        # Récupérer les parties de chaque joueur
        for i, player in enumerate(top_players, 1):
            print(f"[{i}/{len(top_players)}] {player}")
            if player not in players_DB:
                
                print("nouveau joueur ",player)
                games = get_player_recent_games(
                    player, 
                    max_games=max_games_per_player,
                    perf_type=category,
                    since_days=since_days
                )
                
            else:
                
                SK = recovering_SK_from_name_in_DB(player)
                _,_,nb_jours = recovering_last_played_date_from_DB(SK)
                games = get_player_recent_games(
                    player, 
                    max_games=max_games_per_player,
                    perf_type=category,
                    since_days=nb_jours
                )
            category_games[player] = games # la clef : joueur, les valeurs :liste de partie
            time.sleep(1)  # Attendre 1 seconde avant de réessayer

        all_data[category] = category_games # la clef : joueur existant , les valeurs :liste de partie
        
        # Statistiques
        total_games = sum(len(games) for games in category_games.values())
        print(f"\n {category}: {total_games} parties au total")
    
    return all_data #return dico

def Creating_df_lichess(datas):
    '''
    Fonction prenant en paramètre un dictionnaire avec clef = nomdujoueur : valeurs : liste de parties de ce joueur
    Permettant de remplir un autre dictionnaire avec les infos du dico passé en param
    return un df 
    '''
    dictionnaire_partie_lichess = {'Player white' : [],'Player black' : [],'Result white':[],'Result black':[],'Date partie':[],'Heure partie':[],'ELO white': [], 'ELO black':[],'Opening':[],'Code Opening':[],'Rated':[],'Time control':[],'Type de partie':[],'Liste des coups':[]}
    
    
    # datas = 
    # {'blitz': {'CalisthenicsBoy': [{'id': 'tR9q9Waf',
    # 'rated': False,
    # 'variant': 'fromPosition',
    # 'speed': 'blitz',

    for cle_cadence in datas.keys():#'blitz, rapid, classical, bullet

        data_post_cadence=datas[cle_cadence] #on accede à la clef de la cadence en cours
        # {'CalisthenicsBoy': [{'id': 'tR9q9Waf',
        # 'rated': False,
        # 'variant': 'fromPosition',
        # 'speed': 'blitz',
        # 'perf': 'blitz',
        # 'createdAt': datetime.datetime(2026, 2, 26, 19, 38, 45, 721000, tzinfo=datetime.timezone.utc),
        # 'lastMoveAt': datetime.datetime(2026, 2, 26, 19, 40, 44, 914000, tzinfo=datetime.timezone.utc),

        for player in data_post_cadence.keys():#'nom des joueurs = clef
            data_post_players = data_post_cadence[player] #[{'id': 'aEC7Azed',  'rated': True,  'variant': 'standard',  'speed': 'blitz','createdAt': datetime.datetime(2026, 2, 3, 18, 37, 17, 633000, tzinfo=datetime.timezone.utc),
            for i in range(len(data_post_players)):#{'id': 'aEC7Azed',  'rated': True,  'variant': 'standard',  'speed':
                try:
                    dictionnaire_partie_lichess['Player white'].append(data_post_players[i]['players']['white']['user']['name'])#remplir player white
                    dictionnaire_partie_lichess['Player black'].append(data_post_players[i]['players']['black']['user']['name'])#remplir player black
                except:
                    
                    if len(dictionnaire_partie_lichess['Player white']) > len(dictionnaire_partie_lichess['Player black']): #si il y a un probleme, on vérifie si quelque chose a été append dans le dico
                        dictionnaire_partie_lichess['Player white'].pop()
                    continue

                #remplir result white et result black
                try: #car si c'est draw ou out of time ou resign ou autre il n'y a pas de clef winner
                    if data_post_players[i]['winner']=='white':
                        dictionnaire_partie_lichess['Result white'].append('win')
                        dictionnaire_partie_lichess['Result black'].append('resigned')
                    elif data_post_players[i]['winner']=='black':
                        dictionnaire_partie_lichess['Result white'].append('resigned')
                        dictionnaire_partie_lichess['Result black'].append('win')
                    else:
                        print("Ce n'est pas censé arrivé dans le try remplissage de result")
                        print(data_post_players[i]['winner'])
                except:
                    if data_post_players[i]['status']=='draw':
                        dictionnaire_partie_lichess['Result white'].append('draw')
                        dictionnaire_partie_lichess['Result black'].append('draw')
                    else:#dans le cas ou out of time ou resign ou autre
                        if data_post_players[i]['status'] in ['resign','outoftime','timeout']:
                            try:
                                if data_post_players[i]['players']['white']['ratingDiff'] >=0:# si le rating diff de white est superieur ou égal à 0 -> white win
                                    dictionnaire_partie_lichess['Result white'].append('win')
                                    dictionnaire_partie_lichess['Result black'].append('resigned')
                                else:
                                    dictionnaire_partie_lichess['Result white'].append('resigned')
                                    dictionnaire_partie_lichess['Result black'].append('win')
                            except:
                                
                                dictionnaire_partie_lichess['Player white'].pop()
                                dictionnaire_partie_lichess['Player black'].pop()
                                
                                continue
                        
                        elif data_post_players[i]['status'] in ['stalemate','insufficientMaterialClaim']:
                            dictionnaire_partie_lichess['Result white'].append('draw')
                            dictionnaire_partie_lichess['Result black'].append('draw')
                        else:
                            print("Ce n'est pas censé arrivé dans le except remplissage de result")
                            print(data_post_players[i])

                
                #Remplir date et heure partie
                dictionnaire_partie_lichess['Date partie'].append(data_post_players[i]['createdAt'].date())#rentrer en datetime
                dictionnaire_partie_lichess['Heure partie'].append(data_post_players[i]['createdAt'].time())#rentrer en datetime
                
                #Remplir Elo white et ELO black 
                try:
                    dictionnaire_partie_lichess['ELO white'].append(data_post_players[i]['players']['white']['rating'])
                    dictionnaire_partie_lichess['ELO black'].append(data_post_players[i]['players']['black']['rating'])
                except:
                                    
                    if len(dictionnaire_partie_lichess['ELO black']) <len(dictionnaire_partie_lichess['ELO white']):#si il y a un probleme, on vérifie si quelque chose a été append dans le dico et on supprime 
                        
                        dictionnaire_partie_lichess['ELO white'].pop()#
                        dictionnaire_partie_lichess['Player white'].pop()
                        dictionnaire_partie_lichess['Player black'].pop()
                        dictionnaire_partie_lichess['Date partie'].pop()#
                        dictionnaire_partie_lichess['Heure partie'].pop()#
                        dictionnaire_partie_lichess['Result white'].pop()#
                        dictionnaire_partie_lichess['Result black'].pop()#
                    else:

                        dictionnaire_partie_lichess['Player white'].pop()
                        dictionnaire_partie_lichess['Player black'].pop()
                        dictionnaire_partie_lichess['Date partie'].pop()#
                        dictionnaire_partie_lichess['Heure partie'].pop()#
                        dictionnaire_partie_lichess['Result white'].pop()#
                        dictionnaire_partie_lichess['Result black'].pop()#

                    continue
                
                #Remplir Opening et opening code
                try :
                    dictionnaire_partie_lichess['Opening'].append(data_post_players[i]['opening']['name'])
                    dictionnaire_partie_lichess['Code Opening'].append(data_post_players[i]['opening']['eco'])
                except:
                    if data_post_players[i]['rated']== False:
                        
                        dictionnaire_partie_lichess['ELO white'].pop()#
                        dictionnaire_partie_lichess['ELO black'].pop()#
                        dictionnaire_partie_lichess['Player white'].pop()
                        dictionnaire_partie_lichess['Player black'].pop()
                        dictionnaire_partie_lichess['Date partie'].pop()#
                        dictionnaire_partie_lichess['Heure partie'].pop()#
                        dictionnaire_partie_lichess['Result white'].pop()#
                        dictionnaire_partie_lichess['Result black'].pop()#
                        
                        continue
                        
                    else:
                        print("except opening avec rated != false")
                        dictionnaire_partie_lichess['ELO white'].pop()#
                        dictionnaire_partie_lichess['ELO black'].pop()#
                        dictionnaire_partie_lichess['Player white'].pop()
                        dictionnaire_partie_lichess['Player black'].pop()
                        dictionnaire_partie_lichess['Date partie'].pop()#
                        dictionnaire_partie_lichess['Heure partie'].pop()#
                        dictionnaire_partie_lichess['Result white'].pop()#
                        dictionnaire_partie_lichess['Result black'].pop()#
                        
                        continue
                #Remplir rated

                dictionnaire_partie_lichess['Rated'].append(data_post_players[i]['rated'])

                #Remplir Time control

                dictionnaire_partie_lichess['Time control'].append(data_post_players[i]['clock']['initial'])

                #Remplir Liste des coups

                dictionnaire_partie_lichess['Liste des coups'].append(data_post_players[i]['moves'])

                #Remplir Type de partie

                dictionnaire_partie_lichess['Type de partie'].append(cle_cadence)

    df_lichess = pd.DataFrame(dictionnaire_partie_lichess)
    return df_lichess
            
def telecharger_twic(numero_debut, numero_fin, dossier_destination='pgn_files'):
    """
    Télécharge les fichiers TWIC (The Week in Chess)
    numero_debut, numero_fin : numéros des éditions TWIC (ex: 1500 à 1510)
    """
    if not os.path.exists(dossier_destination): #si le dossier n'existe pas on le crée
        os.makedirs(dossier_destination)
    
    base_url = "https://theweekinchess.com/zips/twic"
    
    # Headers pour simuler un navigateur
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/zip, application/octet-stream, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    }
    
    for numero in range(numero_debut, numero_fin + 1):
        try:
            # Format: twic1560g.zip
            url = f"{base_url}{numero}g.zip"
            print(f"Téléchargement de TWIC {numero}...")
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Extraire le ZIP
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
                zip_ref.extractall(dossier_destination)
            
            print(f"TWIC {numero} téléchargé et extrait")
            
            # Pause pour ne pas surcharger le serveur
            time.sleep(1)
        
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"TWIC {numero} n'existe pas (404)")
            else:
                print(f"Erreur HTTP pour TWIC {numero}: {e}")
        except zipfile.BadZipFile:
            print(f"TWIC {numero}: fichier ZIP corrompu")
        except Exception as e:
            print(f"Erreur pour TWIC {numero}: {e}")

# Vérifier d'abord quel est le dernier numéro disponible
def trouver_dernier_twic():
    """
    Trouve le dernier numéro TWIC disponible et le retourne
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    # Commencer par un numéro récent et chercher
    for numero in range(2000, 1550, -1):  # Chercher en arrière depuis 2000
        url = f"https://theweekinchess.com/zips/twic{numero}g.zip"
        try:
            response = requests.head(url, headers=headers, timeout=10)
            if response.status_code == 200:
                print(f"✓ Dernier TWIC trouvé : {numero}")
                return numero
        except:
            continue
    
    return None

def extraire_infos_pgn(chemin_fichier_pgn):
    """
    Extrait les informations essentielles d'un fichier PGN et retourne un DataFrame
    args: chemin d'un fichier pgn
    """
    dictionnaire_parties = {
        'Player white': [],
        'Player black': [],
        'Result white': [],
        'Result black': [],
        'Date partie': [],
        'Heure partie':[],
        'ELO white': [],
        'ELO black': [],
        'Opening': [],
        'Code Opening': [],
        'Rated':[],
        'Time control': [],
        'Type de partie': [],
        'Liste des coups':[]
    }
    
    compteur = 0
    
    print(f"Lecture du fichier : {chemin_fichier_pgn}")
    
    with open(chemin_fichier_pgn, 'r', encoding='utf-8', errors='ignore') as pgn_file:
        while True:
            try:
                partie = chess.pgn.read_game(pgn_file) #module de la bibliotheque python-chess .pgn pour gerer un pgn dejà ouvert
                
                if partie is None:
                    break
                
                headers = partie.headers # certaines infos sont dans le .header(nom du joueur blanc, nom du joueur noir,result et date)
                
                # Noms des joueurs
                dictionnaire_parties['Player white'].append(headers.get('White', 'Unknown'))
                dictionnaire_parties['Player black'].append(headers.get('Black', 'Unknown'))
                
                # Résultats
                result = headers.get('Result', '*')
                if result == '1-0':
                    dictionnaire_parties['Result white'].append('Win')
                    dictionnaire_parties['Result black'].append('Loss')
                elif result == '0-1':
                    dictionnaire_parties['Result white'].append('Loss')
                    dictionnaire_parties['Result black'].append('Win')
                elif result == '1/2-1/2':
                    dictionnaire_parties['Result white'].append('Draw')
                    dictionnaire_parties['Result black'].append('Draw')
                else:
                    dictionnaire_parties['Result white'].append('Unknown')
                    dictionnaire_parties['Result black'].append('Unknown')
                
                # Date
                dictionnaire_parties['Date partie'].append(headers.get('Date', 'Unknown'))

                #Heure partie
                dictionnaire_parties['Heure partie'].append(None)
                
                # ELO
                dictionnaire_parties['ELO white'].append(headers.get('WhiteElo', 'Unknown'))
                dictionnaire_parties['ELO black'].append(headers.get('BlackElo', 'Unknown'))
                
                # Opening
                dictionnaire_parties['Opening'].append(headers.get('Opening', 'Unknown'))
                dictionnaire_parties['Code Opening'].append(headers.get('ECO', 'Unknown'))
                
                # Time control
                time_control = headers.get('TimeControl', 'Unknown')
                dictionnaire_parties['Time control'].append(time_control)
                
                # Type de partie
                type_partie = determiner_type_partie(time_control, headers.get('Event', ''))
                dictionnaire_parties['Type de partie'].append(type_partie)
                
                # Infos supplémentaires
                dictionnaire_parties['Rated'].append(None)
                dictionnaire_parties['Liste des coups'].append(None)
                
                
                compteur += 1
                
                if compteur % 100 == 0:
                    print(f"  {compteur} parties traitées...")
            
            except Exception as e:
                print(f"Erreur lors de la lecture d'une partie : {e}")
                continue
    
    print(f"✓ Total : {compteur} parties extraites")
    
    return pd.DataFrame(dictionnaire_parties)

def determiner_type_partie(time_control, event):
    """
    Détermine le type de partie (blitz, rapid, classical, bullet)
    """
    # Si pas de time control, regarder l'événement
    if time_control == 'Unknown' or time_control == '-' or time_control == '':
        event_lower = event.lower()
        if 'blitz' in event_lower:
            return 'blitz'
        elif 'rapid' in event_lower:
            return 'rapid'
        elif 'bullet' in event_lower:
            return 'bullet'
        else:
            return 'classical'  # Par défaut pour OTB
    
    # Parsing du time control (format: "base+increment" ou juste "base")
    try:
        if '+' in time_control:
            base = int(time_control.split('+')[0])
        else:
            base = int(time_control)
        
        # Classification FIDE
        if base < 180:  # moins de 3 minutes
            return 'bullet'
        elif base < 600:  # moins de 10 minutes
            return 'blitz'
        elif base < 3600:  # moins de 60 minutes
            return 'rapid'
        else:
            return 'classical'
    except:
        return 'classical'

def extraire_tous_pgn(avdernier,dernier,dossier='pgn_files'):
    """
    Extrait toutes les parties de tous les fichiers PGN d'un dossier
    """
    print (avdernier)
    print(dernier)
    fichiers_pgn = glob.glob(os.path.join(dossier, '*.pgn'))#parcourt le dossier et retourne une liste des fichiers qui correspondent au pattern
    
    print(f"Trouvé {len(fichiers_pgn)} fichiers PGN")
    print("="*60)
    
    all_dataframes = []
    
    for fichier in fichiers_pgn:
        
        if (fichier == r"pgn_files\twic"+str(avdernier)+".pgn") or (fichier == r"pgn_files\twic"+str(dernier)+".pgn"):#on ne traite que les deux dernieres semaines

            print(f"\nTraitement de : {os.path.basename(fichier)}")
            try:
                df = extraire_infos_pgn(fichier)
                    
                all_dataframes.append(df)
            except Exception as e:
                    
                print(f" Erreur avec {fichier}: {e}")
    
    # Combiner tous les DataFrames
    if all_dataframes:
        df_final = pd.concat(all_dataframes, ignore_index=True)
        print("\n" + "="*60)
        print(f" TOTAL : {len(df_final)} parties extraites de {len(all_dataframes)} fichiers")
        return df_final
    else:
        print("✗ Aucune donnée extraite")
        return None

def handle_date(row):
    date = row["Date partie"]
    
    if date == "????-??-??":
        return "1980-01-01"
    if len(date) < 10:
        print(len(date))
        print(date)
        print("moins de 10")
        return "1980-01-01"
    
    return date

def Remplissage_Time_control(row):
    
    if (row['Time control'] == '1/259200') or (row['Time control'] == '1/604800') or (row['Time control'] == '1/86400') or (row['Time control'] == '1/432000')\
        or (row['Time control'] == '1/1209600') or (row['Time control'] == '1/172800') or(row['Time control'] == '1/864000'):
        return '5400' #1h30
    if row['Type de partie'] == 'classical' and row['Time control'] == 'Unknown':
        return '5400'
    elif row['Type de partie'] == 'rapid' and row['Time control'] == 'Unknown':
        return '900'#0h15
    elif row['Type de partie'] == 'blitz' and row['Time control'] == 'Unknown':
        return '300'#0h05
    elif row['Type de partie'] == 'bullet' and row['Time control'] == 'Unknown':
        return '120'#0h02
    else:
        return row['Time control']

if __name__=="__main__":

    #Recupere tous les joueurs deja dans la BDD
    players_DB = recovering_players_from_DB()
    
    ###################################################################################################################
    #                                   Chess.com
    ###################################################################################################################
    start = time.time() #start time pour véification du temps d'execution
    clientchesscom = ChessDotComClient(user_agent="MyChessApp/1.0 (contact@example.com)")#appel du constructeur de l'api chesscom avec son user agent (identifiant)
    print("*"*50)
    print("Chess.com")
    print("*"*50)
    print("Récupération du top 50 par type de partie chesscom")
    players_list = get_active_players(clientchesscom)
    timetop50chesscom = time.time()
    print(f"Temps d'exécution timetop50chesscom : {timetop50chesscom - start:.4f} secondes")
    print(f"\nListe finale: {len(players_list)} joueurs")

    
    #on parcourt tous les joueurs
    all_games = []
    for i, player in enumerate(players_list, 1):#enumerate(liste, start (on met 1 pour l'affichage)
        print(f"[{i}/{len(players_list)}] {player}...")
        games = get_player_recent_games_chesscom(player,players_DB)#chaque joueur est envoyé en parametre de la fonction get_player_recent_games
        #retourne une liste avec les json de toutes ses parties
        all_games.extend(games)#extend la liste all_games
        time.sleep(0.3)  # Respecter l'API
        print("all_games",len(all_games))

    print(f"\n{'='*50}")
    print(f"Total de parties récupérées : {len(all_games)}")
    recoverygames = time.time()
    print(f"Temps d'exécution : {recoverygames - start:.4f} secondes")

    df= Creating_df_chesscom(all_games=all_games) #recupere les infos interessantes de toutes les parties des joueurs considérés

    dfchesscom = time.time()
    print(f"Temps d'exécution : {dfchesscom - start:.4f} secondes")

    df_parties=df[df["Opening"]!="N/A"] #on filtre le dataframe en enlevant la ou l'ouverture n'est pas presente car cela ne nous sert a rien dans l'analyse

    #Assurer le typage de date partie et heure partie
    df_parties["Date partie"]=pd.to_datetime(df_parties["Date partie"],format='mixed') #YYYY-MM-DD
    df_parties["Heure partie"]=pd.to_datetime(df_parties["Heure partie"],format='%H:%M:%S').dt.time
    df_chesscom = df_parties.copy()
    df_chesscom['Date partie']= pd.to_datetime(df_chesscom['Date partie']) #c'est peut etre de la parano
    df_chesscom["Heure partie"]=pd.to_datetime(df_chesscom["Heure partie"],format='%H:%M:%S').dt.time

    #sauvegarde intermediaire
    df_chesscom.to_csv(r"./df_chesscom.csv")

    finchesscom = time.time()
    print(f"Temps d'exécution : {finchesscom - start:.4f} secondes")

    
    ###################################################################################################################
    #                                   Lichess
    ###################################################################################################################

    print("="*70)
    print("EXTRACTION DES PARTIES LICHESS")
    print("="*70)
    # Créer le client Lichess 
    clientlichess = berserk.Client() #instance de la classe API berserk

    # Parametres par defaut
    CATEGORIES = ['blitz', 'rapid', 'classical', 'bullet']
    TOP_N = 50  # Top 50 par catégorie
    MAX_GAMES = 10000  # Max 10000 parties par joueur
    SINCE_DAYS = 150  # Parties des 150 derniers jours (5 mois)
    
    # Extraction
    data = extract_all_top_players_games(
        players_DB,
        categories=CATEGORIES,
        top_n=TOP_N,
        max_games_per_player=MAX_GAMES,
        since_days=SINCE_DAYS
        
    )
    finextractlichess = time.time()
    print(f"Temps d'exécution : {finextractlichess - start:.4f} secondes")
    
    df_lichess = Creating_df_lichess(data)

    df_lichess['Date partie']= pd.to_datetime(df_lichess['Date partie'])
    df_lichess["Heure partie"] = pd.to_datetime(df_lichess["Heure partie"], errors="coerce").dt.strftime("%H:%M:%S")
    df_lichess.to_csv("./df_lichess.csv")

    finlichess = time.time()
    print(f"Temps d'exécution : {finlichess - start:.4f} secondes")
    
    ###################################################################################################################
    #                                   TWIC
    ###################################################################################################################

    print("Recherche du dernier numéro TWIC disponible...")
    dernier = trouver_dernier_twic()

    if dernier:
        # Télécharger les 2 derniers
        print(f"\nTéléchargement de TWIC {dernier-2} à {dernier}")
        telecharger_twic(dernier - 2, dernier)
    
    
    df_complet = extraire_tous_pgn(dernier-2,dernier,'pgn_files')

    # Gerer la date partie
    df_complet['Date partie']=df_complet['Date partie'].str.replace(".","-")
    df_complet['Date partie'] = df_complet.apply(handle_date,axis=1)
    df_complet['Date partie']=pd.to_datetime(df_complet['Date partie'])

    if df_complet is not None:
        df_complet.to_csv('Parties_twic.csv', index=False, encoding='utf-8')
        print("Toutes les parties sauvegardées dans 'Parties_twic.csv'")
    
    ###################################################################################################################
    #                                   Concatenation des 3 df
    ###################################################################################################################
    
    df_chesscom = pd.read_csv(r"C:\Users\rasse\_Data_analyst\Formation_Technobel\Maker hub\df_chesscom.csv") ###########
    df_lichess = pd.read_csv(r"C:\Users\rasse\_Data_analyst\Formation_Technobel\Maker hub\df_lichess.csv")#############
    df_complet_concat = pd.concat([df_chesscom,df_lichess,df_complet])
    df_complet_concat.to_csv("./df_complet_concat.csv")

    df_complet_concat= df_complet_concat[
    (df_complet_concat['Result black'] != 'Unknown') & 
    (df_complet_concat['Result white'] != 'Unknown')
    ]   

    # Dictionnaire de mapping
    result_mapping = {
        # Victoires
        'win': 'Win',
        'Win': 'Win',
        
        # Défaites
        'Loss': 'Loss',
        'resigned': 'Loss',
        'checkmated': 'Loss',
        'timeout': 'Loss',
        'abandoned': 'Loss',
        
        # Nuls
        'draw': 'Draw',
        'Draw': 'Draw',
        'stalemate': 'Draw',
        'insufficient': 'Draw',
        'timevsinsufficient': 'Draw',
        'repetition': 'Draw',
        'agreed': 'Draw',
        '50move': 'Draw',
        

    }

    # Application du mapping
    df_complet_concat['Result black'] = df_complet_concat['Result black'].map(result_mapping)
    df_complet_concat['Result white'] = df_complet_concat['Result white'].map(result_mapping)

    #supprimer les parties non rated
    df_complet_concat = df_complet_concat[
    (df_complet_concat['Rated'] != False) ]

    # Dictionnaire de mapping
    type_partie_mapping = {
        
        'daily': 'classical', 
        'bullet' : 'bullet',
        'blitz' : 'blitz',
        'rapid':'rapid',
        'classical' : 'classical'

    }

    # Application du mapping
    df_complet_concat['Type de partie'] = df_complet_concat['Type de partie'].map(type_partie_mapping)
    
    print("=== NETTOYAGE DES UNKNOWN ===")
    print(f"Nombre de lignes AVANT nettoyage: {len(df_complet_concat)}")

    # Supprimer les lignes où il y a Unknown dans ELO white, ELO black, Opening ou Code Opening
    df_games = df_complet_concat[
        (df_complet_concat['ELO white'] != 'Unknown') & 
        (df_complet_concat['ELO black'] != 'Unknown') &
        (df_complet_concat['Opening'] != 'Unknown') &
        (df_complet_concat['Code Opening'] != 'Unknown')
    ].copy()

    print(f"Nombre de lignes APRÈS nettoyage: {len(df_games)}")
    print(f"Lignes supprimées: {len(df_complet_concat) - len(df_games)}")

    # Convertir les ELO en numérique maintenant qu'on a supprimé les Unknown
    df_games['ELO white'] = pd.to_numeric(df_games['ELO white'])
    df_games['ELO black'] = pd.to_numeric(df_games['ELO black'])

    print("✓ Nettoyage terminé")

    df_games['Time control'] = df_games.apply(Remplissage_Time_control,axis=1)

    df_games['Date partie'] = pd.to_datetime(df_games['Date partie'], format='%Y-%m-%d')

    df_games.to_csv("./df_all_games.csv")

    fin = time.time()
    print(f"Temps d'exécution : {fin - start:.4f} secondes")
    
    print("\n✅ Terminé!")


