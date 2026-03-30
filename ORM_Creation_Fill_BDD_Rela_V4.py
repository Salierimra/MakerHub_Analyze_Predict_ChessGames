#########################################################################################################################################
#                           Imports
#########################################################################################################################################

import argparse
import numpy as np
import joblib
from flask_sqlalchemy import SQLAlchemy
from flask import Flask, jsonify, request, render_template,redirect,url_for,session
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import date
import tensorflow as tf
import secrets
import hashlib

#########################################################################################################################################
#                           Initialisation Flask & DB
#########################################################################################################################################

app = Flask(__name__)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.secret_key = secrets.token_hex(32)
DB_NAME = 'BDD_rela_Chess_MakerHub_Staging'

db = SQLAlchemy()

#########################################################################################################################################
#                           Chargement du modèle DL (mode manuel uniquement)
#########################################################################################################################################

model      = None
scaler     = None
label_enc  = None

def load_ml_assets():
    """Charge le modèle Keras, le scaler et le label encoder."""
    global model, scaler, label_enc
    try:
        # Import conditionnel pour ne pas alourdir le mode automatique
        
        model     = tf.keras.models.load_model('best_chess_model_makerhub.keras')
        scaler    = joblib.load('scaler.save')
        label_enc = joblib.load('labelenc.save')
        print("Modèle DL, scaler et label encoder chargés.")
    except Exception as e:
        print(f"Erreur chargement modèle : {e}")

#########################################################################################################################################
#                           Models ORM
#########################################################################################################################################

class TimeControl(db.Model):
    __tablename__ = 'time_control'
    TC_id         = db.Column(db.Integer, primary_key=True)
    Cadence       = db.Column(db.String(10),  nullable=False)
    TP_id         = db.Column(db.Integer, db.ForeignKey('type_game.TP_id'), nullable=False, name="FK_TC_TP")
    Modified_Date = db.Column(db.Date, nullable=False)

class TypeGame(db.Model):
    __tablename__ = 'type_game'
    TP_id         = db.Column(db.Integer, primary_key=True)
    Name_TP       = db.Column(db.String(50), nullable=False)
    Modified_Date = db.Column(db.Date, nullable=False)

class Players(db.Model):
    __tablename__ = 'players'
    Players_id    = db.Column(db.Integer, primary_key=True)
    Name_Player   = db.Column(db.String(150), nullable=False)
    ELO_Class     = db.Column(db.Integer)
    ELO_Rapid     = db.Column(db.Integer)
    ELO_Blitz     = db.Column(db.Integer)
    ELO_Bullet    = db.Column(db.Integer)
    Modified_Date = db.Column(db.Date, nullable=False)

class Opening(db.Model):
    __tablename__   = 'opening'
    Opening_id      = db.Column(db.Integer, primary_key=True)
    Code_Open       = db.Column(db.String(3),   nullable=False)
    Name_Open       = db.Column(db.String(255),  nullable=False)
    Desc_Open       = db.Column(db.String(255),  nullable=True)
    Pourc_Use_Open  = db.Column(db.Float)
    Modified_Date   = db.Column(db.Date, nullable=False)

class Games(db.Model):
    __tablename__ = 'games'
    id            = db.Column(db.Integer, primary_key=True)
    Date          = db.Column(db.String(10),  nullable=False)
    Time          = db.Column(db.String(50),  nullable=False)
    Result_B      = db.Column(db.String(60),  nullable=False)
    Result_W      = db.Column(db.String(60),  nullable=False)
    ELO_W         = db.Column(db.Integer,     nullable=False)
    ELO_B         = db.Column(db.Integer,     nullable=False)
    Modified_Date = db.Column(db.Date,        nullable=False)
    Play_B_id     = db.Column(db.Integer, db.ForeignKey('players.Players_id'), nullable=False, name="FK_Games_Players_B")
    Play_W_id     = db.Column(db.Integer, db.ForeignKey('players.Players_id'), nullable=False, name="FK_Games_Players_W")
    TC_id         = db.Column(db.Integer, db.ForeignKey('time_control.TC_id'), nullable=False, name="FK_Games_TC")
    Open_id       = db.Column(db.Integer, db.ForeignKey('opening.Opening_id'), nullable=False, name="FK_Games_Open")

class User1(db.Model):
    __tablename__ = 'user1'
    id = db.Column(db.Integer, primary_key = True,autoincrement=True)
    username = db.Column(db.String(20), unique = True, nullable = False)
    api_key_hash = db.Column(db.String(128), unique = True, nullable = False)
#########################################################################################################################################
#                           Fonctions gestion de la DB
#########################################################################################################################################

def get_db_uri():
    return (
        f'mssql+pyodbc://localhost/{DB_NAME}?'
        'driver=ODBC+Driver+17+for+SQL+Server&'
        'trusted_connection=yes'
    )

def initialiser_bdd():
    """Crée la base de données et les tables (recrée tout)."""
    master_uri = (
        'mssql+pyodbc://localhost/master?'
        'driver=ODBC+Driver+17+for+SQL+Server&'
        'trusted_connection=yes'
    )
    try:
        print("Connexion à SQL Server...")
        engine = create_engine(master_uri)
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            result = conn.execute(text(f"SELECT database_id FROM sys.databases WHERE name = '{DB_NAME}'"))
            if result.fetchone() is None:
                print(f"Création de la base '{DB_NAME}'...")
                conn.execute(text(f"CREATE DATABASE {DB_NAME}"))
                print(f"Base '{DB_NAME}' créée avec succès !")
            else:
                print(f"Base '{DB_NAME}' existe déjà.")

        app.config['SQLALCHEMY_DATABASE_URI'] = get_db_uri()
        db.init_app(app)
        with app.app_context():
            print("Suppression des tables...")
            db.drop_all()
            print("Création des tables...")
            db.create_all()
            print("Tables créées avec succès !")
        return True
    except Exception as e:
        print(f"✗ Erreur : {e}")
        return False

def initialiser_bdd_short():
    """Connexion à la BDD existante sans la recréer."""
    master_uri = (
        'mssql+pyodbc://localhost/master?'
        'driver=ODBC+Driver+17+for+SQL+Server&'
        'trusted_connection=yes'
    )
    try:
        print("Connexion à SQL Server...")
        engine = create_engine(master_uri)
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            result = conn.execute(text(f"SELECT database_id FROM sys.databases WHERE name = '{DB_NAME}'"))
            if result.fetchone() is None:
                print(f"Création de la base '{DB_NAME}'...")
                conn.execute(text(f"CREATE DATABASE {DB_NAME}"))
            else:
                print(f"Base '{DB_NAME}' existe déjà.")

        app.config['SQLALCHEMY_DATABASE_URI'] = get_db_uri()
        db.init_app(app)
        with app.app_context():
            pass
        return True
    except Exception as e:
        print(f"✗ Erreur : {e}")
        return False

#########################################################################################################################################
#                           Fonctions remplissage de la DB
#########################################################################################################################################

def get_latest_elo(df, player_name, default=1000):
    if df.empty:
        return default
    last_game = df.iloc[-1]
    return int(last_game['ELO white'] if last_game['Player white'] == player_name else last_game['ELO black'])

def Filling_Players(df_games):
    print("Remplissage table Players...")
    white_players = set(df_games['Player white'].unique())
    black_players = set(df_games['Player black'].unique())
    all_players   = white_players | black_players

    with app.app_context():
        cpt = 0
        for _, player_name in enumerate(all_players, 1):
            cpt += 1
            for type_partie, col_elo in [('classical', 'ELO_Class'), ('rapid', 'ELO_Rapid'),
                                          ('blitz', 'ELO_Blitz'), ('bullet', 'ELO_Bullet')]:
                df_type = df_games[
                    ((df_games['Player white'] == player_name) | (df_games['Player black'] == player_name)) &
                    (df_games['Type de partie'] == type_partie)
                ].sort_values(by="Date partie", ascending=True).copy()

            ELO_class  = get_latest_elo(df_games[df_games['Type de partie'] == 'classical'],  player_name)
            ELO_rapid  = get_latest_elo(df_games[df_games['Type de partie'] == 'rapid'],   player_name)
            ELO_blitz  = get_latest_elo(df_games[df_games['Type de partie'] == 'blitz'],   player_name)
            ELO_bullet = get_latest_elo(df_games[df_games['Type de partie'] == 'bullet'],  player_name)

            player = Players.query.filter_by(Name_Player=player_name).first()
            if player is None:
                player = Players(Name_Player=player_name, ELO_Class=ELO_class, ELO_Rapid=ELO_rapid,
                                  ELO_Blitz=ELO_blitz, ELO_Bullet=ELO_bullet, Modified_Date=aujourdhui)
                db.session.add(player)
            else:
                player.ELO_Class  = ELO_class
                player.ELO_Rapid  = ELO_rapid
                player.ELO_Blitz  = ELO_blitz
                player.ELO_Bullet = ELO_bullet
                player.Modified_Date = aujourdhui

            if cpt % 100 == 0:
                db.session.commit()
                print(f"Players commit n° {cpt // 100}")
        db.session.commit()
        print("✓ Tous les joueurs ont été traités !")

def Filling_Opening(df_ouv, df):
    Nb_Games = df.shape[0]
    with app.app_context():
        cpt = 0
        for _, row in df_ouv.iterrows():
            cpt += 1
            df_filter_open = df[df['Opening'] == row['Opening']]
            try:
                perc_use_open = round(df_filter_open.shape[0] / Nb_Games, 2)
            except:
                perc_use_open = 0.0
            o = Opening(Code_Open=row['Code Opening'], Name_Open=row['Opening'],
                        Desc_Open="C'est une tres bonne ouverture",
                        Pourc_Use_Open=perc_use_open, Modified_Date=aujourdhui)
            db.session.add(o)
            if cpt % 100 == 0:
                print(f"Opening commit n° {cpt // 100} / {df_ouv.shape[0]}")
                db.session.commit()
        print("Opening commit final")
        db.session.commit()

def Filling_Type_Game():
    Type_Game = ['classical', 'rapid', 'blitz', 'bullet']
    with app.app_context():
        for t in Type_Game:
            TG = TypeGame(Name_TP=t, Modified_Date=aujourdhui)
            db.session.add(TG)
        db.session.commit()

def Filling_Time_Control(df):
    l = df['Time control'].unique()
    with app.app_context():
        for e in l:
            val = eval(str(e))
            if   val < 180:   TG = 24
            elif val < 600:   TG = 23
            elif val < 3600:  TG = 22
            else:             TG = 21
            TC = TimeControl(Cadence=str(e), TP_id=TG, Modified_Date=aujourdhui)
            db.session.add(TC)
        db.session.commit()

def get_player_id(classe, player_name):
    with app.app_context():
        player = db.session.query(classe).filter_by(Name_Player=player_name).first()
        return player.Players_id if player else None

def get_timecontrol_id(TC):
    with app.app_context():
        tc = db.session.query(TimeControl).filter_by(Cadence=str(TC)).first()
        return tc.TC_id if tc else None

def get_opening_id(codeopen):
    with app.app_context():
        o = db.session.query(Opening).filter_by(Code_Open=codeopen).first()
        return o.Opening_id if o else None

def Filling_Games(df):
    with app.app_context():
        cpt = 0
        for _, row in df.iterrows():
            cpt += 1
            try:    t = str(row['Heure partie'])
            except: t = '00:00:00'
            try:    d = str(row['Date partie'])[:10]
            except: d = '2026-01-01'

            Play_B_id = get_player_id(Players, row['Player black'])
            Play_W_id = get_player_id(Players, row['Player white'])
            TC_id     = get_timecontrol_id(row['Time control'])
            Open_id   = get_opening_id(row['Code Opening'])

            g = Games(Date=d, Time=t, Result_B=row['Result black'], Result_W=row['Result white'],
                      Play_B_id=Play_B_id, Play_W_id=Play_W_id, TC_id=TC_id, Open_id=Open_id,
                      Modified_Date=str(aujourdhui), ELO_W=row["ELO white"], ELO_B=row["ELO black"])
            db.session.add(g)
            if cpt % 100 == 0:
                db.session.commit()
                print(f"Games Commit n° {cpt // 100} / {df.shape[0] // 100}")
        db.session.commit()

#########################################################################################################################################
#                           Fonctions diverses
#########################################################################################################################################

def load_csv_in_chunks(csv_path, chunksize=50000):
    print(f"=== CHARGEMENT PAR CHUNKS ({chunksize} lignes) ===")
    chunks     = []
    total_rows = 0
    for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunksize), 1):
        print(f"Chunk {i}: {len(chunk)} lignes chargées")
        chunks.append(chunk)
        total_rows += len(chunk)
    print(f"\nTotal : {total_rows} lignes en {len(chunks)} chunks")
    return pd.concat(chunks, ignore_index=True)

#########################################################################################################################################
#                           Préprocessing pour la prédiction
#########################################################################################################################################

def preprocess_for_prediction(player_white_id, player_white_name,
                               player_black_id, player_black_name,
                               cadence):
    """
    Construit le vecteur de features pour le modèle DL.
    Features : ELO blanc, ELO noir, type cadence encodé,
               win_rate_white_vs_black, win_rate_black_vs_white,
               nb_parties_ensemble.
    """
    with app.app_context():

        # --- ELO selon cadence ---
        elo_map = {
            'classical': ('ELO_Class',  'ELO_Class'),
            'rapid':     ('ELO_Rapid',  'ELO_Rapid'),
            'blitz':     ('ELO_Blitz',  'ELO_Blitz'),
            'bullet':    ('ELO_Bullet', 'ELO_Bullet'),
        }
        col_w, col_b = elo_map.get(cadence, ('ELO_Rapid', 'ELO_Rapid'))

        player_w = db.session.get(Players, player_white_id)
        player_b = db.session.get(Players, player_black_id)

        elo_w = getattr(player_w, col_w, 1000) or 1000
        elo_b = getattr(player_b, col_b, 1000) or 1000
        
        # --- Nombre partie joueur blanc/noir --- 

        games_white = db.session.query(Games).filter(
        db.or_(
          Games.Play_W_id == player_white_id,
          Games.Play_B_id == player_white_id
          )
        ).all()

        nb_parties_joueur_blanc = len(games_white)

        games_black = db.session.query(Games).filter(
            db.or_(
                Games.Play_W_id == player_black_id,
                Games.Play_B_id == player_black_id)
            
        ).all()

        nb_parties_joueur_black = len(games_black)
        
        # --- Type de partie --- 

        Type_parties_map = {
            'classical': 0,
            'rapid':     1,
            'blitz':     2,
            'bullet':    3,
        }

        type_partie_num = Type_parties_map.get(cadence, 'rapid')
        
        # --- is white advantage ? --- 

        is_white_advantage = 1 if elo_w > elo_b else 0

        # --- white/black winrate last 10 --- 
        # --- 10 dernières parties du joueur blanc (en tant que blanc OU noir) ---
        last10_white = db.session.query(Games).filter(
            db.or_(
                Games.Play_W_id == player_white_id,
                Games.Play_B_id == player_white_id
            )
        ).order_by(Games.id.desc()).limit(10).all()

        wins_white = sum(
            1 for g in last10_white
            if (g.Play_W_id == player_white_id and g.Result_W in ('Win'))
            or (g.Play_B_id == player_white_id and g.Result_B in ('Win'))
        )

        # --- 10 dernières parties du joueur noir ---
        last10_black = db.session.query(Games).filter(
            db.or_(
                Games.Play_W_id == player_black_id,
                Games.Play_B_id == player_black_id
            )
        ).order_by(Games.id.desc()).limit(10).all()

        wins_black = sum(
            1 for g in last10_black
            if (g.Play_W_id == player_black_id and g.Result_W in ('Win'))
            or (g.Play_B_id == player_black_id and g.Result_B in ('Win'))
        )

        # --- ELO DIFF Adjusted ---

        elo_diff_adj = elo_w - (elo_b+35)

        # --- form diff ---

        form_diff = wins_white-wins_black

        # --- white is stronger ---

        white_stronger = 1 if elo_w> elo_b else 0
        
        # --- Vecteur final ---
        features = np.array([[elo_w, elo_b, nb_parties_joueur_blanc,nb_parties_joueur_black,type_partie_num,is_white_advantage,wins_white/10,wins_black/10,elo_diff_adj,form_diff,white_stronger]])

        features_scaled = scaler.transform(features)

        return features_scaled, {
            'elo_white': elo_w,
            'elo_black': elo_b,
            'nb_parties_blanc':nb_parties_joueur_blanc,
            'nb_parties_black':nb_parties_joueur_black,
            'cadence': cadence,
            'is_white_adv': is_white_advantage,
            'wins_white': wins_white/10,
            'wins_black': wins_black/10,
            'elo_diff_adj':elo_diff_adj,
            'form_diff' :form_diff,
            'white_stronger' :white_stronger
        }

#########################################################################################################################################
#                           Routes Flask (mode manuel uniquement)
#########################################################################################################################################



@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/players')
def api_players():
    """Retourne la liste de tous les joueurs (id + nom)."""
    with app.app_context():
        players = db.session.query(Players).order_by(Players.Name_Player).all()
        return jsonify([{'id': p.Players_id, 'name': p.Name_Player} for p in players])
    
@app.route('/register', methods=['GET','POST'])
def register():
    if request.method == 'POST':
        uname = request.form.get('uname')
        key = generate_key()
        h_key = hash_key(key)

        new_user = User1(username = uname,
                        api_key_hash = h_key)
        
        db.session.add(new_user)
        db.session.commit()
        return render_template('login_reussi.html', key = h_key)
    
    return render_template('login.html')
def generate_key():
    return "sk_" + secrets.token_urlsafe(64)
def hash_key(key):
    return hashlib.sha256(key.encode('utf-8')).hexdigest()
@app.route('/api/ressources',methods=[ 'GET','POST'])
def get_value():
    key = request.form.get('key')
    
    user = User1.query.filter_by(api_key_hash=key).first()

    if not user:
        return "Clé non valide : accès refusé"
    
    session['user'] = user.username
    session['user_id'] = user.id
  
    return redirect(url_for('index'))

# Route pour AFFICHER la page (GET)
@app.route('/predict')
def predict_page():
    return render_template('Predict.html')

@app.route('/api/predict', methods=['POST'])
def api_predict():
    """Lance le préprocessing + inférence DL et retourne la prédiction."""
    if model is None:
        return jsonify({'error': 'Modèle non chargé.'}), 500

    data = request.get_json()
    print("DATA REÇUE :", data)  # ← ajoutez ceci
    try:
      white_id = int(data.get('white_id'))
      black_id = int(data.get('black_id'))
      cadence  = data.get('cadence', 'rapid')
    except Exception as e:
        print("ERREUR API PREDICT :", e)  # ← et ceci
        return jsonify({'error': str(e)}), 500

    with app.app_context():
        pw = db.session.get(Players, white_id)
        pb = db.session.get(Players, black_id)
        if not pw or not pb:
            return jsonify({'error': 'Joueur(s) introuvable(s).'}), 404

    try:
        print("ici")
        features_scaled, meta = preprocess_for_prediction(
            white_id, pw.Name_Player,
            black_id, pb.Name_Player,
            cadence
        )

        probs = model.predict(features_scaled)[0]  # shape (3,) : [white, draw, black]

        classes    = ['white', 'draw', 'black']
        prediction = classes[int(np.argmax(probs))]

        return jsonify({
            'prediction': prediction,
            'prob_white': float(probs[0]),
            'prob_draw':  float(probs[1]),
            'prob_black': float(probs[2]),
            'meta': meta
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500
        
@app.route('/logout')
def logout():
    session.clear()   # supprime toutes les données de session
    return redirect(url_for('index'))
#########################################################################################################################################
#                           MAIN
#########################################################################################################################################

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Chess MakerHub – ORM + Flask")
    parser.add_argument(
        '--mode',
        choices=['auto', 'manuel'],
        default='auto',
        help="'auto' : mise à jour BDD sans Flask | 'manuel' : lance l'API Flask"
    )
    args = parser.parse_args()

    aujourdhui = date.today()

    # ── Initialisation commune ──────────────────────────────────────────────
    initialiser_bdd_short()

    if args.mode == 'auto':
        # ── MODE AUTOMATIQUE (planificateur Windows) ────────────────────────
        print("=" * 60)
        print("  MODE AUTO – Mise à jour BDD")
        print("=" * 60)

        df = load_csv_in_chunks('./df_all_games.csv', chunksize=50000)
        df['Date partie']    = pd.to_datetime(df['Date partie'])
        df['ELO white']      = pd.to_numeric(df['ELO white'])
        df['ELO black']      = pd.to_numeric(df['ELO black'])
        df['Time control']   = df['Time control'].astype(str)
        df['Type de partie'] = df['Type de partie'].astype(str)

        print('*' * 60)
        print("Filling Table Players")
        Filling_Players(df)

        print('*' * 60)
        print("Filling Time Control")
        Filling_Time_Control(df)

        print('*' * 60)
        print("Filling Games")
        Filling_Games(df)

        print("\n✓ Mise à jour terminée.")

    else:
        # ── MODE MANUEL (API Flask locale) ──────────────────────────────────
        print("=" * 60)
        print("  MODE MANUEL – Démarrage API Flask")
        print("  → http://localhost:5000")
        print("=" * 60)

        load_ml_assets()
        app.run(debug=True, port=5000)
