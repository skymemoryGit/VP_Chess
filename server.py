"""
Scacchi Multiplayer - server.py
--------------------------------
Backend Flask + Flask-SocketIO per un MVP di scacchi multiplayer in tempo reale.

Principi di sicurezza applicati in questo file:
- Lo stato di gioco (scacchiera, turno, orologi, risultato) vive SOLO sul server,
  in memoria RAM (nessun database). Il client non puo' mai imporre una posizione,
  un turno, un tempo residuo o un risultato: puo' solo *proporre* una mossa.
- Ogni mossa viene validata con python-chess prima di essere applicata.
- Gli orologi sono calcolati con timestamp lato server (time.monotonic()), mai
  fidandosi di timer del browser.
- Ogni socket riceve un token di sessione casuale (non un account) usato solo
  per permettere la riconnessione alla stessa partita entro 60 secondi.
- Non vengono salvati indirizzi IP, credenziali o cronologia permanente.

Eventi Socket.IO (client -> server):
    join_lobby, create_table, join_table, leave_table, attempt_move,
    choose_promotion, resign_game, offer_draw, respond_draw,
    toggle_reconnect, request_state, send_reaction, send_chat

Eventi Socket.IO (server -> client):
    lobby_state, table_joined, game_state, game_started, move_accepted,
    move_rejected, clock_sync, player_disconnected, player_reconnected,
    game_over, error_message, draw_offered, draw_declined, lobby_joined,
    reconnect_failed, reaction, chat_message

Nota: 'table_joined', 'lobby_joined', 'draw_offered', 'draw_declined',
'reconnect_failed', 'reaction' e 'chat_message' sono aggiunte rispetto alla
lista base indicata nelle specifiche, per rendere l'API piu' chiara
(esplicitamente consentito).

Profili, reazioni a frasi predefinite e chat libera:
- Non esiste piu' un nickname libero: l'utente sceglie uno dei due profili
  fissi definiti in PROFILES (nessun testo libero, niente da sanitizzare).
- Le reazioni rapide (send_reaction) non inviano mai testo libero: il
  client puo' solo mandare la CHIAVE di un'emoji predefinita. Il server
  sceglie una frase casuale dalla tabella PERSONA_PHRASES associata al
  profilo del mittente e la trasmette (evento 'reaction'). Il testo della
  frase non arriva MAI dal client in questo canale: e' sempre e solo
  scelto server-side.
- La chat libera (send_chat) invece consente testo scritto dall'utente,
  ma con vincoli stretti pensati per un'app senza account e senza
  moderazione umana: solo giocatori (non spettatori), messaggio troncato
  a CHAT_MAX_LEN caratteri, niente caratteri di controllo/andare a capo,
  e un cooldown anti-spam per posto (PlayerSlot), non per sid, cosi' da
  resistere anche a riconnessioni. Il rendering lato client usa sempre
  textContent (mai innerHTML) sul testo del messaggio.
"""

import eventlet
eventlet.monkey_patch()

import os
import re
import time
import random
import secrets
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple

import chess
from flask import Flask, render_template
from flask_socketio import SocketIO, join_room, leave_room
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("chess-mvp")

# --------------------------------------------------------------------------
# Configurazione applicazione
# --------------------------------------------------------------------------

SECRET_KEY = os.environ.get("SECRET_KEY", secrets.token_hex(32))
PORT = int(os.environ.get("PORT", "5000"))
HOST = os.environ.get("HOST", "0.0.0.0")
DEBUG = os.environ.get("DEBUG", "false").lower() == "true"

app = Flask(__name__)
app.config["SECRET_KEY"] = SECRET_KEY

socketio = SocketIO(
    app,
    async_mode="eventlet",
    cors_allowed_origins="*",
    ping_interval=10,
    ping_timeout=20,
)

LOBBY_ROOM = "lobby"

RECONNECT_GRACE_SECONDS = 60
FINISHED_TABLE_TTL_SECONDS = 60
CLOCK_TICK_SECONDS = 1
REACTION_COOLDOWN_SECONDS = 6
CHAT_MAX_LEN = 30
CHAT_COOLDOWN_SECONDS = 2

# --------------------------------------------------------------------------
# Validazione input
# --------------------------------------------------------------------------

# Nome tavolo: fino a 30 caratteri, charset ristretto (niente < > & " ' per
# evitare qualunque iniezione HTML, anche se il client usa comunque textContent).
TABLE_NAME_RE = re.compile(r"^[\w\-\.\,\!\?\: ]{0,30}$", re.UNICODE)

# Messaggio di chat: nessun carattere di controllo (niente a-capo, tab,
# ecc.) cosi' un messaggio resta sempre su una riga sola. Il charset non
# e' altrimenti ristretto (emoji e lettere accentate sono benvenute): la
# protezione da injection e' comunque garantita dal rendering client con
# textContent, mai innerHTML.
CHAT_CONTROL_CHAR_RE = re.compile(r"[\x00-\x1f\x7f]")

PROMOTION_MAP = {"q": chess.QUEEN, "r": chess.ROOK, "b": chess.BISHOP, "n": chess.KNIGHT}

# --------------------------------------------------------------------------
# Profili fissi (nessun nickname libero) e chat a frasi predefinite
# --------------------------------------------------------------------------
# Personaggi originali e di fantasia (non impersonano persone reali): un
# frate umile e un magnate teatrale. L'utente sceglie uno dei due all'entrata
# invece di digitare un nickname.
PROFILES = {
    "francesco": {"name": "Fra Francesco", "avatar": "\U0001F54A"},    # colomba
    "magnifico": {"name": "Duca Silvio", "avatar": "\U0001F3A9"},      # cilindro
    "nis2": {"name": "NIS2 Consulente", "avatar": "\U0001F4BC"},       # valigetta
    "mclaren": {"name": "Fan McLaren", "avatar": "\U0001F3C1"},        # bandiera a scacchi
}

# Chiave emoji -> carattere emoji mostrato nel pulsante e nel messaggio.
REACTIONS = {
    "joy": "\U0001F603",
    "laugh": "\U0001F602",
    "worried": "\U0001F61F",
    "thinking": "\U0001F914",
    "crying": "\U0001F62D",
    "sleepy": "\U0001F634",
    "angry": "\U0001F92C",
    "thumbsup": "\U0001F44D",
    "thumbsdown": "\U0001F44E",
    "clap": "\U0001F44F",
}

# Per ogni profilo, per ogni emoji: elenco di frasi predefinite. Il client
# NON puo' mai inviare testo libero: puo' solo scegliere una chiave emoji,
# e il server sceglie una frase a caso da questa tabella.
PERSONA_PHRASES = {
    "francesco": {
        "joy": [
            "Ringraziamo il Signore per questa bella mossa... ma restiamo umili, la partita e' ancora lunga!",
            "Bene, fratelli: anche sulla scacchiera una piccola luce puo' illuminare tutta la strada.",
        ],
        "laugh": [
            "Eh, fratello mio, anche il pedone oggi ha deciso di fare un piccolo pellegrinaggio!",
            "Capita, capita... nessuno e' perfetto, nemmeno il cavallo quando salta dove non dovrebbe.",
        ],
        "worried": [
            "Qui serve silenzio e discernimento: non paura, ma occhi aperti e cuore calmo.",
            "Siamo sotto pressione, si', ma il Signore non abbandona chi cerca una buona strada.",
        ],
        "thinking": [
            "Un momento di raccoglimento... Signore, illuminaci anche davanti a questa scacchiera.",
            "Non sempre la mossa piu' forte e' quella piu' rumorosa: cerchiamo quella piu' giusta.",
        ],
        "crying": [
            "Abbiamo perso un pezzo importante, ma non perdiamo la speranza: si riparte sempre.",
            "Anche nelle difficolta' il Signore apre una via. Vediamo se questo pedone puo' ancora camminare.",
        ],
        "sleepy": [
            "Fratello, con calma e senza ansia... ma ricordiamoci che anche il tempo e' un dono del Signore!",
            "La pazienza e' una virtu', certo... pero' non addormentiamoci tutti sulla scacchiera!",
        ],
        "angry": [
            "No, no, no: non trasformiamo una partita in una guerra. Giochiamo con rispetto.",
            "Il diavolo ama la divisione; noi scegliamo correttezza, dialogo e una mossa legale.",
        ],
        "thumbsup": [
            "Bella mossa, davvero. L'intelligenza, quando e' umile, e' un dono prezioso.",
            "Bravo, fratello: riconoscere il bene nell'altro rende piu' bella anche la partita.",
        ],
        "thumbsdown": [
            "Quella mossa non porta molto frutto, fratello. Proviamo una strada piu' saggia.",
            "No, non scoraggiarti: una scelta sbagliata non definisce tutta la tua partita.",
        ],
        "clap": [
            "Applausi: abbiamo giocato, sofferto e imparato insieme. Questa e' una piccola fraternita'.",
            "Che bella partita! Vincere e' bello, ma giocare con cuore pulito vale ancora di piu'.",
        ],
    },
    "magnifico": {
        "joy": [
            "Ecco, questa e' una mossa da campione: classe, visione e una certa genialita' naturale.",
            "Mi consenta: io non faccio mosse, io creo eventi storici sulla scacchiera!",
        ],
        "laugh": [
            "Ma dai, questa non e' una mossa: e' un regalo di compleanno, e io accetto volentieri!",
            "Magnifico! Lei mi sta aiutando a vincere con una generosita' quasi commovente.",
        ],
        "worried": [
            "Aspetti un momento... questa e' una posizione delicata, ma io nelle difficolta' do il meglio.",
            "Qui serve sangue freddo. Tranquilli: ho gestito situazioni ben piu' complicate di un cavallo aggressivo.",
        ],
        "thinking": [
            "Vediamo... qui non serve una mossa normale, serve un'idea da fuoriclasse.",
            "Datemi trenta secondi: sto elaborando un piano che cambiera' la storia di questa partita.",
        ],
        "crying": [
            "No, la regina no! Questo e' un attacco diretto al mio prestigio personale!",
            "Va bene, abbiamo perso una torre... ma io ho costruito imperi, figurarsi se mi fermo qui!",
        ],
        "sleepy": [
            "Caro amico, si svegli: a questo ritmo finiamo quando io avro' gia' scritto le mie memorie!",
            "Dai, faccia una mossa! Non siamo in una riunione senza ordine del giorno.",
        ],
        "angry": [
            "Mi consenta: questa e' una provocazione bella e buona. Ma io non mi lascio intimidire!",
            "No, no, no! Questa partita deve essere regolamentare, trasparente e soprattutto favorevole a me.",
        ],
        "thumbsup": [
            "Bravo! Finalmente una mossa degna di un avversario che vuole entrare nella storia.",
            "Le faccio i complimenti: non capita spesso, ma quando vedo qualita' la riconosco subito.",
        ],
        "thumbsdown": [
            "No, no, no... questa mossa non la approverebbe nemmeno il suo pedone!",
            "Mi faccia il piacere: con questa scelta sta facendo campagna elettorale per la mia vittoria.",
        ],
        "clap": [
            "Applausi, prego! Questa non e' una vittoria: e' una dimostrazione di superiorita'.",
            "Signore e signori, abbiamo assistito a un capolavoro. E modestamente, l'autore sono io!",
        ],
    },
    "nis2": {
        "joy": [
            "Ah, non so nulla... pero' questa mossa mi sembra molto pulita. Fondamentale.",
            "Vedi? Regola 80/20: hai trovato quella mossa che risolve quasi tutto con poco sforzo.",
        ],
        "laugh": [
            "Secondo me questa mossa dovevi tenertela per te stesso.",
            "Non fare il panzone sulla scacchiera: un pezzo alla volta, non regalare tutto insieme!",
        ],
        "worried": [
            "Relax. Prendi un caffe', respira e guarda la posizione: non serve andare in panico.",
            "Qui il rischio e' alto, ma e' come mangiare un elefante: un boccone alla volta.",
        ],
        "thinking": [
            "Quando alzo il dito e' perche' questo concetto e'... [pausa]... fonda-menta-le.",
            "Ah, non so nulla, pero' forse qui devi guardare prima le minacce e poi le opportunita'.",
        ],
        "crying": [
            "Hai perso la regina o altro? Va bene... ma non fare il disperato: ora si lavora con quello che resta.",
            "Secondo Oppenheimer, essere una bomba e' fantastico... basta che non sia atomica. Qui pero' hai appena fatto esplodere la posizione.",
        ],
        "sleepy": [
            "Relax, prenditi il tuo tempo... ma magari ascoltati «La vita com'e'» e fai una mossa, dai.",
            "Bitcoin va to the moon prima o poi, ma questo turno non puo' arrivarci da solo.",
        ],
        "angry": [
            "Ok, calma. Non reagire di pancia: ragiona. La scacchiera premia chi resta lucido.",
            "Secondo me questa provocazione dovresti tenertela per te stesso. Giochiamo la posizione, non l'ego.",
        ],
        "thumbsup": [
            "Bella mossa. Pulita, semplice, efficace: 80/20 applicato bene.",
            "Questa e' una scelta sana, come riso con pollo: non sara' glamour, ma funziona sempre.",
        ],
        "thumbsdown": [
            "No, questa e' una mossa troppo processata: meglio giocare pulito.",
            "Dovresti mangiare piu' pulito... e anche muovere piu' pulito, sinceramente.",
        ],
        "clap": [
            "Bravo. Hai gestito la partita come un progetto: priorita' chiare, un boccone alla volta.",
            "Questa vittoria e'... [alza il dito, pausa]... fonda-menta-le.",
        ],
    },
    "mclaren": {
        "joy": [
            "Questa e' pulita come una rete dopo un vulnerability assessment con Pentera.",
            "Mossa perfetta. Stasera Don Peppe Pizzeria: pizza kebab, sorriso malefico e si festeggia.",
        ],
        "laugh": [
            "Hai fatto una configurazione talmente bella che il firewall ha deciso di bloccare anche te.",
            "Questa mossa e' come mettere Proxmox dove serviva VMware: coraggiosa, ma proprio no.",
        ],
        "worried": [
            "Qui siamo messi male: serve Oplon, un PAM, Pentera e forse anche un ramen di emergenza.",
            "Aspetta... questa posizione ha una vulnerabilita' critica. CVSS dieci: la regina e' esposta.",
        ],
        "thinking": [
            "Fammi pensare: se avessi un NAS con fibra diretta a casa, scaricherei una soluzione a 2.000 mega.",
            "Aspetta, bevo un sorso e vedo se arriva l'ispirazione papaya.",
        ],
        "crying": [
            "Mi hanno preso la torre... peggio di quando il firewall chiude fuori il sistemista.",
            "Nooo, questa mossa mi ha fatto piu' male di un aggiornamento partito storto mentre configuro Oplon.",
        ],
        "sleepy": [
            "Dai fai la mossa, che oggi ho saltato pranzo e sto pensando a pizza kebab da un'ora.",
            "Se continui cosi' vado da Don Peppe prima che finisca la partita. Vuoi venire? Come no...",
        ],
        "angry": [
            "No, questa e' una mossa da accesso senza Oplon: non autorizzata e pericolosa.",
            "Ti chiudo fuori dal firewall come hanno chiuso fuori me. E fidati: il nostro e' potente.",
            "ma Va******"
        ],
        "thumbsup": [
            "Ok, questa e' fatta bene: passa Pentera, passa il firewall e merita anche una pizza kebab.",
            "Bella mossa. Questa la approva perfino il team papaya.",
        ],
        "thumbsdown": [
            "Questa e' piu' pesante di uno smash burger seguito da roll kebab e ramen.",
            "No, no: hai lasciato la regina esposta come una VPN senza PAM. Solo Oplon, sempre.",
        ],
        "clap": [
            "Spacco tutto nel mio laboratorio di reti! Matto e poi pizza kebab da Don Peppe.",
            "Vittoria papaya! Anche se oggi non si corre, si festeggia con smash burger e birra.",
        ],
    },
}


def sanitize_table_name(raw) -> Optional[str]:
    if raw is None:
        return ""
    if not isinstance(raw, str):
        return None
    value = raw.strip()
    value = re.sub(r"\s+", " ", value)
    if len(value) > 30:
        value = value[:30]
    if not TABLE_NAME_RE.match(value):
        return None
    return value


# --------------------------------------------------------------------------
# Modelli dati (in memoria)
# --------------------------------------------------------------------------

@dataclass
class TimeControl:
    key: str
    label: str
    initial: int      # secondi
    increment: int     # secondi aggiunti dopo ogni mossa completata


TIME_CONTROLS: Dict[str, TimeControl] = {
    "rapid": TimeControl("rapid", "Rapid 15+0", 15 * 60, 0),
    "blitz": TimeControl("blitz", "Blitz 5+30", 5 * 60, 30),
}


@dataclass
class PlayerSlot:
    """Un posto (Bianco o Nero) al tavolo."""
    sid: Optional[str] = None
    token: Optional[str] = None
    nickname: Optional[str] = None
    profile_id: Optional[str] = None
    connected: bool = False
    disconnect_deadline: Optional[float] = None  # time.monotonic() limite riconnessione
    last_reaction_ts: Optional[float] = None      # time.monotonic() dell'ultima reazione inviata
    last_chat_ts: Optional[float] = None          # time.monotonic() dell'ultimo messaggio di chat

    def occupied(self) -> bool:
        return self.token is not None

    def avatar(self) -> Optional[str]:
        return PROFILES.get(self.profile_id, {}).get("avatar")

    def public(self) -> dict:
        if not self.occupied():
            return {"nickname": None, "connected": False, "avatar": None}
        return {"nickname": self.nickname, "connected": self.connected, "avatar": self.avatar()}


@dataclass
class Table:
    id: str
    name: str
    time_control: TimeControl
    host_token: str
    board: chess.Board = field(default_factory=chess.Board)
    white: PlayerSlot = field(default_factory=PlayerSlot)
    black: PlayerSlot = field(default_factory=PlayerSlot)
    spectators: Dict[str, str] = field(default_factory=dict)  # sid -> nickname
    status: str = "waiting"  # waiting | playing | finished
    white_time: float = 0.0
    black_time: float = 0.0
    turn_start_ts: Optional[float] = None
    move_history_san: List[str] = field(default_factory=list)
    last_move: Optional[Tuple[str, str]] = None
    result: Optional[str] = None          # '1-0' | '0-1' | '1/2-1/2'
    result_reason: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None
    draw_offer_by: Optional[str] = None   # 'white' | 'black'

    def slot_for_color(self, color: str) -> PlayerSlot:
        return self.white if color == "white" else self.black

    def color_of_sid(self, sid: str) -> Optional[str]:
        if self.white.sid == sid:
            return "white"
        if self.black.sid == sid:
            return "black"
        return None

    def color_of_token(self, token: str) -> Optional[str]:
        if self.white.token == token:
            return "white"
        if self.black.token == token:
            return "black"
        return None

    def is_empty(self) -> bool:
        return not self.white.occupied() and not self.black.occupied()

    def host_slot(self) -> Optional[PlayerSlot]:
        if self.white.token == self.host_token:
            return self.white
        if self.black.token == self.host_token:
            return self.black
        return None

    def host_nickname(self) -> str:
        slot = self.host_slot()
        return (slot.nickname if slot else None) or "?"

    # ----- Orologi -----

    def remaining_time(self, color: str, now: float) -> float:
        base = self.white_time if color == "white" else self.black_time
        if self.status == "playing" and self.board.turn == (chess.WHITE if color == "white" else chess.BLACK):
            if self.turn_start_ts is not None:
                elapsed = now - self.turn_start_ts
                return max(0.0, base - elapsed)
        return max(0.0, base)

    def public_state(self) -> dict:
        now = time.monotonic()
        check = self.board.is_check() if self.status == "playing" else False
        legal_targets: Dict[str, List[str]] = {}
        if self.status == "playing":
            for mv in self.board.legal_moves:
                origin = chess.square_name(mv.from_square)
                dest = chess.square_name(mv.to_square)
                legal_targets.setdefault(origin, [])
                if dest not in legal_targets[origin]:
                    legal_targets[origin].append(dest)
        return {
            "table_id": self.id,
            "name": self.name,
            "time_control": {"key": self.time_control.key, "label": self.time_control.label},
            "status": self.status,
            "fen": self.board.fen(),
            "turn": "white" if self.board.turn == chess.WHITE else "black",
            "check": check,
            "white": {**self.white.public(), "time": round(self.remaining_time("white", now), 1)},
            "black": {**self.black.public(), "time": round(self.remaining_time("black", now), 1)},
            "spectators_count": len(self.spectators),
            "move_history": list(self.move_history_san),
            "last_move": list(self.last_move) if self.last_move else None,
            "result": self.result,
            "result_reason": self.result_reason,
            "draw_offer_by": self.draw_offer_by,
            "legal_targets": legal_targets,
            "white_disconnect_deadline": self._deadline_remaining(self.white, now),
            "black_disconnect_deadline": self._deadline_remaining(self.black, now),
        }

    @staticmethod
    def _deadline_remaining(slot: PlayerSlot, now: float) -> Optional[float]:
        if slot.disconnect_deadline is None:
            return None
        return max(0.0, round(slot.disconnect_deadline - now, 1))

    def lobby_view(self) -> dict:
        host = self.host_slot()
        return {
            "id": self.id,
            "name": self.name,
            "host": self.host_nickname(),
            "host_avatar": host.avatar() if host else None,
            "time_control": self.time_control.label,
            "status": self.status,
            "white": self.white.nickname,
            "black": self.black.nickname,
            "spectators": len(self.spectators),
        }


# Stato globale in memoria (nessun database)
TABLES: Dict[str, Table] = {}
# sid -> {"token": str, "nickname": str, "table_id": Optional[str]}
SESSIONS: Dict[str, dict] = {}


def new_id(n=8) -> str:
    return secrets.token_urlsafe(n)[:n]


# --------------------------------------------------------------------------
# Helper broadcast
# --------------------------------------------------------------------------

def broadcast_lobby():
    tables_view = [t.lobby_view() for t in TABLES.values()]
    socketio.emit("lobby_state", {"tables": tables_view}, room=LOBBY_ROOM)


def broadcast_game(table: Table):
    socketio.emit("game_state", table.public_state(), room=table.id)


def emit_error(sid: str, message: str):
    socketio.emit("error_message", {"message": message}, room=sid)


def get_session(sid: str) -> Optional[dict]:
    return SESSIONS.get(sid)


def get_table_or_none(table_id) -> Optional[Table]:
    if not isinstance(table_id, str):
        return None
    return TABLES.get(table_id)


def remove_table(table_id: str):
    TABLES.pop(table_id, None)


# --------------------------------------------------------------------------
# Logica di fine partita
# --------------------------------------------------------------------------

def finish_game(table: Table, result: str, reason: str):
    table.status = "finished"
    table.result = result
    table.result_reason = reason
    table.finished_at = time.time()
    table.turn_start_ts = None
    table.draw_offer_by = None
    table.white.disconnect_deadline = None
    table.black.disconnect_deadline = None
    winner = None
    if result == "1-0":
        winner = "white"
    elif result == "0-1":
        winner = "black"
    socketio.emit(
        "game_over",
        {"result": result, "reason": reason, "winner": winner},
        room=table.id,
    )
    broadcast_game(table)
    broadcast_lobby()


def evaluate_board_end(table: Table) -> Optional[Tuple[str, str]]:
    """Ritorna (result, reason) se la partita e' terminata per motivi di
    scacchiera dopo l'ultima mossa, altrimenti None."""
    board = table.board
    if board.is_checkmate():
        winner_is_white = not board.turn  # chi ha appena mosso ha vinto
        return ("1-0" if winner_is_white else "0-1", "Scacco matto")
    if board.is_stalemate():
        return ("1/2-1/2", "Patta per stallo")
    if board.is_insufficient_material():
        return ("1/2-1/2", "Patta per materiale insufficiente")
    if board.is_repetition(3):
        return ("1/2-1/2", "Patta per triplice ripetizione")
    if board.halfmove_clock >= 100:
        return ("1/2-1/2", "Patta per regola delle 50 mosse")
    return None


def forfeit_on_time(table: Table, color_out_of_time: str):
    winner_result = "0-1" if color_out_of_time == "white" else "1-0"
    finish_game(table, winner_result, "Tempo scaduto")


def forfeit_on_disconnect(table: Table, color_disconnected: str):
    winner_result = "0-1" if color_disconnected == "white" else "1-0"
    finish_game(table, winner_result, "Sconfitta per disconnessione")


# --------------------------------------------------------------------------
# Background task: orologi, timeout, disconnessioni, pulizia tavoli
# --------------------------------------------------------------------------

def background_loop():
    while True:
        socketio.sleep(CLOCK_TICK_SECONDS)
        now_mono = time.monotonic()
        now_wall = time.time()
        lobby_dirty = False
        to_remove = []

        for table in list(TABLES.values()):
            if table.status == "playing":
                turn_color = "white" if table.board.turn == chess.WHITE else "black"
                remaining = table.remaining_time(turn_color, now_mono)
                if remaining <= 0:
                    forfeit_on_time(table, turn_color)
                    lobby_dirty = True
                    continue

                for color in ("white", "black"):
                    slot = table.slot_for_color(color)
                    if slot.disconnect_deadline is not None and now_mono > slot.disconnect_deadline:
                        forfeit_on_disconnect(table, color)
                        lobby_dirty = True
                        break
                else:
                    socketio.emit("clock_sync", {
                        "white_time": round(table.remaining_time("white", now_mono), 1),
                        "black_time": round(table.remaining_time("black", now_mono), 1),
                        "turn": turn_color,
                        "white_connected": table.white.connected,
                        "black_connected": table.black.connected,
                        "white_disconnect_deadline": Table._deadline_remaining(table.white, now_mono),
                        "black_disconnect_deadline": Table._deadline_remaining(table.black, now_mono),
                    }, room=table.id)

            elif table.status == "waiting":
                if table.is_empty():
                    to_remove.append(table.id)

            elif table.status == "finished":
                if table.finished_at and (now_wall - table.finished_at) > FINISHED_TABLE_TTL_SECONDS:
                    to_remove.append(table.id)

        for tid in to_remove:
            remove_table(tid)
            lobby_dirty = True

        if lobby_dirty:
            broadcast_lobby()


# --------------------------------------------------------------------------
# Routes HTTP
# --------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


# --------------------------------------------------------------------------
# Socket.IO: connessione
# --------------------------------------------------------------------------

@socketio.on("connect")
def on_connect():
    join_room(LOBBY_ROOM)


@socketio.on("disconnect")
def on_disconnect():
    sid = request_sid()
    sess = SESSIONS.pop(sid, None)
    if not sess:
        return
    table_id = sess.get("table_id")
    if not table_id:
        return
    table = get_table_or_none(table_id)
    if not table:
        return

    if sid in table.spectators:
        table.spectators.pop(sid, None)
        broadcast_game(table)
        broadcast_lobby()
        return

    color = table.color_of_sid(sid)
    if color is None:
        return
    slot = table.slot_for_color(color)

    if table.status == "waiting":
        # Nessuna partita iniziata: il tavolo non ha senso di esistere.
        remove_table(table.id)
        broadcast_lobby()
        return

    if table.status == "playing":
        slot.sid = None
        slot.connected = False
        slot.disconnect_deadline = time.monotonic() + RECONNECT_GRACE_SECONDS
        socketio.emit("player_disconnected", {"color": color, "seconds": RECONNECT_GRACE_SECONDS}, room=table.id)
        broadcast_game(table)
        broadcast_lobby()
        return
    # status finished: nulla da fare


def request_sid() -> str:
    from flask import request
    return request.sid


# --------------------------------------------------------------------------
# Socket.IO: lobby
# --------------------------------------------------------------------------

@socketio.on("join_lobby")
def on_join_lobby(data):
    sid = request_sid()
    data = data or {}
    profile_id = data.get("profile_id")
    if profile_id not in PROFILES:
        emit_error(sid, "Profilo non valido.")
        return
    profile = PROFILES[profile_id]
    token = secrets.token_urlsafe(24)
    SESSIONS[sid] = {"token": token, "nickname": profile["name"], "profile_id": profile_id, "table_id": None}
    join_room(LOBBY_ROOM)
    socketio.emit("lobby_joined", {
        "token": token,
        "nickname": profile["name"],
        "profile_id": profile_id,
        "avatar": profile["avatar"],
    }, room=sid)
    socketio.emit("lobby_state", {"tables": [t.lobby_view() for t in TABLES.values()]}, room=sid)


@socketio.on("request_state")
def on_request_state(_data):
    sid = request_sid()
    sess = get_session(sid)
    if not sess:
        emit_error(sid, "Sessione non trovata. Scegli di nuovo il tuo profilo.")
        return
    table = get_table_or_none(sess.get("table_id"))
    if table:
        socketio.emit("game_state", table.public_state(), room=sid)
    else:
        socketio.emit("lobby_state", {"tables": [t.lobby_view() for t in TABLES.values()]}, room=sid)


# --------------------------------------------------------------------------
# Socket.IO: creazione / ingresso tavoli
# --------------------------------------------------------------------------

@socketio.on("create_table")
def on_create_table(data):
    sid = request_sid()
    sess = get_session(sid)
    if not sess:
        emit_error(sid, "Devi scegliere un profilo prima di creare un tavolo.")
        return
    if sess.get("table_id"):
        emit_error(sid, "Sei gia' in un tavolo.")
        return

    data = data or {}
    name = sanitize_table_name(data.get("name"))
    if name is None:
        emit_error(sid, "Nome tavolo non valido.")
        return
    if not name:
        name = f"Tavolo di {sess['nickname']}"

    tc_key = data.get("time_control")
    if tc_key not in TIME_CONTROLS:
        emit_error(sid, "Controllo del tempo non valido.")
        return
    time_control = TIME_CONTROLS[tc_key]

    color_choice = data.get("color_choice")
    if color_choice not in ("random", "white", "black"):
        emit_error(sid, "Scelta colore non valida.")
        return
    if color_choice == "random":
        host_color = random.choice(["white", "black"])
    else:
        host_color = color_choice

    table_id = new_id()
    while table_id in TABLES:
        table_id = new_id()

    table = Table(
        id=table_id,
        name=name,
        time_control=time_control,
        host_token=sess["token"],
    )
    table.white_time = float(time_control.initial)
    table.black_time = float(time_control.initial)

    slot = table.slot_for_color(host_color)
    slot.sid = sid
    slot.token = sess["token"]
    slot.nickname = sess["nickname"]
    slot.profile_id = sess.get("profile_id")
    slot.connected = True

    TABLES[table_id] = table
    sess["table_id"] = table_id

    join_room(table_id)
    socketio.emit("table_joined", {"table_id": table_id, "color": host_color, "role": "player"}, room=sid)
    broadcast_game(table)
    broadcast_lobby()


@socketio.on("join_table")
def on_join_table(data):
    sid = request_sid()
    sess = get_session(sid)
    if not sess:
        emit_error(sid, "Devi scegliere un profilo prima di entrare in un tavolo.")
        return
    if sess.get("table_id"):
        emit_error(sid, "Sei gia' in un tavolo.")
        return

    data = data or {}
    table = get_table_or_none(data.get("table_id"))
    if not table:
        emit_error(sid, "Tavolo non trovato.")
        return

    if table.status == "waiting" and (not table.white.occupied() or not table.black.occupied()):
        empty_color = "white" if not table.white.occupied() else "black"
        slot = table.slot_for_color(empty_color)
        slot.sid = sid
        slot.token = sess["token"]
        slot.nickname = sess["nickname"]
        slot.profile_id = sess.get("profile_id")
        slot.connected = True
        sess["table_id"] = table.id
        join_room(table.id)
        socketio.emit("table_joined", {"table_id": table.id, "color": empty_color, "role": "player"}, room=sid)

        # Entrambi i posti occupati: la partita puo' iniziare.
        table.status = "playing"
        table.turn_start_ts = time.monotonic()
        socketio.emit("game_started", {}, room=table.id)
        broadcast_game(table)
        broadcast_lobby()
        return

    # Tavolo pieno o partita in corso: si entra come spettatore.
    table.spectators[sid] = sess["nickname"]
    sess["table_id"] = table.id
    join_room(table.id)
    socketio.emit("table_joined", {"table_id": table.id, "color": None, "role": "spectator"}, room=sid)
    broadcast_game(table)
    broadcast_lobby()


@socketio.on("leave_table")
def on_leave_table(_data):
    sid = request_sid()
    sess = get_session(sid)
    if not sess or not sess.get("table_id"):
        return
    table = get_table_or_none(sess["table_id"])
    sess["table_id"] = None
    if not table:
        return

    leave_room(table.id)

    if sid in table.spectators:
        table.spectators.pop(sid, None)
        broadcast_game(table)
        broadcast_lobby()
        return

    color = table.color_of_sid(sid)
    if color is None:
        return

    if table.status == "waiting":
        remove_table(table.id)
        broadcast_lobby()
        return

    if table.status == "playing":
        result = "0-1" if color == "white" else "1-0"
        finish_game(table, result, "Abbandono")
        return
    # finished: nulla da fare oltre a lasciare la room


# --------------------------------------------------------------------------
# Socket.IO: riconnessione
# --------------------------------------------------------------------------

@socketio.on("toggle_reconnect")
def on_toggle_reconnect(data):
    # La riconnessione richiede solo il token: nickname e profilo vengono
    # ripristinati dal posto (PlayerSlot) gia' memorizzato sul tavolo, mai
    # da un valore mandato dal client.
    sid = request_sid()
    data = data or {}
    token = data.get("token")
    if not token or not isinstance(token, str):
        socketio.emit("reconnect_failed", {}, room=sid)
        return

    found_table = None
    found_color = None
    for table in TABLES.values():
        color = table.color_of_token(token)
        if color:
            found_table = table
            found_color = color
            break

    if not found_table:
        socketio.emit("reconnect_failed", {}, room=sid)
        return

    slot = found_table.slot_for_color(found_color)
    slot.sid = sid
    slot.connected = True
    slot.disconnect_deadline = None

    SESSIONS[sid] = {
        "token": token,
        "nickname": slot.nickname,
        "profile_id": slot.profile_id,
        "table_id": found_table.id,
    }
    join_room(LOBBY_ROOM)
    join_room(found_table.id)

    socketio.emit("table_joined", {"table_id": found_table.id, "color": found_color, "role": "player"}, room=sid)
    if found_table.status == "playing":
        socketio.emit("player_reconnected", {"color": found_color}, room=found_table.id)
    broadcast_game(found_table)
    broadcast_lobby()


# --------------------------------------------------------------------------
# Socket.IO: mosse
# --------------------------------------------------------------------------

def handle_move_attempt(data):
    sid = request_sid()
    sess = get_session(sid)
    if not sess or not sess.get("table_id"):
        emit_error(sid, "Non sei in nessuna partita.")
        return
    table = get_table_or_none(sess["table_id"])
    if not table:
        emit_error(sid, "Tavolo non trovato.")
        return
    if table.status != "playing":
        socketio.emit("move_rejected", {"reason": "La partita non e' in corso."}, room=sid)
        return

    color = table.color_of_sid(sid)
    if color is None:
        socketio.emit("move_rejected", {"reason": "Gli spettatori non possono muovere."}, room=sid)
        return

    board = table.board
    is_white_turn = board.turn == chess.WHITE
    if (color == "white") != is_white_turn:
        socketio.emit("move_rejected", {"reason": "Non e' il tuo turno."}, room=sid)
        return

    data = data or {}
    from_str = data.get("from")
    to_str = data.get("to")
    promotion = data.get("promotion")

    try:
        from_sq = chess.parse_square(from_str)
        to_sq = chess.parse_square(to_str)
    except Exception:
        socketio.emit("move_rejected", {"reason": "Coordinate non valide."}, room=sid)
        return

    promo_piece = None
    if promotion is not None:
        if promotion not in PROMOTION_MAP:
            socketio.emit("move_rejected", {"reason": "Pezzo di promozione non valido."}, room=sid)
            return
        promo_piece = PROMOTION_MAP[promotion]

    # Rileva se serve una promozione ma non e' stata specificata
    piece = board.piece_at(from_sq)
    to_rank = chess.square_rank(to_sq)
    needs_promotion = (
        piece is not None
        and piece.piece_type == chess.PAWN
        and promo_piece is None
        and ((piece.color == chess.WHITE and to_rank == 7) or (piece.color == chess.BLACK and to_rank == 0))
    )
    if needs_promotion:
        candidate = chess.Move(from_sq, to_sq, promotion=chess.QUEEN)
        if candidate in board.legal_moves:
            socketio.emit("move_rejected", {"reason": "promotion_required", "from": from_str, "to": to_str}, room=sid)
            return

    move = chess.Move(from_sq, to_sq, promotion=promo_piece)
    if move not in board.legal_moves:
        socketio.emit("move_rejected", {"reason": "Mossa illegale."}, room=sid)
        return

    now_mono = time.monotonic()
    elapsed = now_mono - (table.turn_start_ts or now_mono)
    remaining_before = table.remaining_time(color, now_mono)
    if remaining_before <= 0:
        forfeit_on_time(table, color)
        return

    san = board.san(move)
    board.push(move)

    if color == "white":
        table.white_time = max(0.0, table.white_time - elapsed) + table.time_control.increment
    else:
        table.black_time = max(0.0, table.black_time - elapsed) + table.time_control.increment

    table.turn_start_ts = now_mono
    table.move_history_san.append(san)
    table.last_move = (from_str, to_str)
    table.draw_offer_by = None

    socketio.emit("move_accepted", {"san": san}, room=sid)

    end = evaluate_board_end(table)
    if end:
        result, reason = end
        finish_game(table, result, reason)
        return

    broadcast_game(table)


@socketio.on("attempt_move")
def on_attempt_move(data):
    handle_move_attempt(data)


@socketio.on("choose_promotion")
def on_choose_promotion(data):
    # Stessa logica di attempt_move: il payload include gia' 'promotion'.
    handle_move_attempt(data)


# --------------------------------------------------------------------------
# Socket.IO: resa, patta
# --------------------------------------------------------------------------

@socketio.on("resign_game")
def on_resign_game(_data):
    sid = request_sid()
    sess = get_session(sid)
    if not sess or not sess.get("table_id"):
        return
    table = get_table_or_none(sess["table_id"])
    if not table or table.status != "playing":
        return
    color = table.color_of_sid(sid)
    if color is None:
        emit_error(sid, "Gli spettatori non possono abbandonare la partita.")
        return
    result = "0-1" if color == "white" else "1-0"
    finish_game(table, result, "Abbandono")


@socketio.on("offer_draw")
def on_offer_draw(_data):
    sid = request_sid()
    sess = get_session(sid)
    if not sess or not sess.get("table_id"):
        return
    table = get_table_or_none(sess["table_id"])
    if not table or table.status != "playing":
        return
    color = table.color_of_sid(sid)
    if color is None:
        emit_error(sid, "Gli spettatori non possono offrire patta.")
        return
    if table.draw_offer_by is not None:
        return
    table.draw_offer_by = color
    opponent = table.black if color == "white" else table.white
    if opponent.sid:
        socketio.emit("draw_offered", {"from": color}, room=opponent.sid)
    broadcast_game(table)


@socketio.on("respond_draw")
def on_respond_draw(data):
    sid = request_sid()
    sess = get_session(sid)
    if not sess or not sess.get("table_id"):
        return
    table = get_table_or_none(sess["table_id"])
    if not table or table.status != "playing" or table.draw_offer_by is None:
        return
    color = table.color_of_sid(sid)
    if color is None or color == table.draw_offer_by:
        return

    accept = bool((data or {}).get("accept"))
    offering_color = table.draw_offer_by
    table.draw_offer_by = None
    if accept:
        finish_game(table, "1/2-1/2", "Patta per accordo")
    else:
        offering_slot = table.slot_for_color(offering_color)
        if offering_slot.sid:
            socketio.emit("draw_declined", {}, room=offering_slot.sid)
        broadcast_game(table)


# --------------------------------------------------------------------------
# Socket.IO: chat a frasi predefinite (nessun testo libero dal client)
# --------------------------------------------------------------------------

@socketio.on("send_reaction")
def on_send_reaction(data):
    sid = request_sid()
    sess = get_session(sid)
    if not sess or not sess.get("table_id"):
        return
    table = get_table_or_none(sess["table_id"])
    if not table or table.status != "playing":
        return
    color = table.color_of_sid(sid)
    if color is None:
        emit_error(sid, "Solo i giocatori possono inviare una reazione.")
        return

    # Anti-spam lato server: non fidarsi del solo cooldown del client.
    # Il timer e' legato al posto (PlayerSlot), quindi sopravvive anche
    # a una riconnessione con un sid diverso.
    slot = table.slot_for_color(color)
    now_mono = time.monotonic()
    if slot.last_reaction_ts is not None and (now_mono - slot.last_reaction_ts) < REACTION_COOLDOWN_SECONDS:
        wait = round(REACTION_COOLDOWN_SECONDS - (now_mono - slot.last_reaction_ts), 1)
        emit_error(sid, f"Aspetta ancora {wait}s prima di inviare un'altra reazione.")
        return

    data = data or {}
    emoji_key = data.get("emoji_key")
    if not isinstance(emoji_key, str) or emoji_key not in REACTIONS:
        emit_error(sid, "Reazione non valida.")
        return

    profile_id = sess.get("profile_id")
    phrases = PERSONA_PHRASES.get(profile_id, {}).get(emoji_key)
    if not phrases:
        emit_error(sid, "Reazione non disponibile per questo profilo.")
        return

    slot.last_reaction_ts = now_mono

    # La frase NON arriva mai dal client: viene sempre scelta qui, a
    # caso, dalla tabella fissa associata al profilo del mittente.
    phrase = random.choice(phrases)

    socketio.emit("reaction", {
        "color": color,
        "nickname": sess.get("nickname"),
        "avatar": PROFILES.get(profile_id, {}).get("avatar"),
        "emoji_key": emoji_key,
        "emoji": REACTIONS[emoji_key],
        "phrase": phrase,
    }, room=table.id)


@socketio.on("send_chat")
def on_send_chat(data):
    """Chat libera (testo scritto dall'utente), a differenza delle reazioni
    rapide che usano solo frasi predefinite. Vincoli stretti perche' non
    c'e' account ne' moderazione umana: solo giocatori, messaggio breve,
    niente caratteri di controllo, cooldown anti-spam per posto."""
    sid = request_sid()
    sess = get_session(sid)
    if not sess or not sess.get("table_id"):
        return
    table = get_table_or_none(sess["table_id"])
    if not table or table.status != "playing":
        return
    color = table.color_of_sid(sid)
    if color is None:
        emit_error(sid, "Solo i giocatori possono scrivere in chat.")
        return

    # Anti-spam lato server, legato al posto (PlayerSlot) cosi' da
    # sopravvivere anche a una riconnessione con un sid diverso.
    slot = table.slot_for_color(color)
    now_mono = time.monotonic()
    if slot.last_chat_ts is not None and (now_mono - slot.last_chat_ts) < CHAT_COOLDOWN_SECONDS:
        wait = round(CHAT_COOLDOWN_SECONDS - (now_mono - slot.last_chat_ts), 1)
        emit_error(sid, f"Aspetta ancora {wait}s prima di scrivere di nuovo.")
        return

    data = data or {}
    text = data.get("text")
    if not isinstance(text, str):
        emit_error(sid, "Messaggio non valido.")
        return

    text = text.strip()
    if not text:
        return
    text = text[:CHAT_MAX_LEN]
    if CHAT_CONTROL_CHAR_RE.search(text):
        emit_error(sid, "Messaggio non valido: niente a-capo o caratteri di controllo.")
        return

    slot.last_chat_ts = now_mono
    profile_id = sess.get("profile_id")

    socketio.emit("chat_message", {
        "color": color,
        "nickname": sess.get("nickname"),
        "avatar": PROFILES.get(profile_id, {}).get("avatar"),
        "text": text,
    }, room=table.id)


# --------------------------------------------------------------------------
# Avvio
# --------------------------------------------------------------------------

socketio.start_background_task(background_loop)

if __name__ == "__main__":
    log.info("Avvio server su %s:%s (debug=%s)", HOST, PORT, DEBUG)
    socketio.run(app, host=HOST, port=PORT, debug=DEBUG)
