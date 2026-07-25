# Scacchi Online - MVP multiplayer senza account

Web app minimale per giocare a scacchi in tempo reale in una lobby pubblica,
senza registrazione, password, email, profili, ranking o database. Tutto lo
stato vive in memoria RAM sul server e viene validato con `python-chess`.

## Struttura

```
.
├── server.py            # Backend Flask + Flask-SocketIO (unica fonte di verita')
├── templates/
│   └── index.html       # Frontend: HTML + CSS + JS vanilla, un solo file
├── requirements.txt
├── .env.example
└── README.md
```

## Requisiti

- Python 3.11 o superiore
- pip

## Installazione

```bash
python3 -m venv venv
source venv/bin/activate          # Su Windows: venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env              # facoltativo, i valori di default funzionano
```

## Avvio in locale

```bash
python server.py
```

Il server parte su `http://localhost:5000` (porta configurabile in `.env`
tramite `PORT`). Apri quell'indirizzo in due schede/browser diversi (o due
dispositivi sulla stessa rete) per simulare due giocatori.

## Note per l'hosting

- L'app usa `eventlet` come worker asincrono per Flask-SocketIO: qualsiasi
  host Python compatibile con processi long-running e WebSocket va bene
  (es. un semplice VPS, Render, Railway, Fly.io, un container Docker fatto
  in casa, ecc.). Non serve Docker per l'MVP, ma puoi containerizzarlo se
  vuoi: basta `pip install -r requirements.txt` e `python server.py`.
- Lo stato è **solo in RAM**: se il processo viene riavviato, tutte le
  lobby/partite in corso vengono perse. E' il comportamento atteso per
  questo MVP (nessun database).
- Se metti l'app dietro un reverse proxy (nginx, Caddy, ecc.), assicurati
  che gli upgrade WebSocket vengano inoltrati correttamente.
- In produzione imposta una `SECRET_KEY` robusta nel file `.env`.

## Cosa NON fa (volutamente)

- Nessuna registrazione, login, password o email.
- Nessun database persistente: nickname e tavoli sono temporanei.
- Nessuna chat, nessun ranking, nessun salvataggio PGN, nessun motore di analisi.
- Nessun dato personale salvato (niente IP, niente cronologia).

## Checklist di test manuale

Esegui questi test aprendo l'app in più schede/finestre del browser
(o dispositivi diversi sulla stessa rete) per simulare più utenti.

1. **Creazione tavolo** — Entra con un nickname, crea un tavolo Rapid 15+0
   con colore "Casuale". Verifica che il tavolo appaia nella lobby con stato
   "In attesa" e che tu risulti già seduto a un colore.
2. **Ingresso avversario** — Da una seconda scheda/nickname, clicca "Entra"
   sul tavolo creato. Verifica che lo stato passi a "In corso", che
   compaiano entrambi i nickname e che parta l'orologio del Bianco.
3. **Spettatore** — Da una terza scheda, entra sullo stesso tavolo (ora "In
   corso"): deve entrare come spettatore, vedere scacchiera/mosse/orologi in
   tempo reale, e NON deve poter muovere pezzi né vedere pulsanti di
   abbandono/patta.
4. **Mosse illegali** — Da un giocatore, prova a muovere un pezzo su una
   casa non raggiungibile, o a muovere un pezzo dell'avversario, o a
   muovere fuori turno: la mossa deve essere rifiutata con un messaggio,
   senza alterare la scacchiera.
5. **Mostra mosse legali** — Attiva il toggle "Mostra mosse legali", clicca
   un tuo pezzo: devono comparire solo i pallini sulle destinazioni
   effettivamente legali per quel pezzo (in base al turno).
6. **Arrocco** — Porta una partita in una posizione con arrocco disponibile
   (es. libera le case tra Re e Torre senza muoverli prima) ed esegui
   l'arrocco corto e/o lungo: entrambi i pezzi devono spostarsi
   correttamente e comparire in notazione SAN (O-O / O-O-O) nell'elenco mosse.
7. **En passant** — Avanza un pedone di due case accanto a un pedone
   avversario nella posizione corretta e cattura en passant al turno
   immediatamente successivo: la cattura deve essere accettata e il pedone
   catturato deve sparire dalla casa corretta.
8. **Promozione** — Porta un pedone in ottava/prima traversa: deve apparire
   la modale di scelta (Donna, Torre, Alfiere, Cavallo); seleziona un pezzo
   diverso dalla Donna e verifica che il pezzo promosso sia quello scelto.
9. **Patta per accordo** — Un giocatore preme "Offri patta": l'avversario
   deve ricevere una modale con Accetta/Rifiuta. Testa entrambi i percorsi
   (accetta -> partita termina in patta; rifiuta -> il proponente riceve
   notifica di rifiuto e la partita continua).
10. **Resa** — Un giocatore preme "Abbandona", conferma nella modale:
    la partita deve terminare immediatamente con vittoria dell'avversario
    e motivazione "Abbandono" visibile a entrambi e agli spettatori.
11. **Timeout (scacco per tempo)** — Crea un tavolo Blitz 5+30, lascia
    scorrere l'orologio di un giocatore fino a zero (o riduci temporaneamente
    i tempi nel codice per test rapidi): la partita deve terminare con
    vittoria dell'avversario e motivazione "Tempo scaduto", sincronizzata
    per tutti i client.
12. **Disconnessione/riconnessione** — Durante una partita in corso, chiudi
    la scheda di un giocatore (o disabilita la rete) senza cliccare
    "Abbandona": gli altri devono vedere "Giocatore disconnesso: rientro
    entro 60 secondi". Riapri la pagina entro 60 secondi dallo stesso
    browser (stesso `sessionStorage`): il giocatore deve rientrare nello
    stesso ruolo/partita. Ripeti superando i 60 secondi: l'avversario deve
    vincere per disconnessione.
13. **Mobile / responsive** — Apri l'app da uno smartphone (o riduci la
    finestra del browser a ~360px): la scacchiera deve occupare quasi tutta
    la larghezza senza overflow orizzontale, i pulsanti devono essere
    facilmente toccabili, e il pannello mosse deve restare leggibile sotto
    la scacchiera.
14. **Pulizia lobby** — Termina una partita (scacco matto, patta o
    abbandono) e verifica che il tavolo mostri "Terminata" e sparisca dalla
    lobby dopo circa 60 secondi. Crea un tavolo e abbandonalo prima che
    entri un avversario: il tavolo deve sparire immediatamente dalla lobby.
