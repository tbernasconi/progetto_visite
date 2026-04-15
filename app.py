from datetime import datetime, timedelta, date
from flask import Flask, render_template, request, send_file, redirect, url_for
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
import os
import re
import tempfile

app = Flask(__name__)

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")
engine = create_engine(DATABASE_URL) if DATABASE_URL else None



def get_cliente_id(nome_cliente):
    nome_cliente = str(nome_cliente).strip()

    assert engine is not None
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT id FROM clienti WHERE TRIM(nome) = :nome"),
            {"nome": nome_cliente}
        ).fetchone()

        if result:
            return result[0]

        result = conn.execute(
            text("""
                INSERT INTO clienti (nome)
                VALUES (:nome)
                RETURNING id
            """),
            {"nome": nome_cliente}
        ).fetchone()

        conn.commit()

        if result is None:
            raise Exception("Errore inserimento cliente")

        return result[0]

def salva_o_aggiorna_cliente(nome, divisione="", nazione="", referente=""):
    nome = str(nome).strip()
    divisione = str(divisione).strip()
    nazione = str(nazione).strip()
    referente = str(referente).strip()

    if not nome:
        raise ValueError("Il nome cliente è obbligatorio")

    assert engine is not None
    with engine.connect() as conn:
        esistente = conn.execute(
            text("""
                SELECT id
                FROM clienti
                WHERE TRIM(nome) = :nome
            """),
            {"nome": nome}
        ).fetchone()

        if esistente:
            conn.execute(
                text("""
                    UPDATE clienti
                    SET divisione = :divisione,
                        nazione = :nazione,
                        referente = :referente
                    WHERE id = :cliente_id
                """),
                {
                    "cliente_id": esistente[0],
                    "divisione": divisione,
                    "nazione": nazione,
                    "referente": referente
                }
            )
            conn.commit()
            return esistente[0]

        nuovo = conn.execute(
            text("""
                INSERT INTO clienti (nome, divisione, nazione, referente)
                VALUES (:nome, :divisione, :nazione, :referente)
                RETURNING id
            """),
            {
                "nome": nome,
                "divisione": divisione,
                "nazione": nazione,
                "referente": referente
            }
        ).fetchone()

        conn.commit()

        if nuovo is None:
            raise Exception("Errore inserimento cliente")

        return nuovo[0]

def salva_configurazione(cliente, valore, unita):
    cliente_id = get_cliente_id(cliente)

    assert engine is not None
    with engine.connect() as conn:
        # Verifica se esiste già
        existing = conn.execute(
            text("""
                SELECT id FROM configurazioni_visita
                WHERE cliente_id = :cliente_id
            """),
            {"cliente_id": cliente_id}
        ).fetchone()

        if existing:
            conn.execute(
                text("""
                    UPDATE configurazioni_visita
                    SET frequenza_valore = :valore,
                        frequenza_unita = :unita
                    WHERE cliente_id = :cliente_id
                """),
                {
                    "valore": valore,
                    "unita": unita,
                    "cliente_id": cliente_id
                }
            )
        else:
            conn.execute(
                text("""
                    INSERT INTO configurazioni_visita
                    (cliente_id, frequenza_valore, frequenza_unita)
                    VALUES (:cliente_id, :valore, :unita)
                """),
                {
                    "cliente_id": cliente_id,
                    "valore": valore,
                    "unita": unita
                }
            )

        conn.commit()

     

def test_connessione_db():
    if not DATABASE_URL:
        return "DATABASE_URL non trovata"

    try:
        assert engine is not None
        with engine.connect() as conn:
            risultato = conn.execute(text("SELECT 1"))
            return f"Connessione OK: {risultato.scalar()}"
    except Exception as e:
        return f"Errore connessione DB: {e}"

def estrai_numero_settimana(nome_colonna):
    match = re.match(r"wk\s*(\d+)", str(nome_colonna).strip().lower())
    if match:
        return int(match.group(1))
    return None

def colonne_settimane(df):
    cols = []
    for col in df.columns:
        num = estrai_numero_settimana(col)
        if num is not None:
            cols.append((col, num))
    return sorted(cols, key=lambda x: x[1])

def data_da_settimana(anno, numero_settimana):
    return date.fromisocalendar(anno, numero_settimana, 1)

def calcola_prossima_visita(data_ultima_visita, frequenza):
    valore = frequenza.get("frequenza_valore")
    unita = frequenza.get("frequenza_unita")

    if not valore or not unita:
        return None

    if unita == "settimane":
        return data_ultima_visita + timedelta(weeks=valore)

    if unita == "mesi":
        return data_ultima_visita + timedelta(days=valore * 30)

    return None


def carica_bozze_db():
    assert engine is not None
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT b.id, c.nome, b.settimana, b.tipo, b.data_inserimento
            FROM bozze_visite b
            JOIN clienti c ON c.id = b.cliente_id
            ORDER BY b.id
        """))

        return [
            {
                "id": row[0],
                "cliente": row[1],
                "settimana": row[2],
                "tipo": row[3],
                "data_inserimento": row[4].strftime("%Y-%m-%d") if row[4] else ""
            }
            for row in result
        ]


def salva_bozza_db(cliente, tipo, settimana, data_inserimento):
    cliente_id = get_cliente_id(cliente)

    assert engine is not None
    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO bozze_visite (cliente_id, settimana, tipo, data_inserimento)
                VALUES (:cliente_id, :settimana, :tipo, :data_inserimento)
            """),
            {
                "cliente_id": cliente_id,
                "settimana": settimana,
                "tipo": tipo,
                "data_inserimento": data_inserimento
            }
        )
        conn.commit()


def aggiorna_bozza_db(bozza_id, cliente, tipo, settimana, data_inserimento):
    cliente_id = get_cliente_id(cliente)

    assert engine is not None
    with engine.connect() as conn:
        conn.execute(
            text("""
                UPDATE bozze_visite
                SET cliente_id = :cliente_id,
                    settimana = :settimana,
                    tipo = :tipo,
                    data_inserimento = :data_inserimento
                WHERE id = :bozza_id
            """),
            {
                "bozza_id": bozza_id,
                "cliente_id": cliente_id,
                "settimana": settimana,
                "tipo": tipo,
                "data_inserimento": data_inserimento
            }
        )
        conn.commit()

@app.route("/")
def index():
    clienti = get_clienti_db()
    settimana_corrente = f"wk {datetime.now().isocalendar().week}"

    bozze_visite = carica_bozze_db()

    modifica_index = request.args.get("modifica")
    bozza_in_modifica = None

    if modifica_index is not None:
        modifica_index = int(modifica_index)
        bozza_in_modifica = get_bozza_by_id(modifica_index)
    else:
        modifica_index = None

    return render_template(
        "index.html",
        clienti=clienti,
        settimana_corrente=settimana_corrente,
        bozze_visite=bozze_visite,
        bozza_in_modifica=bozza_in_modifica,
        modifica_index=modifica_index
    )

def get_bozza_by_id(bozza_id):
    
    assert engine is not None
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT b.id, c.nome, b.settimana, b.tipo, b.data_inserimento
                FROM bozze_visite b
                JOIN clienti c ON c.id = b.cliente_id
                WHERE b.id = :bozza_id
            """),
            {"bozza_id": bozza_id}
        ).fetchone()

        if not row:
            return None

        return {
            "id": row[0],
            "cliente": row[1],
            "settimana": row[2],
            "tipo": row[3],
            "data_inserimento": row[4].strftime("%Y-%m-%d") if row[4] else ""
        }


def svuota_bozze_db():
    
    assert engine is not None
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM bozze_visite"))
        conn.commit()

def salva_visita_db(cliente, settimana, tipo, data_inserimento):
    cliente_id = get_cliente_id(cliente)

    match = re.match(r"wk\s*(\d+)", str(settimana).strip().lower())
    if not match:
        return

    numero_settimana = int(match.group(1))
    anno = datetime.now().year
    settimana_db = f"wk {numero_settimana}"

    assert engine is not None
    with engine.connect() as conn:
        esistente = conn.execute(
            text("""
                SELECT id
                FROM visite
                WHERE cliente_id = :cliente_id
                  AND anno = :anno
                  AND settimana = :settimana
                  AND COALESCE(tipo, '') = COALESCE(:tipo, '')
                LIMIT 1
            """),
            {
                "cliente_id": cliente_id,
                "anno": anno,
                "settimana": settimana_db,
                "tipo": tipo
            }
        ).fetchone()

        if esistente:
            return

        conn.execute(
            text("""
                INSERT INTO visite (cliente_id, anno, settimana, tipo, data_inserimento)
                VALUES (:cliente_id, :anno, :settimana, :tipo, :data_inserimento)
            """),
            {
                "cliente_id": cliente_id,
                "anno": anno,
                "settimana": settimana_db,
                "tipo": tipo,
                "data_inserimento": data_inserimento
            }
        )
        conn.commit()

def get_configurazioni_db():
    assert engine is not None
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT c.nome, cfg.frequenza_valore, cfg.frequenza_unita
            FROM configurazioni_visita cfg
            JOIN clienti c ON c.id = cfg.cliente_id
            ORDER BY c.nome
        """))

        return [
            {
                "cliente": row[0],
                "frequenza_valore": row[1],
                "frequenza_unita": row[2]
            }
            for row in result
        ]

def get_clienti_db():
    assert engine is not None
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT nome FROM clienti ORDER BY nome")
        )
        return [row[0].strip() for row in result]

def get_clienti_anagrafica_db():
    assert engine is not None
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT nome, divisione, nazione, referente
            FROM clienti
            ORDER BY nome
        """))

        return [
            {
                "nome": row[0].strip() if row[0] else "",
                "divisione": row[1] or "",
                "nazione": row[2] or "",
                "referente": row[3] or ""
            }
            for row in result
        ]

def get_clienti_configurati_db():
    config = get_configurazioni_db()
    return [c["cliente"] for c in config]

def get_config_cliente_db(cliente):
    cliente = str(cliente).strip()

    assert engine is not None
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT cfg.frequenza_valore, cfg.frequenza_unita
                FROM configurazioni_visita cfg
                JOIN clienti c ON c.id = cfg.cliente_id
                WHERE TRIM(c.nome) = :cliente
            """),
            {"cliente": cliente}
        ).fetchone()

        if not row:
            return None

        return {
            "frequenza_valore": row[0],
            "frequenza_unita": row[1]
        }

def get_ultima_visita_db(cliente):
    cliente = str(cliente).strip()

    assert engine is not None
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT v.anno, v.settimana, v.tipo, v.data_inserimento
                FROM visite v
                JOIN clienti c ON c.id = v.cliente_id
                WHERE TRIM(c.nome) = :cliente
                ORDER BY
                    v.anno DESC,
                    CAST(REPLACE(LOWER(v.settimana), 'wk ', '') AS INTEGER) DESC,
                    v.id DESC
                LIMIT 1
        """),
            {"cliente": cliente}
        ).fetchone()


        if not row:
            return None

        match = re.match(r"wk\s*(\d+)", str(row[1]).strip().lower())
        if not match:
            return None

        numero_settimana = int(match.group(1))

        return {
            "anno": row[0],
            "settimana_colonna": row[1],
            "numero_settimana": numero_settimana,
            "valore": row[2],
            "data_visita": data_da_settimana(row[0], numero_settimana)
        }

def clienti_da_programmare_db():
    
    oggi = datetime.today()
    settimana_corrente = oggi.isocalendar()[1]

    settimane_target = {
        settimana_corrente: [],
        settimana_corrente + 1: [],
        settimana_corrente + 2: [],
        settimana_corrente + 3: []
    }

    mai_visitati = []

    for cliente in get_clienti_configurati_db():
        config = get_config_cliente_db(cliente)
        if not config:
            continue

        cliente_info = next(
                (c for c in get_clienti_anagrafica_db() if c["nome"] == cliente),
            {}
        )

        referente = cliente_info.get("referente", "")

        ultima_visita = get_ultima_visita_db(cliente)

        if not ultima_visita:
            record = {
                "cliente": cliente,
                "frequenza": f'{config["frequenza_valore"]} {config["frequenza_unita"]}'
            }
            mai_visitati.append(record)
            continue

        prossima_data = calcola_prossima_visita(
            ultima_visita["data_visita"],
            config
        )

        if prossima_data:
            settimana_prossima = prossima_data.isocalendar()[1]

            if settimana_prossima in settimane_target:
                record = {
                    "cliente": cliente,
                    "referente": referente,
                    "settimana_numero": settimana_prossima,
                    "ultima_visita": f'{ultima_visita["valore"]} ({ultima_visita["settimana_colonna"]})',
                    "tipo_ultima_visita": ultima_visita["valore"],
                    "data_ultima_visita": ultima_visita["data_visita"].strftime("%d/%m/%Y"),
                    "prossima_visita": prossima_data.strftime("%d/%m/%Y"),
                    "frequenza": f'{config["frequenza_valore"]} {config["frequenza_unita"]}'
                }
                settimane_target[settimana_prossima].append(record)

    return settimane_target, mai_visitati

def get_visite_db():
    assert engine is not None
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT c.nome, v.anno, v.settimana, v.tipo
            FROM visite v
            JOIN clienti c ON c.id = v.cliente_id
            ORDER BY c.nome, v.anno, v.id
        """))

        return [
            {
                "cliente": row[0].strip(),
                "anno": row[1],
                "settimana": row[2],
                "tipo": row[3]
            }
            for row in result
        ]

@app.route("/salva", methods=["POST"])
def salva():
    cliente_scelto = request.form["cliente"]
    tipo = request.form["tipo"]
    settimana = request.form["settimana"]
    modifica_index = request.form.get("modifica_index")

    data_inserimento = datetime.now().strftime("%Y-%m-%d")

    if modifica_index not in (None, ""):
        aggiorna_bozza_db(
            int(modifica_index),
            cliente_scelto,
            tipo,
            settimana,
            data_inserimento
        )
    else:
        salva_bozza_db(
            cliente_scelto,
            tipo,
            settimana,
            data_inserimento
        )

    return redirect(url_for("index", step=5))

@app.route("/scarica")
def scarica():
    bozze_visite = carica_bozze_db()

    for bozza in bozze_visite:
        salva_visita_db(
            bozza["cliente"],
            bozza["settimana"],
            bozza["tipo"],
            bozza["data_inserimento"]
        )

    svuota_bozze_db()

    clienti = [
        c for c in get_clienti_anagrafica_db()
        if str(c.get("nome", "")).strip().upper() != "TOTALE"
    ]

    visite = [
        v for v in get_visite_db()
        if str(v.get("cliente", "")).strip().upper() != "TOTALE"
    ]

    settimane = [f"wk {i}" for i in range(1, 54)]

    righe = []
    for cliente in clienti:
        riga = {
            "Cliente": cliente["nome"],
            "Divisione": cliente["divisione"],
            "Nazione": cliente["nazione"],
            "Referente": cliente["referente"]
        }

        for settimana in settimane:
            riga[settimana] = ""

        visite_cliente = [v for v in visite if v["cliente"] == cliente["nome"]]

        for visita in visite_cliente:
            settimana = visita["settimana"]
            tipo = visita["tipo"]

            if settimana in riga:
                if not riga[settimana]:
                    riga[settimana] = tipo
                else:
                    riga[settimana] = f'{riga[settimana]} / {tipo}'

        righe.append(riga)

    riga_totale = {
        "Cliente": "TOTALE",
        "Divisione": "",
        "Nazione": "",
        "Referente": ""
    }

    riga_totale_luca = {
        "Cliente": "TOTALE LUCA",
        "Divisione": "",
        "Nazione": "",
        "Referente": "Luca"
    }

    riga_totale_simone = {
        "Cliente": "TOTALE SIMONE",
        "Divisione": "",
        "Nazione": "",
        "Referente": "Simone"
    }

    for settimana in settimane:
        totale_luca = 0
        totale_simone = 0

        for riga in righe:
            valore = str(riga.get(settimana, "")).strip()
            referente = str(riga.get("Referente", "")).strip().lower()

            if valore:
                if referente == "luca":
                    totale_luca += 1
                elif referente == "simone":
                    totale_simone += 1

        riga_totale_luca[settimana] = str(totale_luca)
        riga_totale_simone[settimana] = str(totale_simone)

    for settimana in settimane:
        totale_settimana = 0

        for riga in righe:
            valore = str(riga.get(settimana, "")).strip()
            if valore:
                totale_settimana += 1

        riga_totale[settimana] = str(totale_settimana)

    righe.append(riga_totale)
    righe.append(riga_totale_luca)
    righe.append(riga_totale_simone)

    df = pd.DataFrame(righe)

    nome_file = f"visite_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    percorso = os.path.join(tempfile.gettempdir(), nome_file)
    df.to_excel(percorso, index=False)

    return send_file(percorso, as_attachment=True, download_name=nome_file)

@app.route("/planning")
def planning():
    settimane_target, mai_visitati = clienti_da_programmare_db()
    settimana_corrente = datetime.today().isocalendar()[1]

    return render_template(
        "planning.html",
        questa_settimana=settimane_target.get(settimana_corrente, []),
        prossima_settimana=settimane_target.get(settimana_corrente + 1, []),
        tra_due_settimane=settimane_target.get(settimana_corrente + 2, []),
        tra_tre_settimane=settimane_target.get(settimana_corrente + 3, []),
        numero_settimana_corrente=settimana_corrente,
        numero_settimana_prossima=settimana_corrente + 1,
        numero_settimana_due=settimana_corrente + 2,
        numero_settimana_tre=settimana_corrente + 3,
        mai_visitati=mai_visitati
    )

@app.route("/test-db")
def test_db():
    return test_connessione_db()

@app.route("/configura", methods=["GET", "POST"])
def configura():
    clienti = get_clienti_db()

    if request.method == "POST":
        nuovo_cliente = request.form.get("nuovo_cliente", "").strip()
        divisione = request.form.get("divisione", "").strip()
        nazione = request.form.get("nazione", "").strip()
        referente = request.form.get("referente", "").strip()

        valore = int(request.form["valore"])
        unita = request.form["unita"]

        if nuovo_cliente:
            cliente = nuovo_cliente
            salva_o_aggiorna_cliente(
                nome=nuovo_cliente,
                divisione=divisione,
                nazione=nazione,
                referente=referente
            )
        else:
            cliente = request.form["cliente"]

        salva_configurazione(cliente, valore, unita)

        return redirect(url_for("configura"))

    config_db = get_configurazioni_db()

    return render_template(
        "configura.html",
        clienti=clienti,
        clienti_config=config_db
    )

if __name__ == "__main__":
    app.run(debug=True)