import os
import re
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL non trovata nel file .env")

engine = create_engine(DATABASE_URL)

FILE_EXCEL = "VISITE 2026.xlsx"
ANNO_PLANNING = 2026


def pulisci_valore(valore):
    if pd.isna(valore):
        return ""
    return str(valore).strip()


def trova_colonna(df, parole_chiave):
    for col in df.columns:
        nome_col = str(col).strip().lower()
        for parola in parole_chiave:
            if parola in nome_col:
                return col
    return None


def trova_colonna_cliente(df):
    return trova_colonna(df, ["client"])


def trova_colonna_divisione(df):
    return trova_colonna(df, ["division"])


def trova_colonna_nazione(df):
    return trova_colonna(df, ["nazione", "nation", "country"])


def trova_colonna_referente(df):
    return trova_colonna(df, ["referente", "contact", "responsible"])


def estrai_numero_settimana(nome_colonna):
    match = re.match(r"wk\s*(\d+)", str(nome_colonna).strip().lower())
    if match:
        return int(match.group(1))
    return None


def estrai_tipi_visita(valore_cella):
    testo = pulisci_valore(valore_cella)
    if not testo:
        return []

    parti = re.split(r"\s*/\s*", testo)
    return [p.strip() for p in parti if p.strip()]


def upsert_cliente(conn, nome_cliente, divisione="", nazione="", referente=""):
    nome_cliente = pulisci_valore(nome_cliente)
    divisione = pulisci_valore(divisione)
    nazione = pulisci_valore(nazione)
    referente = pulisci_valore(referente)

    result = conn.execute(
        text("SELECT id FROM clienti WHERE TRIM(nome) = :nome"),
        {"nome": nome_cliente}
    ).fetchone()

    if result:
        cliente_id = result[0]
        conn.execute(
            text("""
                UPDATE clienti
                SET divisione = :divisione,
                    nazione = :nazione,
                    referente = :referente
                WHERE id = :cliente_id
            """),
            {
                "cliente_id": cliente_id,
                "divisione": divisione,
                "nazione": nazione,
                "referente": referente
            }
        )
        return cliente_id, False  # False = non creato, ma aggiornato

    result = conn.execute(
        text("""
            INSERT INTO clienti (nome, divisione, nazione, referente)
            VALUES (:nome, :divisione, :nazione, :referente)
            RETURNING id
        """),
        {
            "nome": nome_cliente,
            "divisione": divisione,
            "nazione": nazione,
            "referente": referente
        }
    ).fetchone()

    if result is None:
        raise Exception(f"Errore inserimento cliente: {nome_cliente}")

    return result[0], True  # True = creato


def visita_esiste(conn, cliente_id, anno, settimana, tipo):
    result = conn.execute(
        text("""
            SELECT id
            FROM visite
            WHERE cliente_id = :cliente_id
              AND anno = :anno
              AND settimana = :settimana
              AND tipo = :tipo
            LIMIT 1
        """),
        {
            "cliente_id": cliente_id,
            "anno": anno,
            "settimana": settimana,
            "tipo": tipo
        }
    ).fetchone()

    return result is not None


def importa_visite():
    if not os.path.exists(FILE_EXCEL):
        raise FileNotFoundError(f"File non trovato: {FILE_EXCEL}")

    df = pd.read_excel(FILE_EXCEL)

    col_cliente = trova_colonna_cliente(df)
    if not col_cliente:
        raise Exception("Colonna cliente non trovata nel file Excel")

    col_divisione = trova_colonna_divisione(df)
    col_nazione = trova_colonna_nazione(df)
    col_referente = trova_colonna_referente(df)

    colonne_settimana = []
    for col in df.columns:
        numero = estrai_numero_settimana(col)
        if numero is not None:
            colonne_settimana.append((col, numero))

    if not colonne_settimana:
        raise Exception("Nessuna colonna settimana trovata")

    colonne_settimana.sort(key=lambda x: x[1])

    visite_inserite = 0
    visite_saltate = 0
    clienti_senza_nome = 0
    clienti_creati = 0
    clienti_aggiornati = 0

    with engine.begin() as conn:
        for _, row in df.iterrows():
            nome_cliente = pulisci_valore(row[col_cliente])

            if not nome_cliente or nome_cliente.lower() == "nan":
                clienti_senza_nome += 1
                continue

            divisione = pulisci_valore(row[col_divisione]) if col_divisione else ""
            nazione = pulisci_valore(row[col_nazione]) if col_nazione else ""
            referente = pulisci_valore(row[col_referente]) if col_referente else ""

            cliente_id, creato = upsert_cliente(
                conn,
                nome_cliente,
                divisione,
                nazione,
                referente
            )

            if creato:
                clienti_creati += 1
            else:
                clienti_aggiornati += 1

            for nome_colonna, numero_settimana in colonne_settimana:
                valore = row[nome_colonna]

                if pd.isna(valore):
                    continue

                tipi_visita = estrai_tipi_visita(valore)
                if not tipi_visita:
                    continue

                settimana = f"wk {numero_settimana}"

                for tipo in tipi_visita:
                    if visita_esiste(conn, cliente_id, ANNO_PLANNING, settimana, tipo):
                        visite_saltate += 1
                        continue

                    conn.execute(
                        text("""
                            INSERT INTO visite (
                                cliente_id,
                                anno,
                                settimana,
                                tipo,
                                data_inserimento
                            )
                            VALUES (
                                :cliente_id,
                                :anno,
                                :settimana,
                                :tipo,
                                CURRENT_DATE
                            )
                        """),
                        {
                            "cliente_id": cliente_id,
                            "anno": ANNO_PLANNING,
                            "settimana": settimana,
                            "tipo": tipo
                        }
                    )
                    visite_inserite += 1

    print("Import completato")
    print(f"Clienti creati: {clienti_creati}")
    print(f"Clienti aggiornati: {clienti_aggiornati}")
    print(f"Visite inserite: {visite_inserite}")
    print(f"Visite saltate (già presenti): {visite_saltate}")
    print(f"Righe senza cliente: {clienti_senza_nome}")

    print("\nColonne rilevate:")
    print(f"Cliente: {col_cliente}")
    print(f"Divisione: {col_divisione}")
    print(f"Nazione: {col_nazione}")
    print(f"Referente: {col_referente}")
    print(f"Settimane trovate: {len(colonne_settimana)}")


if __name__ == "__main__":
    importa_visite()