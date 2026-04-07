import os
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL non trovata nel file .env")

engine = create_engine(DATABASE_URL)

FILE_JSON = "clienti_config.json"


def get_cliente_id(conn, nome_cliente):
    result = conn.execute(
        text("SELECT id FROM clienti WHERE nome = :nome"),
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

    return result[0]


def importa_configurazioni():
    if not os.path.exists(FILE_JSON):
        raise FileNotFoundError(f"File non trovato: {FILE_JSON}")

    with open(FILE_JSON, "r", encoding="utf-8") as f:
        dati = json.load(f)

    count_insert = 0
    count_update = 0

    with engine.begin() as conn:
        for riga in dati:
            cliente = str(riga.get("cliente", "")).strip()
            valore = riga.get("frequenza_valore")
            unita = riga.get("frequenza_unita")

            if not cliente or valore is None or not unita:
                continue

            cliente_id = get_cliente_id(conn, cliente)

            existing = conn.execute(
                text("""
                    SELECT id
                    FROM configurazioni_visita
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
                        "valore": int(valore),
                        "unita": str(unita).strip(),
                        "cliente_id": cliente_id
                    }
                )
                count_update += 1
            else:
                conn.execute(
                    text("""
                        INSERT INTO configurazioni_visita
                        (cliente_id, frequenza_valore, frequenza_unita)
                        VALUES (:cliente_id, :valore, :unita)
                    """),
                    {
                        "cliente_id": cliente_id,
                        "valore": int(valore),
                        "unita": str(unita).strip()
                    }
                )
                count_insert += 1

    print(f"Import completato. Inserite: {count_insert}, aggiornate: {count_update}")


if __name__ == "__main__":
    importa_configurazioni()