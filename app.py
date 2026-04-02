from datetime import datetime, timedelta, date
from flask import Flask, render_template, request, send_file, redirect, url_for
import pandas as pd
import os
import json
import re

app = Flask(__name__)

FILE_BOZZE = "bozze_visite.json"
bozze_visite = []

FILE_CLIENTI = "clienti_config.json"
clienti_config = []
FILE_EXCEL = "VISITE 2026.xlsx"


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

def settimana_iso(data):
    return data.isocalendar().week

def trova_ultima_visita_excel(df, nome_cliente, anno_planning):
    col_cliente = trova_colonna_cliente(df)
    if not col_cliente:
        return None

    righe_cliente = df[
        df[col_cliente].astype(str).str.strip().str.upper()
        == str(nome_cliente).strip().upper()
    ]
    if righe_cliente.empty:
        return None

    settimane = colonne_settimane(df)

    ultima_visita = None

    for _, riga in righe_cliente.iterrows():
        for nome_colonna, num_settimana in settimane:
            valore = riga[nome_colonna]

            if pd.notna(valore) and str(valore).strip() != "":
                ultima_visita = {
                    "settimana_colonna": nome_colonna,
                    "numero_settimana": num_settimana,
                    "valore": str(valore).strip(),
                    "data_visita": data_da_settimana(anno_planning, num_settimana)
                }

    return ultima_visita

def calcola_prossima_visita(data_ultima_visita, frequenza):
    valore = frequenza.get("valore")
    unita = frequenza.get("unita")

    if not valore or not unita:
        return None

    if unita == "settimane":
        return data_ultima_visita + timedelta(weeks=valore)

    if unita == "mesi":
        return data_ultima_visita + timedelta(days=valore * 30)

    return None

def clienti_da_programmare():
    df = carica_file()
    anno_planning = 2026

    oggi = date.today()
    anno_attuale, settimana_attuale, _ = oggi.isocalendar()
    settimana_prossima = settimana_attuale + 1

    questa_settimana = []
    prossima_settimana = []

    for config in clienti_config:
        cliente = config["cliente"]
        ultima_visita = trova_ultima_visita_excel(df, cliente, anno_planning)

        if not ultima_visita:
            continue

        frequenza = {
            "valore": config.get("frequenza_valore"),
            "unita": config.get("frequenza_unita")
        }
        prossima_data = calcola_prossima_visita(ultima_visita["data_visita"], frequenza)

        if not prossima_data:
            continue

        anno_prossima, settimana_prossima_visita, _ = prossima_data.isocalendar()

        record = {
            "cliente": cliente,
            "ultima_visita": ultima_visita["settimana_colonna"],
            "tipo_ultima_visita": ultima_visita["valore"],
            "data_ultima_visita": ultima_visita["data_visita"].strftime("%d/%m/%Y"),
            "prossima_visita": prossima_data.strftime("%d/%m/%Y"),
            "frequenza": f'{config["frequenza_valore"]} {config["frequenza_unita"]}'
        }

        if anno_prossima == anno_attuale and settimana_prossima_visita == settimana_attuale:
            questa_settimana.append(record)
        elif anno_prossima == anno_attuale and settimana_prossima_visita == settimana_prossima:
            prossima_settimana.append(record)

    return questa_settimana, prossima_settimana

def carica_bozze():
    global bozze_visite

    if os.path.exists(FILE_BOZZE):
        with open(FILE_BOZZE, "r", encoding="utf-8") as f:
            bozze_visite = json.load(f)
    else:
        bozze_visite = []

carica_bozze()

def salva_bozze():
    with open(FILE_BOZZE, "w", encoding="utf-8") as f:
        json.dump(bozze_visite, f, ensure_ascii=False, indent=2)

def carica_clienti():
    global clienti_config

    if os.path.exists(FILE_CLIENTI):
        with open(FILE_CLIENTI, "r", encoding="utf-8") as f:
            clienti_config = json.load(f)
    else:
        clienti_config = []

carica_clienti()



def salva_clienti():
    with open(FILE_CLIENTI, "w", encoding="utf-8") as f:
        json.dump(clienti_config, f, ensure_ascii=False, indent=2)




def carica_file():
    return pd.read_excel(FILE_EXCEL)

def trova_colonna_cliente(df):
    for col in df.columns:
        if "client" in col.lower():
            return col
    return None


def trova_settimane(df):
    return [col for col in df.columns if "wk" in col.lower()]


def trova_settimana_corrente(settimane):
    numero_settimana = datetime.now().isocalendar().week
    nome_settimana = f"wk {numero_settimana}"
    if nome_settimana in settimane:
        return nome_settimana
    return None


@app.route("/")
def index():
    df = carica_file()
    col_cliente = trova_colonna_cliente(df)
    settimane = trova_settimane(df)
    settimana_corrente = trova_settimana_corrente(settimane)

    clienti = df[col_cliente].dropna().astype(str).tolist()

    modifica_index = request.args.get("modifica")
    bozza_in_modifica = None

    if modifica_index is not None:
        modifica_index = int(modifica_index)
        if 0 <= modifica_index < len(bozze_visite):
            bozza_in_modifica = bozze_visite[modifica_index]
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

@app.route("/salva", methods=["POST"])
def salva():
    cliente_scelto = request.form["cliente"]
    tipo = request.form["tipo"]
    settimana = request.form["settimana"]
    modifica_index = request.form.get("modifica_index")

    nuova_bozza = {
        "cliente": cliente_scelto,
        "tipo": tipo,
        "settimana": settimana,
        "data_inserimento":datetime.now().strftime("%Y-%m-%d")
    }

    if modifica_index not in (None, ""):
        modifica_index = int(modifica_index)
        if 0 <= modifica_index < len(bozze_visite):
            bozze_visite[modifica_index] = nuova_bozza
    else:
        bozze_visite.append(nuova_bozza)
    salva_bozze()   
    return redirect(url_for("index", step=5))

@app.route("/scarica")
def scarica():
    df = pd.read_excel(FILE_EXCEL)
    col_cliente = trova_colonna_cliente(df)

    for bozza in bozze_visite:
        cliente_scelto = bozza["cliente"]
        tipo = bozza["tipo"]
        settimana = bozza["settimana"]

        indice_lista = df[
            df[col_cliente].astype(str).str.strip().str.upper()
            == str(cliente_scelto).strip().upper()
        ].index 

        if len(indice_lista) == 0:
            continue

        indice = indice_lista[0]

        if settimana not in df.columns:
            continue

        df[settimana] = df[settimana].astype("object")
        valore_attuale = df.loc[indice, settimana]

        if pd.isna(valore_attuale) or valore_attuale == "":
            nuovo_valore = tipo
        else:
            nuovo_valore = f"{valore_attuale} / {tipo}"

        df.loc[indice, settimana] = nuovo_valore

    
    df.to_excel(FILE_EXCEL, index=False)


    
# 👉 Svuota bozze
    bozze_visite.clear()
    salva_bozze()
    # 👉 Prepara download
    return send_file(FILE_EXCEL, as_attachment=True)



@app.route("/planning")
def planning():
    questa_settimana, prossima_settimana = clienti_da_programmare()

    return render_template(
        "planning.html",
        questa_settimana=questa_settimana,
        prossima_settimana=prossima_settimana
    )

@app.route("/configura", methods=["GET", "POST"])
def configura():
    df = carica_file()
    col_cliente = trova_colonna_cliente(df)
    clienti = df[col_cliente].dropna().astype(str).tolist()

    if request.method == "POST":
        cliente = request.form["cliente"]
        valore = int(request.form["valore"])
        unita = request.form["unita"]

        trovato = False
        for c in clienti_config:
            if c["cliente"] == cliente:
                c["frequenza_valore"] = valore
                c["frequenza_unita"] = unita
                trovato = True
                break

        if not trovato:
            clienti_config.append({
                "cliente": cliente,
                "frequenza_valore": valore,
                "frequenza_unita": unita
            })

        salva_clienti()

        return redirect(url_for("configura"))

    return render_template("configura.html", clienti=clienti, clienti_config=clienti_config)

if __name__ == "__main__":
    app.run(debug=True)