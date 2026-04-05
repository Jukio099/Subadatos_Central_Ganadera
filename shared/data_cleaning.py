import re


FERIA_CENTRAL = "Central Ganadera"
FERIA_CASANARE = "Casanare"


_CENTRAL_EQUIVALENCIAS = {
    "ENTRA DE FLACO": "Entrada De Flaco",
    "ENTRADAD DE FLACO": "Entrada De Flaco",
    "ENTRADA DE FALCO": "Entrada De Flaco",
    "ENTRADA DE GANDO FLACO": "Entrada De Flaco",
    "ENTRA DE FERIA": "Entrada De Feria",
    "ENTRADA D EFERIA": "Entrada De Feria",
    "ENTRADA DE GANADO": "Entrada De Feria",
    "SANTA BARBARA": "Santa Barbara",
    "SABANA LARGA": "Sabanalarga",
    "CAUCASI": "Caucasia",
    "SAN PEDRO DE LOS": "San Pedro De Los Milagros",
    "SAN PEDRO": "San Pedro De Los Milagros",
    "SANTUARIO": "El Santuario",
    "SANTA ROSA": "Santa Rosa De Osos",
    "LANDAZURI -": "Landazuri",
    "VICTORIA- CALDAS": "Victoria",
}

_CASANARE_EQUIVALENCIAS = {
    "YOPA": "Yopal",
    "YOPAL": "Yopal",
    "PORE": "Pore",
    "MANI": "Mani",
    "TAME": "Tame",
    "PAEZ": "Paez",
    "ARAUCA": "Arauca",
    "MONTERREY": "Monterrey",
    "TRINIDAD": "Trinidad",
    "NUNCHIA": "Nunchia",
    "OROCUE": "Orocue",
    "TAMARA": "Tamara",
    "AGUAZUL": "Aguazul",
    "TAURAMENA": "Tauramena",
    "HATO COROZAL": "Hato Corozal",
    "PAZ DE ARIPORO": "Paz De Ariporo",
    "SAN LUIS DE PALENQUE": "San Luis De Palenque",
    "PUERTO RONDON": "Puerto Rondon",
    "PUERTO GAITAN": "Puerto Gaitan",
    "PUERTO LOPEZ": "Puerto Lopez",
    "SAN MARTIN": "San Martin",
    "CRAVO NORTE": "Cravo Norte",
    "SARAVENA": "Saravena",
    "SUSACON": "Susacon",
    "PARATEBUENO": "Paratebueno",
    "CUMARAL": "Cumaral",
    "PAJARITO": "Pajarito",
    "PAYA": "Paya",
    "PUERTO CONCORDIA": "Puerto Concordia",
    "ARAUQUITA": "Arauquita",
    "BUCARAMANGA": "Bucaramanga",
    "CABUYARO": "Cabuyaro",
    "CHAMEZA": "Chameza",
    "LABRANZAGRANDE": "Labranzagrande",
    "MAPIRIPAN": "Mapiripan",
    "PUERTO NARE": "Puerto Nare",
    "RECETOR": "Recetor",
    "SABANALARGA": "Sabanalarga",
    "SUAITA": "Suaita",
    "VILLANUEVA": "Villanueva",
}

_PATRON_ESPACIOS = re.compile(r"\s+")
_PATRON_INSTALACIONES = re.compile(r"^(ENTRA\s+|ENTRADA\s+|ENTRAN\s+)")


def limpiar_texto(valor) -> str | None:
    if valor is None:
        return None
    try:
        if valor != valor:
            return None
    except Exception:
        pass
    texto = str(valor).strip()
    if not texto or texto in {"<NA>", "nan", "None"}:
        return None
    return _PATRON_ESPACIOS.sub(" ", texto)


def normalizar_tipo_subasta(valor, feria: str) -> str:
    texto = limpiar_texto(valor)
    if not texto:
        return "Tradicional"

    texto_upper = texto.upper()
    if "GYR" in texto_upper:
        return "Especial GYR"
    if "EQUIN" in texto_upper:
        return "Equina"
    if "MULAR" in texto_upper:
        return "Mulares"
    if "ESPECIAL" in texto_upper:
        return "Especial"
    if "TRADICIONAL" in texto_upper or "COMERCIAL" in texto_upper:
        return "Tradicional"
    if feria == FERIA_CASANARE and (texto_upper == "GENERAL" or texto_upper == "CASANARE" or texto_upper.isdigit()):
        return "Tradicional"
    return texto.title()


def _normalizar_central(texto: str) -> str:
    texto_upper = texto.upper().strip(" -/")
    if _PATRON_INSTALACIONES.match(texto_upper):
        return "Instalaciones Central Ganadera"
    if texto_upper in _CENTRAL_EQUIVALENCIAS:
        return _CENTRAL_EQUIVALENCIAS[texto_upper]
    return texto_upper.title()


def _normalizar_casanare(texto: str) -> str:
    texto_upper = texto.upper().strip(" -/")
    texto_upper = texto_upper.lstrip("?")
    texto_upper = texto_upper.replace("?", "")
    texto_upper = texto_upper.replace("(", " ").replace(")", " ")
    texto_upper = _PATRON_ESPACIOS.sub(" ", texto_upper)

    if texto_upper.startswith("PAZ DE ARIPORO"):
        return "Paz De Ariporo"
    if texto_upper.startswith("PA Z DE ARIPORO") or texto_upper.startswith("PAZ DE AIPORO"):
        return "Paz De Ariporo"
    if texto_upper.startswith("TRINIDAD") or texto_upper.startswith("?TRINIDAD"):
        return "Trinidad"
    if texto_upper.startswith("TRNIDAD") or texto_upper.startswith("TRINIDIAD"):
        return "Trinidad"
    if texto_upper.startswith("SAN LUIS DE PALEN") or texto_upper.startswith("?SAN LUIS DE PALEN"):
        return "San Luis De Palenque"
    if texto_upper.startswith("SAN LUS DE PALEN") or texto_upper.startswith("SAN LUI DE PALEN"):
        return "San Luis De Palenque"
    if texto_upper.startswith("SAN LUIS DE PAOLEN") or texto_upper.startswith("SAN LUIS DE PLE"):
        return "San Luis De Palenque"
    if texto_upper.startswith("SAN LUISDE PALEN") or texto_upper.startswith("SAN LUIS DEPALEN"):
        return "San Luis De Palenque"
    if texto_upper.startswith("NUNCH"):
        return "Nunchia"
    if texto_upper.startswith("NUCHIA"):
        return "Nunchia"
    if texto_upper.startswith("OROCU"):
        return "Orocue"
    if texto_upper.startswith("TMARA") or texto_upper.startswith("TAMARA"):
        return "Tamara"
    if texto_upper.startswith("AGAUZUL"):
        return "Aguazul"
    if texto_upper.startswith("TAUARAMENA"):
        return "Tauramena"
    if texto_upper.startswith("PUERTO ROND"):
        return "Puerto Rondon"
    if texto_upper.startswith("PUERTO GAIT"):
        return "Puerto Gaitan"
    if texto_upper.startswith("PUERTO L"):
        return "Puerto Lopez"
    if texto_upper.startswith("SAN MART"):
        return "San Martin"
    if texto_upper.startswith("CRAVO NORTE") or texto_upper.startswith("ARAUCA CRAVO NORT"):
        return "Cravo Norte"
    if texto_upper.startswith("SARAVENA"):
        return "Saravena"
    if texto_upper.startswith("SUSAC"):
        return "Susacon"
    if texto_upper.startswith("HAT COROZAL") or texto_upper.startswith("HATO COROOZAL"):
        return "Hato Corozal"
    if texto_upper.startswith("HATO CORORZAL") or texto_upper.startswith("HAATO COROZAL"):
        return "Hato Corozal"
    if texto_upper.startswith("HATO COROZAL MONTA") or texto_upper.startswith("HATO COROZAL PUE"):
        return "Hato Corozal"
    if texto_upper.startswith("PARATEBUENO"):
        return "Paratebueno"
    if texto_upper.startswith("CUMARAL"):
        return "Cumaral"
    if texto_upper.startswith("PAJARITO"):
        return "Pajarito"
    if texto_upper.startswith("PAYA"):
        return "Paya"
    if texto_upper.startswith("PUERTO CONCORDIA"):
        return "Puerto Concordia"
    if texto_upper.startswith("PAEZ") or texto_upper.startswith("PEEZ"):
        return "Paez"
    if texto_upper.startswith("PEZ BOYACA"):
        return "Paez"
    if texto_upper.startswith("YOPA"):
        return "Yopal"
    if texto_upper.startswith("85001"):
        return "Yopal"
    if texto_upper.startswith("TAME ARAUCA"):
        return "Tame"
    if texto_upper.startswith("MANI-SANTA HELENA") or texto_upper.startswith("MANI SANTA HELENA"):
        return "Mani"
    if texto_upper.startswith("SUAITA - SANTANDER") or texto_upper.startswith("SUITA - SANTANDER"):
        return "Suaita"
    if texto_upper.startswith("CABUYARO - META"):
        return "Cabuyaro"
    if texto_upper.startswith("LABRANZAGRADE"):
        return "Labranzagrande"
    if texto_upper in _CASANARE_EQUIVALENCIAS:
        return _CASANARE_EQUIVALENCIAS[texto_upper]
    return texto_upper.title()


def normalizar_procedencia(valor, feria: str) -> str | None:
    texto = limpiar_texto(valor)
    if not texto:
        return None
    if feria == FERIA_CASANARE:
        return _normalizar_casanare(texto)
    return _normalizar_central(texto)
