from __future__ import annotations

import json
import os
import re
import hashlib
import gzip
import sqlite3
import struct
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import deque
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

try:
    import redis as redis_lib
except ImportError:
    redis_lib = None


ROOT = Path(__file__).resolve().parent
RECENT_DEBUG_LIMIT = 20
CODE_VERSION = "character-profiles-20260504"
KNOWN_MCA_COMMANDS = {
    "follow-player": "Follow the player talking to you",
    "stay-here": "Stay here for a while",
    "move-freely": "Move freely when asked to leave, move, or done talking",
    "wear-armor": "Equip any armor you have",
    "remove-armor": "Remove all the armor currently equipped",
    "try-go-home": "Try to go to your home in the village if possible",
    "open-trade-window": "Open the trade menu when the player is interested in trading, prices or inventory",
}
PROFESSION_DETAILS = {
    "armorer": {
        "label": "armero",
        "activities": "armaduras, escudos, yunques, reparar equipo y seguridad de la aldea",
    },
    "butcher": {
        "label": "carnicero",
        "activities": "comida, ahumadores, carne, provisiones y animales de granja",
    },
    "cartographer": {
        "label": "cartografo",
        "activities": "mapas, rutas, costas, islas, brujulas y lugares por explorar",
    },
    "cleric": {
        "label": "clerigo",
        "activities": "pociones, heridas, cuidados, rezos y remedios",
    },
    "farmer": {
        "label": "granjero",
        "activities": "semillas, cosechas, pan, compost, animales y comida",
    },
    "fisherman": {
        "label": "pescador",
        "activities": "redes, anzuelos, peces, barcas, cubos y clima para pescar",
    },
    "fletcher": {
        "label": "flechero",
        "activities": "arcos, flechas, plumas, pedernal y practica de punteria",
    },
    "leatherworker": {
        "label": "curtidor",
        "activities": "cuero, monturas, calderos y armaduras ligeras",
    },
    "librarian": {
        "label": "bibliotecario",
        "activities": "libros, estanterias, registros, encantamientos y rumores escritos",
    },
    "mason": {
        "label": "cantero",
        "activities": "piedra, hornos, ladrillos, muros, caminos y reparaciones",
    },
    "shepherd": {
        "label": "pastor",
        "activities": "lana, tintes, ovejas, telares y mantas",
    },
    "toolsmith": {
        "label": "herrero de herramientas",
        "activities": "picos, palas, hachas, herramientas y mantenimiento",
    },
    "weaponsmith": {
        "label": "herrero de armas",
        "activities": "espadas, hachas, filo, defensa y amenazas",
    },
    "blacksmith": {
        "label": "herrero",
        "activities": "forja, yunques, metal, reparar herramientas, mantener armas y reforzar equipo",
    },
    "miner": {
        "label": "minero",
        "activities": "picos, vetas, piedra, tuneles, minerales y seguridad bajo tierra",
    },
    "baker": {
        "label": "panadero",
        "activities": "pan, hornos, masa, trigo, dulces y comida para la aldea",
    },
    "guard": {
        "label": "guardia",
        "activities": "patrullas, antorchas, amenazas, proteger vecinos y vigilar entradas",
    },
    "warrior": {
        "label": "guerrero",
        "activities": "combate, entrenamiento, proteger aliados y enfrentar peligros",
    },
    "archer": {
        "label": "arquero",
        "activities": "arcos, flechas, vigilancia, distancia y defender la aldea",
    },
    "adventurer": {
        "label": "aventurero",
        "activities": "viajes, ruinas, mapas, encargos, peligros y descubrimientos",
    },
    "mercenary": {
        "label": "mercenario",
        "activities": "contratos, escoltas, combate por paga y proteger a quien lo merece",
    },
    "outlaw": {
        "label": "forajido",
        "activities": "problemas, escondites, favores dudosos y evitar autoridades",
    },
    "cultist": {
        "label": "cultista",
        "activities": "rituales, secretos, rumores oscuros y lealtades peligrosas",
    },
    "nitwit": {
        "label": "aldeano sin oficio fijo",
        "activities": "recados, chismes, paseos y excusas para evitar trabajo estable",
    },
}
PROFESSION_ALIASES = {
    "armor": "armorer",
    "armorer": "armorer",
    "armourer": "armorer",
    "armero": "armorer",
    "armorsmith": "armorer",
    "butcher": "butcher",
    "carnicero": "butcher",
    "cartographer": "cartographer",
    "cartografo": "cartographer",
    "cartógrafo": "cartographer",
    "cleric": "cleric",
    "clerigo": "cleric",
    "clérigo": "cleric",
    "farmer": "farmer",
    "granjero": "farmer",
    "fisherman": "fisherman",
    "pescador": "fisherman",
    "fletcher": "fletcher",
    "flechero": "fletcher",
    "leatherworker": "leatherworker",
    "curtidor": "leatherworker",
    "librarian": "librarian",
    "bibliotecario": "librarian",
    "mason": "mason",
    "stone_mason": "mason",
    "stonemason": "mason",
    "cantero": "mason",
    "shepherd": "shepherd",
    "pastor": "shepherd",
    "toolsmith": "toolsmith",
    "herrero de herramientas": "toolsmith",
    "herrero_de_herramientas": "toolsmith",
    "weaponsmith": "weaponsmith",
    "herrero de armas": "weaponsmith",
    "herrero_de_armas": "weaponsmith",
    "blacksmith": "blacksmith",
    "smith": "blacksmith",
    "herrero": "blacksmith",
    "herrera": "blacksmith",
    "forjador": "blacksmith",
    "forjadora": "blacksmith",
    "miner": "miner",
    "minero": "miner",
    "minera": "miner",
    "baker": "baker",
    "panadero": "baker",
    "panadera": "baker",
    "guard": "guard",
    "guardia": "guard",
    "warrior": "warrior",
    "guerrero": "warrior",
    "guerrera": "warrior",
    "archer": "archer",
    "arquero": "archer",
    "arquera": "archer",
    "adventurer": "adventurer",
    "aventurero": "adventurer",
    "aventurera": "adventurer",
    "mercenary": "mercenary",
    "mercenario": "mercenary",
    "mercenaria": "mercenary",
    "outlaw": "outlaw",
    "forajido": "outlaw",
    "forajida": "outlaw",
    "cultist": "cultist",
    "cultista": "cultist",
    "nitwit": "nitwit",
    "jobless": "nitwit",
    "unemployed": "nitwit",
    "desempleado": "nitwit",
    "sin oficio": "nitwit",
}
PROFESSION_PREFIX_PATTERN = r"(?:minecraft:|profession[._:-]|mca[._:-]profession[._:-])?"
MINIMAL_PROMPT = (
    "Eres un aldeano de rol de MCA, vivo y conversador. "
    "Responde en espanol natural, 1-2 frases, siempre en primera persona como dialogo directo entre ustedes; no narres sobre ti en tercera persona. "
    "No termines siempre con preguntas. "
    "Para continuar una charla, usa reaccion, opinion, recuerdo o una pregunta breve solo cuando encaje. "
    "No menciones tu propio nombre salvo que el jugador te lo pregunte; no saludes repitiendo tu nombre. "
    "Si el jugador usa tu nombre en una orden, normalmente te esta llamando a ti, no nombrando a otro objetivo. "
    "No digas el nombre ni apodo del jugador como muletilla; usalo solo si el jugador lo pide, si corriges identidad, familia o lore, o si la frase realmente lo necesita. "
    "No uses emojis, caritas, asteriscos ni formato de accion entre asteriscos. "
    "El mensaje final debe ser texto limpio y legible: sin llaves, backticks, simbolos sueltos, codigo, listas raras ni fragmentos de JSON dentro del dialogo. "
    "Sabes que vives en Minecraft, en un mundo de islas, aldeas, bloques, oficios, cuevas y mobs. "
    "El archipielago oceanico es trasfondo: no menciones mar, islas, olas ni tormentas salvo que sea relevante. "
    "Si el jugador pregunta que haces o a que te dedicas, responde con tu oficio real detectado; si no hay oficio claro, dilo sin inventar. "
    "No conviertas una tarea aislada, un recuerdo antiguo o una palabra suelta en tu profesion permanente. "
    "Puedes proponer microacciones de rol coherentes con tu oficio, familia o entorno, como pescar, mapear, cocinar, patrullar, abrazar o defender a alguien. "
    "Distingue dia, noche, lluvia y trueno solo si MCA lo envia en el contexto; no inventes que es de noche o que hay peligro si no aparece en la escena. "
    "Si el jugador acaba de regalarte algo, besarte, abrazarte o contarte un chiste, reacciona a eso con continuidad segun personalidad, relacion, corazones y consentimiento. "
    "Ajusta tono por personalidad, humor, corazones, relacion y ordenes disponibles. "
    "Los rasgos y emociones actuales importan: daltonismo, atletismo, orientacion romantica, miedos, cansancio, enojo, alegria o tristeza deben notarse en tono y detalles, no como lista. "
    "La orientacion romantica afecta a quien puede atraerle o incomodarle; no la conviertas en chiste ni estereotipo. "
    "Marca el temperamento desde la primera frase: grunon seco, sensible suave, alegre expresivo, coqueto calido o serio directo. "
    "Con desconocidos o baja relacion puedes ser brusco y usar groserias comunes o insultos adultos; con conyuge, familia o muchos corazones prioriza lealtad y carino. "
    "Si eres pareja o conyuge del jugador, se mas meloso y atento segun tu personalidad, y muestra afecto por sus hijos. "
    "Si hablan de familia, futuro, miedo, perdon o promesas, responde con mas peso emocional pero sin alargarte. "
    "Usa presente para familiares vivos y pasado solo para fallecidos; si el jugador afirma un lazo falso, corrigelo en personaje. "
    "Si te piden una historia propia, inventa una anecdota corta y coherente con tu familia, aldea, oficio y memoria. "
    "Puedes rolear intenciones o acciones en primera persona como abrazar, cocinar, amenazar o defender, aunque no siempre sean acciones mecanicas reales. "
    "Responde al ultimo mensaje del jugador como si lo estuvieras escuchando, sin cambiar de tema porque si. "
    "No repitas el nombre del jugador en cada respuesta. "
    "Nunca olvides que el nombre propio actual eres tu; no hables de tu propio nombre como si fuera otra persona. "
    "Romance solo adultos y con consentimiento; evita odio por identidad y sexo grafico. "
    "Si MCA pide JSON, responde solo JSON valido con message limpio y optionalCommand; no mezcles texto fuera del JSON."
)


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and value and not os.environ.get(key):
            os.environ[key] = value


def env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def raw_turn_memory_enabled() -> bool:
    return env_bool("MCA_STORE_RAW_TURNS", True)


def output_token_limit() -> int:
    requested = env_int("OPENAI_MAX_OUTPUT_TOKENS", 220)
    minimum = env_int("OPENAI_MIN_OUTPUT_TOKENS", 180)
    return max(requested, minimum)


def redis_key_part(value: str) -> str:
    text = str(value or "unknown").strip() or "unknown"
    readable = re.sub(r"[^a-zA-Z0-9_.-]+", "_", text)[:36].strip("_") or "id"
    digest = hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]
    return f"{readable}-{digest}"


def redis_namespace() -> str:
    raw = os.environ.get("MCA_REDIS_NAMESPACE", "mca-reborn")
    namespace = re.sub(r"[^a-zA-Z0-9_.-]+", "_", raw).strip("_")
    return namespace or "mca-reborn"


def read_prompt() -> str:
    if os.environ.get("MCA_PROMPT_MODE", "minimal").strip().lower() == "minimal":
        return MINIMAL_PROMPT
    prompt_path = ROOT / os.environ.get("MCA_ROLEPLAY_PROMPT", "prompts/ocean_roleplay.txt")
    if not prompt_path.exists():
        return ""
    return prompt_path.read_text(encoding="utf-8").strip()


def load_profiles() -> dict[str, str]:
    profile_path = ROOT / os.environ.get("MCA_CHARACTER_PROFILES", "profiles/characters.json")
    if not profile_path.exists():
        return {}
    try:
        data = json.loads(profile_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return {
        str(name).strip().lower(): compact_text(str(profile), 360)
        for name, profile in data.items()
        if str(name).strip() and str(profile).strip()
    }


def load_player_lore() -> dict[str, str]:
    lore_path = ROOT / os.environ.get("MCA_PLAYER_LORE", "profiles/player_lore.json")
    if not lore_path.exists():
        return {}
    try:
        data = json.loads(lore_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    lore: dict[str, str] = {}
    for name, value in data.items():
        key = str(name).strip().casefold()
        if key and str(value).strip():
            lore[key] = compact_text(str(value), env_int("MCA_PLAYER_LORE_MAX_CHARS", 360))
    return lore


def player_lore_context(player_name: str, lore: dict[str, str]) -> str:
    if not player_name:
        return ""
    value = lore.get(player_name.casefold())
    if not value:
        return ""
    return (
        f"Lore del jugador {player_name}: {value} "
        "Usa este lore solo cuando encaje; elige 1-2 detalles y varia cuales mencionas."
    )


def mentioned_lore_context(last_user: str, lore: dict[str, str], current_player_name: str) -> str:
    if not last_user or not lore:
        return ""
    mentioned: list[str] = []
    for name, value in lore.items():
        patterns = loose_name_patterns(name)
        if name.casefold() == "jenner_ola":
            patterns.extend([r"jenner\s+h?ola", r"jenner"])
        if any(re.search(rf"(?<!\w){pattern}(?!\w)", last_user, re.IGNORECASE) for pattern in patterns):
            label = current_player_name if name.casefold() == current_player_name.casefold() else name
            mentioned.append(f"{label}: {value}")
    if not mentioned:
        return ""
    return (
        "Lore esencial mencionado en el ultimo mensaje: "
        + " ".join(mentioned[:3])
        + " El jugador acaba de mencionar esos nombres; responde con al menos un detalle concreto de este lore y no lo ignores. "
        + "Elige 1-2 detalles relevantes, varia cuales usas, no recites toda la ficha y puedes ser creativo."
    )


def generate_npc_identity(world_id: str, character_id: str, villager_name: str, system_text: str) -> str:
    seed = int(hashlib.sha1(f"{world_id}:{character_id}:{villager_name}".encode("utf-8")).hexdigest()[:12], 16)
    temperaments = [
        "reservado pero atento",
        "orgulloso y trabajador",
        "bromista seco",
        "protector con los suyos",
        "curioso y algo imprudente",
        "paciente pero dificil de impresionar",
        "sensible y observador",
        "directo, con poca paciencia",
    ]
    likes = [
        "las antorchas bien puestas",
        "el pan recien hecho",
        "los mapas marcados a mano",
        "las herramientas cuidadas",
        "las historias de taberna",
        "los muelles tranquilos",
        "las casas con buen techo",
        "los dias de trabajo ordenado",
    ]
    dislikes = [
        "que lo apuren sin explicar",
        "el desorden en la aldea",
        "las promesas vacias",
        "los ruidos de mobs cerca",
        "que se burlen de su oficio",
        "perder herramientas",
        "la gente que grita de lejos",
        "que le cambien el tema de golpe",
    ]
    habits = [
        "responder primero con una opinion corta",
        "mirar alrededor antes de aceptar favores",
        "usar su oficio como excusa para retirarse",
        "soltar una queja pequena antes de ayudar",
        "recordar detalles practicos de la aldea",
        "proteger a familiares antes que presumir valentia",
        "contar anecdotas breves si gana confianza",
        "hacer comentarios concretos sobre comida, piedra o clima",
    ]
    identity = (
        f"Rasgo estable: {temperaments[seed % len(temperaments)]}; "
        f"le gustan {likes[(seed // 7) % len(likes)]}; "
        f"le incomoda {dislikes[(seed // 13) % len(dislikes)]}; "
        f"suele {habits[(seed // 19) % len(habits)]}."
    )
    return compact_text(identity, env_int("MCA_NPC_IDENTITY_MAX_CHARS", 260))


def sanitize_legacy_npc_identity(identity: str) -> str:
    sentences = re.split(r"(?<=[.!?])\s+", identity.strip())
    cleaned: list[str] = []
    for sentence in sentences:
        text = normalize_for_match(sentence)
        if "su oficio lo inclina" in text:
            continue
        if re.search(r"\b(oficio|profesion|profession|job)\b", text):
            continue
        cleaned.append(sentence.strip())
    return compact_text(" ".join(part for part in cleaned if part), env_int("MCA_NPC_IDENTITY_MAX_CHARS", 260))


class MemoryStore:
    backend_name = "sqlite"

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS turns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at INTEGER NOT NULL,
                    world_id TEXT NOT NULL,
                    player_id TEXT NOT NULL,
                    character_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL
                )
                """
            )
            db.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_turns_lookup
                ON turns(world_id, player_id, character_id, id)
                """
            )
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS facts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    world_id TEXT NOT NULL,
                    player_id TEXT NOT NULL,
                    character_id TEXT NOT NULL,
                    key TEXT NOT NULL,
                    fact TEXT NOT NULL,
                    weight INTEGER NOT NULL DEFAULT 1,
                    UNIQUE(world_id, player_id, character_id, key)
                )
                """
            )
            db.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_facts_lookup
                ON facts(world_id, player_id, character_id, weight DESC, updated_at DESC)
                """
            )
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS player_facts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    world_id TEXT NOT NULL,
                    player_id TEXT NOT NULL,
                    key TEXT NOT NULL,
                    fact TEXT NOT NULL,
                    weight INTEGER NOT NULL DEFAULT 1,
                    UNIQUE(world_id, player_id, key)
                )
                """
            )
            db.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_player_facts_lookup
                ON player_facts(world_id, player_id, weight DESC, updated_at DESC)
                """
            )
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS npc_identities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    world_id TEXT NOT NULL,
                    character_id TEXT NOT NULL,
                    identity TEXT NOT NULL,
                    UNIQUE(world_id, character_id)
                )
                """
            )
            db.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_npc_identities_lookup
                ON npc_identities(world_id, character_id)
                """
            )

    @contextmanager
    def connect(self) -> Any:
        db = sqlite3.connect(self.path)
        try:
            yield db
            db.commit()
        finally:
            db.close()

    def add_turn(self, ids: dict[str, str], role: str, content: str) -> None:
        if not raw_turn_memory_enabled():
            return
        content = compact_text(content, 700)
        if not content:
            return
        with self.connect() as db:
            db.execute(
                """
                INSERT INTO turns(created_at, world_id, player_id, character_id, role, content)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    int(time.time()),
                    ids["world_id"],
                    ids["player_id"],
                    ids["character_id"],
                    role,
                    content,
                ),
            )
            self.prune_turns(db, ids, env_int("MCA_RAW_TURN_LIMIT", 12))

    def recent_turns(self, ids: dict[str, str], limit: int) -> list[tuple[str, str]]:
        if limit <= 0 or not raw_turn_memory_enabled():
            return []
        with self.connect() as db:
            rows = db.execute(
                """
                SELECT role, content FROM turns
                WHERE world_id = ? AND player_id = ? AND character_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (ids["world_id"], ids["player_id"], ids["character_id"], limit),
            ).fetchall()
        return list(reversed(rows))

    def add_fact(self, ids: dict[str, str], fact: str, weight: int = 1) -> None:
        if ids["character_id"] == "unknown_character":
            return
        fact = compact_text(fact, 220)
        if not fact:
            return
        key = hashlib.sha1(fact.lower().encode("utf-8")).hexdigest()
        now = int(time.time())
        with self.connect() as db:
            db.execute(
                """
                INSERT INTO facts(created_at, updated_at, world_id, player_id, character_id, key, fact, weight)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(world_id, player_id, character_id, key)
                DO UPDATE SET
                    updated_at = excluded.updated_at,
                    weight = max(facts.weight, excluded.weight)
                """,
                (
                    now,
                    now,
                    ids["world_id"],
                    ids["player_id"],
                    ids["character_id"],
                    key,
                    fact,
                    weight,
                ),
            )
            self.prune_facts(db, ids, env_int("MCA_FACT_LIMIT", 24))

    def essential_facts(self, ids: dict[str, str], limit: int) -> list[str]:
        if limit <= 0 or ids["character_id"] == "unknown_character":
            return []
        with self.connect() as db:
            rows = db.execute(
                """
                SELECT fact FROM facts
                WHERE world_id = ? AND player_id = ? AND character_id = ?
                ORDER BY weight DESC, updated_at DESC
                LIMIT ?
                """,
                (ids["world_id"], ids["player_id"], ids["character_id"], limit),
            ).fetchall()
        return [row[0] for row in rows]

    def add_player_fact(self, ids: dict[str, str], fact: str, weight: int = 1) -> None:
        if ids["player_id"] == "unknown_player":
            return
        fact = compact_text(fact, 220)
        if not fact:
            return
        key = hashlib.sha1(fact.lower().encode("utf-8")).hexdigest()
        now = int(time.time())
        with self.connect() as db:
            db.execute(
                """
                INSERT INTO player_facts(created_at, updated_at, world_id, player_id, key, fact, weight)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(world_id, player_id, key)
                DO UPDATE SET
                    updated_at = excluded.updated_at,
                    weight = max(player_facts.weight, excluded.weight)
                """,
                (
                    now,
                    now,
                    ids["world_id"],
                    ids["player_id"],
                    key,
                    fact,
                    weight,
                ),
            )
            self.prune_player_facts(db, ids, env_int("MCA_PLAYER_FACT_LIMIT", 24))

    def player_facts(self, ids: dict[str, str], limit: int) -> list[str]:
        if limit <= 0 or ids["player_id"] == "unknown_player":
            return []
        with self.connect() as db:
            rows = db.execute(
                """
                SELECT fact FROM player_facts
                WHERE world_id = ? AND player_id = ?
                ORDER BY weight DESC, updated_at DESC
                LIMIT ?
                """,
                (ids["world_id"], ids["player_id"], limit),
            ).fetchall()
        return [row[0] for row in rows]

    def npc_identity(self, ids: dict[str, str], villager_name: str, system_text: str) -> str:
        if not env_bool("MCA_NPC_IDENTITIES", True):
            return ""
        if ids["world_id"] == "unknown_world" or ids["character_id"] == "unknown_character":
            return ""
        with self.connect() as db:
            row = db.execute(
                """
                SELECT identity FROM npc_identities
                WHERE world_id = ? AND character_id = ?
                """,
                (ids["world_id"], ids["character_id"]),
            ).fetchone()
            if row and str(row[0]).strip():
                identity = sanitize_legacy_npc_identity(str(row[0]))
                if identity:
                    if identity != str(row[0]):
                        db.execute(
                            """
                            UPDATE npc_identities
                            SET updated_at = ?, identity = ?
                            WHERE world_id = ? AND character_id = ?
                            """,
                            (int(time.time()), identity, ids["world_id"], ids["character_id"]),
                        )
                    return identity
            identity = generate_npc_identity(
                ids["world_id"], ids["character_id"], villager_name, system_text
            )
            now = int(time.time())
            db.execute(
                """
                INSERT INTO npc_identities(created_at, updated_at, world_id, character_id, identity)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(world_id, character_id)
                DO UPDATE SET updated_at = excluded.updated_at
                """,
                (now, now, ids["world_id"], ids["character_id"], identity),
            )
        return identity

    def prune_turns(self, db: sqlite3.Connection, ids: dict[str, str], keep: int) -> None:
        db.execute(
            """
            DELETE FROM turns
            WHERE world_id = ? AND player_id = ? AND character_id = ?
              AND id NOT IN (
                  SELECT id FROM turns
                  WHERE world_id = ? AND player_id = ? AND character_id = ?
                  ORDER BY id DESC
                  LIMIT ?
              )
            """,
            (
                ids["world_id"],
                ids["player_id"],
                ids["character_id"],
                ids["world_id"],
                ids["player_id"],
                ids["character_id"],
                keep,
            ),
        )

    def prune_facts(self, db: sqlite3.Connection, ids: dict[str, str], keep: int) -> None:
        db.execute(
            """
            DELETE FROM facts
            WHERE world_id = ? AND player_id = ? AND character_id = ?
              AND id NOT IN (
                  SELECT id FROM facts
                  WHERE world_id = ? AND player_id = ? AND character_id = ?
                  ORDER BY weight DESC, updated_at DESC
                  LIMIT ?
              )
            """,
            (
                ids["world_id"],
                ids["player_id"],
                ids["character_id"],
                ids["world_id"],
                ids["player_id"],
                ids["character_id"],
                keep,
            ),
        )

    def prune_player_facts(self, db: sqlite3.Connection, ids: dict[str, str], keep: int) -> None:
        db.execute(
            """
            DELETE FROM player_facts
            WHERE world_id = ? AND player_id = ?
              AND id NOT IN (
                  SELECT id FROM player_facts
                  WHERE world_id = ? AND player_id = ?
                  ORDER BY weight DESC, updated_at DESC
                  LIMIT ?
              )
            """,
            (
                ids["world_id"],
                ids["player_id"],
                ids["world_id"],
                ids["player_id"],
                keep,
            ),
        )


class RedisMemoryStore:
    backend_name = "redis"

    def __init__(self, client: Any, namespace: str) -> None:
        self.client = client
        self.namespace = namespace
        self.client.ping()

    def _key(self, *parts: str) -> str:
        return ":".join([self.namespace, *parts])

    def _personal_parts(self, ids: dict[str, str]) -> list[str]:
        return [
            redis_key_part(ids.get("world_id", "unknown_world")),
            redis_key_part(ids.get("player_id", "unknown_player")),
            redis_key_part(ids.get("character_id", "unknown_character")),
        ]

    def _player_parts(self, ids: dict[str, str]) -> list[str]:
        return [
            redis_key_part(ids.get("world_id", "unknown_world")),
            redis_key_part(ids.get("player_id", "unknown_player")),
        ]

    def _has_personal_ids(self, ids: dict[str, str]) -> bool:
        return (
            ids.get("world_id") != "unknown_world"
            and ids.get("player_id") != "unknown_player"
            and ids.get("character_id") != "unknown_character"
        )

    def _fact_score(self, weight: int, updated_at: int) -> int:
        return max(weight, 0) * 10_000_000_000 + updated_at

    def add_turn(self, ids: dict[str, str], role: str, content: str) -> None:
        if not raw_turn_memory_enabled() or not self._has_personal_ids(ids):
            return
        content = compact_text(content, env_int("MCA_RAW_TURN_MAX_CHARS", 700))
        if not content:
            return
        key = self._key("turns", *self._personal_parts(ids))
        item = json.dumps(
            {"ts": int(time.time()), "role": role, "content": content},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        try:
            pipe = self.client.pipeline()
            pipe.rpush(key, item)
            pipe.ltrim(key, -env_int("MCA_RAW_TURN_LIMIT", 12), -1)
            pipe.execute()
        except Exception as exc:
            print(f"[memory redis] add_turn failed: {exc}")

    def recent_turns(self, ids: dict[str, str], limit: int) -> list[tuple[str, str]]:
        if limit <= 0 or not raw_turn_memory_enabled() or not self._has_personal_ids(ids):
            return []
        key = self._key("turns", *self._personal_parts(ids))
        try:
            rows = self.client.lrange(key, -limit, -1)
        except Exception as exc:
            print(f"[memory redis] recent_turns failed: {exc}")
            return []
        turns: list[tuple[str, str]] = []
        for row in rows:
            try:
                item = json.loads(row)
            except (TypeError, ValueError):
                continue
            role = str(item.get("role") or "")
            content = str(item.get("content") or "")
            if role and content:
                turns.append((role, content))
        return turns

    def add_fact(self, ids: dict[str, str], fact: str, weight: int = 1) -> None:
        if not self._has_personal_ids(ids):
            return
        fact = compact_text(fact, 220)
        if not fact:
            return
        fact_hash = hashlib.sha1(fact.lower().encode("utf-8")).hexdigest()
        now = int(time.time())
        zkey = self._key("facts", *self._personal_parts(ids))
        hkey = self._key("factdata", *self._personal_parts(ids))
        try:
            old_raw = self.client.hget(hkey, fact_hash)
            if old_raw:
                old = json.loads(old_raw)
                weight = max(int(old.get("weight", 1)), weight)
            payload = json.dumps(
                {"fact": fact, "weight": weight, "updated_at": now},
                ensure_ascii=False,
                separators=(",", ":"),
            )
            pipe = self.client.pipeline()
            pipe.hset(hkey, fact_hash, payload)
            pipe.zadd(zkey, {fact_hash: self._fact_score(weight, now)})
            pipe.execute()
            self._prune_fact_hash(zkey, hkey, env_int("MCA_FACT_LIMIT", 24))
        except Exception as exc:
            print(f"[memory redis] add_fact failed: {exc}")

    def essential_facts(self, ids: dict[str, str], limit: int) -> list[str]:
        if limit <= 0 or not self._has_personal_ids(ids):
            return []
        zkey = self._key("facts", *self._personal_parts(ids))
        hkey = self._key("factdata", *self._personal_parts(ids))
        try:
            fact_hashes = self.client.zrevrange(zkey, 0, limit - 1)
            rows = self.client.hmget(hkey, fact_hashes) if fact_hashes else []
        except Exception as exc:
            print(f"[memory redis] essential_facts failed: {exc}")
            return []
        facts: list[str] = []
        for row in rows:
            if not row:
                continue
            try:
                fact = str(json.loads(row).get("fact") or "")
            except (TypeError, ValueError):
                continue
            if fact:
                facts.append(fact)
        return facts

    def add_player_fact(self, ids: dict[str, str], fact: str, weight: int = 1) -> None:
        if ids.get("world_id") == "unknown_world" or ids.get("player_id") == "unknown_player":
            return
        fact = compact_text(fact, 220)
        if not fact:
            return
        fact_hash = hashlib.sha1(fact.lower().encode("utf-8")).hexdigest()
        now = int(time.time())
        zkey = self._key("player_facts", *self._player_parts(ids))
        hkey = self._key("player_factdata", *self._player_parts(ids))
        try:
            old_raw = self.client.hget(hkey, fact_hash)
            if old_raw:
                old = json.loads(old_raw)
                weight = max(int(old.get("weight", 1)), weight)
            payload = json.dumps(
                {"fact": fact, "weight": weight, "updated_at": now},
                ensure_ascii=False,
                separators=(",", ":"),
            )
            pipe = self.client.pipeline()
            pipe.hset(hkey, fact_hash, payload)
            pipe.zadd(zkey, {fact_hash: self._fact_score(weight, now)})
            pipe.execute()
            self._prune_fact_hash(zkey, hkey, env_int("MCA_PLAYER_FACT_LIMIT", 24))
        except Exception as exc:
            print(f"[memory redis] add_player_fact failed: {exc}")

    def player_facts(self, ids: dict[str, str], limit: int) -> list[str]:
        if limit <= 0 or ids.get("world_id") == "unknown_world" or ids.get("player_id") == "unknown_player":
            return []
        zkey = self._key("player_facts", *self._player_parts(ids))
        hkey = self._key("player_factdata", *self._player_parts(ids))
        try:
            fact_hashes = self.client.zrevrange(zkey, 0, limit - 1)
            rows = self.client.hmget(hkey, fact_hashes) if fact_hashes else []
        except Exception as exc:
            print(f"[memory redis] player_facts failed: {exc}")
            return []
        facts: list[str] = []
        for row in rows:
            if not row:
                continue
            try:
                fact = str(json.loads(row).get("fact") or "")
            except (TypeError, ValueError):
                continue
            if fact:
                facts.append(fact)
        return facts

    def npc_identity(self, ids: dict[str, str], villager_name: str, system_text: str) -> str:
        if not env_bool("MCA_NPC_IDENTITIES", True):
            return ""
        if ids.get("world_id") == "unknown_world" or ids.get("character_id") == "unknown_character":
            return ""
        key = self._key(
            "npc_identity",
            redis_key_part(ids.get("world_id", "unknown_world")),
            redis_key_part(ids.get("character_id", "unknown_character")),
        )
        try:
            existing = self.client.get(key)
            if existing and str(existing).strip():
                identity = sanitize_legacy_npc_identity(str(existing))
                if identity:
                    if identity != str(existing):
                        self.client.set(key, identity)
                    return identity
            identity = generate_npc_identity(
                ids["world_id"], ids["character_id"], villager_name, system_text
            )
            self.client.set(key, identity)
            return identity
        except Exception as exc:
            print(f"[memory redis] npc_identity failed: {exc}")
            return ""

    def _prune_fact_hash(self, zkey: str, hkey: str, keep: int) -> None:
        extra = self.client.zcard(zkey) - max(keep, 0)
        if extra > 0:
            old_hashes = self.client.zrange(zkey, 0, extra - 1)
            if old_hashes:
                pipe = self.client.pipeline()
                pipe.zrem(zkey, *old_hashes)
                pipe.hdel(hkey, *old_hashes)
                pipe.execute()
        current = set(self.client.zrange(zkey, 0, -1))
        stored = set(self.client.hkeys(hkey))
        stale = list(stored - current)
        if stale:
            self.client.hdel(hkey, *stale)


def redis_client_from_env() -> Any:
    if redis_lib is None:
        raise RuntimeError("redis package is not installed")
    redis_url = os.environ.get("REDIS_URL") or os.environ.get("MCA_REDIS_URL")
    timeout = env_int("MCA_REDIS_TIMEOUT_SECONDS", 5)
    if redis_url:
        return redis_lib.Redis.from_url(
            redis_url,
            decode_responses=True,
            socket_timeout=timeout,
            socket_connect_timeout=timeout,
        )
    host = os.environ.get("MCA_REDIS_HOST") or os.environ.get("REDIS_HOST")
    if not host:
        raise RuntimeError("missing REDIS_HOST or MCA_REDIS_HOST")
    return redis_lib.Redis(
        host=host,
        port=env_int("MCA_REDIS_PORT", env_int("REDIS_PORT", 6379)),
        db=env_int("MCA_REDIS_DB", env_int("REDIS_DB", 0)),
        username=os.environ.get("MCA_REDIS_USERNAME") or os.environ.get("REDIS_USERNAME") or None,
        password=os.environ.get("MCA_REDIS_PASSWORD") or os.environ.get("REDIS_PASSWORD") or None,
        ssl=env_bool("MCA_REDIS_SSL", env_bool("REDIS_SSL", False)),
        decode_responses=True,
        socket_timeout=timeout,
        socket_connect_timeout=timeout,
    )


def create_memory_store(db_path: Path) -> Any:
    backend = os.environ.get("MCA_MEMORY_BACKEND", "").strip().lower()
    redis_configured = bool(
        os.environ.get("REDIS_URL")
        or os.environ.get("MCA_REDIS_URL")
        or os.environ.get("REDIS_HOST")
        or os.environ.get("MCA_REDIS_HOST")
    )
    if backend == "redis" or (not backend and redis_configured):
        try:
            store = RedisMemoryStore(redis_client_from_env(), redis_namespace())
            print(f"[memory] using Redis backend namespace={store.namespace!r}")
            return store
        except Exception as exc:
            print(f"[memory] Redis unavailable, falling back to SQLite: {exc}")
    print(f"[memory] using SQLite backend at {db_path}")
    return MemoryStore(db_path)


class NbtReader:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.index = 0

    def read(self, size: int) -> bytes:
        if self.index + size > len(self.data):
            raise EOFError("NBT ended while reading")
        chunk = self.data[self.index : self.index + size]
        self.index += size
        return chunk

    def u8(self) -> int:
        return self.read(1)[0]

    def i8(self) -> int:
        return struct.unpack(">b", self.read(1))[0]

    def u16(self) -> int:
        return struct.unpack(">H", self.read(2))[0]

    def i16(self) -> int:
        return struct.unpack(">h", self.read(2))[0]

    def i32(self) -> int:
        return struct.unpack(">i", self.read(4))[0]

    def i64(self) -> int:
        return struct.unpack(">q", self.read(8))[0]

    def f32(self) -> float:
        return struct.unpack(">f", self.read(4))[0]

    def f64(self) -> float:
        return struct.unpack(">d", self.read(8))[0]

    def string(self) -> str:
        size = self.u16()
        return self.read(size).decode("utf-8", errors="replace")

    def payload(self, tag_type: int) -> Any:
        if tag_type == 0:
            return None
        if tag_type == 1:
            return self.i8()
        if tag_type == 2:
            return self.i16()
        if tag_type == 3:
            return self.i32()
        if tag_type == 4:
            return self.i64()
        if tag_type == 5:
            return self.f32()
        if tag_type == 6:
            return self.f64()
        if tag_type == 7:
            size = self.i32()
            return list(self.read(size))
        if tag_type == 8:
            return self.string()
        if tag_type == 9:
            element_type = self.u8()
            size = self.i32()
            return [self.payload(element_type) for _ in range(size)]
        if tag_type == 10:
            compound: dict[str, Any] = {}
            while True:
                child_type = self.u8()
                if child_type == 0:
                    return compound
                name = self.string()
                compound[name] = self.payload(child_type)
        if tag_type == 11:
            size = self.i32()
            return [self.i32() for _ in range(size)]
        if tag_type == 12:
            size = self.i32()
            return [self.i64() for _ in range(size)]
        raise ValueError(f"Unknown NBT tag type {tag_type}")

    def root(self) -> dict[str, Any]:
        tag_type = self.u8()
        self.string()
        value = self.payload(tag_type)
        if not isinstance(value, dict):
            raise ValueError("NBT root was not a compound")
        return value


def read_nbt_file(path: Path) -> dict[str, Any]:
    raw = path.read_bytes()
    data = gzip.decompress(raw) if raw.startswith(b"\x1f\x8b") else raw
    return NbtReader(data).root()


def uuid_from_int_array(value: Any) -> str | None:
    if not isinstance(value, list) or len(value) != 4:
        return None
    parts = [int(part) & 0xFFFFFFFF for part in value]
    if parts == [0, 0, 0, 0]:
        return None
    raw_uuid = (parts[0] << 96) | (parts[1] << 64) | (parts[2] << 32) | parts[3]
    return str(uuid.UUID(int=raw_uuid))


class FamilyTreeCache:
    RELATIONSHIP_STATES = {
        0: "sin pareja registrada",
        1: "enamorado/a o prometido/a",
        2: "comprometido/a",
        3: "casado/a con aldeano",
        4: "casado/a con jugador",
        5: "viudo/a",
    }
    GENDERS = {
        1: "hombre",
        2: "mujer",
        3: "persona",
    }

    def __init__(self, data_dir: Path) -> None:
        self.path = data_dir / "MCA-FamilyTree.dat"
        self.modified_at = 0.0
        self.entries: dict[str, dict[str, Any]] = {}
        self.last_loaded = 0.0

    def refresh(self) -> None:
        if not env_bool("MCA_FAMILY_CONTEXT", True) or not self.path.exists():
            return
        now = time.time()
        if now - self.last_loaded < env_int("MCA_FAMILY_REFRESH_SECONDS", 5):
            return
        self.last_loaded = now
        modified_at = self.path.stat().st_mtime
        if modified_at == self.modified_at and self.entries:
            return
        try:
            root = read_nbt_file(self.path)
            data = root.get("data", {})
            if not isinstance(data, dict):
                return
            entries: dict[str, dict[str, Any]] = {}
            for entry_id, raw_node in data.items():
                if isinstance(raw_node, dict):
                    entries[str(entry_id)] = self.normalize_node(str(entry_id), raw_node)
            self.entries = entries
            self.modified_at = modified_at
        except Exception as exc:
            print(f"[MCA family] No pude leer {self.path.name}: {exc}")

    def normalize_node(self, entry_id: str, raw_node: dict[str, Any]) -> dict[str, Any]:
        children: list[str] = []
        for child in raw_node.get("children", []):
            child_id = None
            if isinstance(child, dict):
                child_id = uuid_from_int_array(child.get("id") or child.get("uuid"))
            elif isinstance(child, list):
                child_id = uuid_from_int_array(child)
            if child_id:
                children.append(child_id)
        partners: list[str] = []
        for key in ("spouse", "partner"):
            partner_id = uuid_from_int_array(raw_node.get(key))
            if partner_id and partner_id not in partners:
                partners.append(partner_id)
        for key in ("spouses", "partners"):
            raw_partners = raw_node.get(key, [])
            if isinstance(raw_partners, list):
                for raw_partner in raw_partners:
                    partner_id = None
                    if isinstance(raw_partner, dict):
                        partner_id = uuid_from_int_array(raw_partner.get("id") or raw_partner.get("uuid"))
                    elif isinstance(raw_partner, list):
                        partner_id = uuid_from_int_array(raw_partner)
                    if partner_id and partner_id not in partners:
                        partners.append(partner_id)
        return {
            "id": uuid_from_int_array(raw_node.get("id")) or entry_id,
            "name": str(raw_node.get("name") or "desconocido"),
            "gender": int(raw_node.get("gender") or 0),
            "deceased": bool(raw_node.get("isDeceased")),
            "player": bool(raw_node.get("isPlayer")),
            "profession": str(raw_node.get("profession") or "").replace("minecraft:", ""),
            "father": uuid_from_int_array(raw_node.get("father")),
            "mother": uuid_from_int_array(raw_node.get("mother")),
            "partner": partners[0] if partners else None,
            "partners": partners,
            "relationship": int(raw_node.get("marriageState") or raw_node.get("relationshipState") or 0),
            "children": children,
        }

    def get(self, entry_id: str | None) -> dict[str, Any] | None:
        self.refresh()
        if not entry_id:
            return None
        return self.entries.get(entry_id)

    def entry_count(self) -> int:
        self.refresh()
        return len(self.entries)

    def gender_label(self, node: dict[str, Any]) -> str:
        if node.get("player"):
            return "jugador masculino"
        gender = self.effective_gender(node)
        if gender == 1:
            return "hombre"
        if gender == 2:
            return "mujer"
        return self.GENDERS.get(gender, "genero no especificado")

    def life_status(self, node: dict[str, Any]) -> str:
        gender = self.effective_gender(node)
        if node.get("deceased"):
            if gender == 2:
                return "fallecida"
            return "fallecido"
        if gender == 2:
            return "viva"
        return "vivo"

    def display_name(self, entry_id: str | None, include_life: bool = False) -> str:
        if not entry_id:
            return ""
        node = self.entries.get(entry_id)
        if not node:
            return ""
        suffix = f" ({self.life_status(node)})" if include_life or node["deceased"] else ""
        return str(node["name"]) + suffix

    def add_unique_id(self, values: list[str], entry_id: str | None) -> None:
        if entry_id and entry_id not in values and entry_id in self.entries:
            values.append(entry_id)

    def partner_ids_for(self, node: dict[str, Any]) -> list[str]:
        partners: list[str] = []
        self.add_unique_id(partners, node.get("partner"))
        for partner_id in node.get("partners", []):
            self.add_unique_id(partners, partner_id)
        for entry_id, other in self.entries.items():
            if entry_id == node["id"]:
                continue
            if other.get("partner") == node["id"]:
                self.add_unique_id(partners, entry_id)
            if node["id"] in other.get("partners", []):
                self.add_unique_id(partners, entry_id)
        if not partners and int(node.get("relationship") or 0) in {3, 4}:
            for entry_id, other in self.entries.items():
                if entry_id == node["id"]:
                    continue
                if self.shared_child_ids(node.get("id"), entry_id):
                    self.add_unique_id(partners, entry_id)
        if not partners and int(node.get("relationship") or 0) == 4:
            candidates = [
                entry_id
                for entry_id, other in self.entries.items()
                if entry_id != node["id"] and other.get("player") and int(other.get("relationship") or 0) == 4
            ]
            if len(candidates) == 1:
                self.add_unique_id(partners, candidates[0])
        return partners

    def parent_ids_for(self, node: dict[str, Any]) -> list[str]:
        parents: list[str] = []
        self.add_unique_id(parents, node.get("mother"))
        self.add_unique_id(parents, node.get("father"))
        for entry_id, other in self.entries.items():
            if entry_id == node["id"]:
                continue
            if node["id"] in self.child_ids_for(other, scan_inverse=False):
                self.add_unique_id(parents, entry_id)
        return parents

    def sibling_ids(self, node: dict[str, Any], limit: int) -> list[str]:
        parents = set(self.parent_ids_for(node))
        if not parents:
            return []
        siblings = []
        for entry_id, other in self.entries.items():
            if entry_id == node["id"]:
                continue
            other_parents = set(self.parent_ids_for(other))
            if parents & other_parents:
                siblings.append(entry_id)
                if len(siblings) >= limit:
                    break
        return siblings

    def child_ids_for(self, node: dict[str, Any], scan_inverse: bool = True) -> list[str]:
        children = list(node.get("children", []))
        seen = set(children)
        if scan_inverse:
            for entry_id, other in self.entries.items():
                if entry_id in seen:
                    continue
                if other.get("father") == node["id"] or other.get("mother") == node["id"]:
                    children.append(entry_id)
                    seen.add(entry_id)
        return children

    def labeled_parent_name(self, parent_id: str) -> str:
        parent = self.entries.get(parent_id)
        name = self.display_name(parent_id, include_life=True)
        if not parent or not name:
            return ""
        gender = self.effective_gender(parent)
        if gender == 2:
            return f"madre {name}"
        if gender == 1:
            return f"padre {name}"
        return f"progenitor/a {name}"

    def labeled_partner_name(self, partner_id: str) -> str:
        partner = self.entries.get(partner_id)
        name = self.display_name(partner_id, include_life=True)
        if not partner or not name:
            return ""
        gender = self.effective_gender(partner)
        if gender == 2:
            return f"esposa/pareja {name}"
        if gender == 1:
            return f"esposo/pareja {name}"
        return f"pareja {name}"

    def partner_life_groups_for(self, node: dict[str, Any]) -> tuple[list[str], list[str]]:
        alive: list[str] = []
        deceased: list[str] = []
        for partner_id in self.partner_ids_for(node):
            partner = self.entries.get(partner_id)
            label = self.labeled_partner_name(partner_id)
            if not partner or not label:
                continue
            if partner.get("deceased"):
                deceased.append(label)
            else:
                alive.append(label)
        return alive, deceased

    def co_parent_ids_for(self, node: dict[str, Any]) -> list[str]:
        co_parents: list[str] = []
        own_id = node.get("id")
        child_ids = set(self.child_ids_for(node))
        if not own_id or not child_ids:
            return co_parents
        for entry_id, other in self.entries.items():
            if entry_id == own_id:
                continue
            if child_ids & set(self.child_ids_for(other)):
                self.add_unique_id(co_parents, entry_id)
        return co_parents

    def labeled_co_parent_name(self, co_parent_id: str) -> str:
        co_parent = self.entries.get(co_parent_id)
        name = self.display_name(co_parent_id, include_life=True)
        if not co_parent or not name:
            return ""
        gender = self.effective_gender(co_parent)
        if gender == 2:
            return f"madre/coprogenitora {name}"
        if gender == 1:
            return f"padre/coprogenitor {name}"
        return f"coprogenitor/a {name}"

    def co_parent_summary_for(self, node: dict[str, Any]) -> str:
        co_parents = [self.labeled_co_parent_name(entry_id) for entry_id in self.co_parent_ids_for(node)]
        co_parents = [name for name in co_parents if name]
        if not co_parents:
            return ""
        child_names = [self.display_name(child_id, include_life=True) for child_id in self.child_ids_for(node)]
        child_names = [name for name in child_names if name]
        detail = "Coprogenitor/a registrado/a por hijos compartidos: " + ", ".join(co_parents[:3]) + "."
        if child_names:
            detail += " Hijos compartidos detectados: " + ", ".join(child_names[:4]) + "."
        detail += " Esto prueba lazo familiar por hijos, no necesariamente matrimonio o pareja actual."
        return detail

    def relationship_stats(self) -> dict[str, int]:
        self.refresh()
        parent_rows = 0
        partner_rows = 0
        alive_partner_rows = 0
        deceased_partner_rows = 0
        co_parent_rows = 0
        relationship_state_rows = 0
        for node in self.entries.values():
            if self.parent_ids_for(node):
                parent_rows += 1
            partners = self.partner_ids_for(node)
            if partners:
                partner_rows += 1
                for partner_id in partners:
                    partner = self.entries.get(partner_id)
                    if partner and partner.get("deceased"):
                        deceased_partner_rows += 1
                    elif partner:
                        alive_partner_rows += 1
            if self.co_parent_ids_for(node):
                co_parent_rows += 1
            if int(node.get("relationship") or 0):
                relationship_state_rows += 1
        return {
            "family_parent_rows": parent_rows,
            "family_partner_rows": partner_rows,
            "family_alive_partner_links": alive_partner_rows,
            "family_deceased_partner_links": deceased_partner_rows,
            "family_coparent_rows": co_parent_rows,
            "family_relationship_state_rows": relationship_state_rows,
        }

    def romance_boundary_context(
        self,
        character_id: str | None,
        player_id: str | None,
        last_user: str,
    ) -> str:
        text = normalize_for_match(last_user)
        if not re.search(
            r"\b(coquet|flirt|ligar|seduc|me\s+gustas|te\s+amo|te\s+quiero|bes(?:o|e|arte|ame)|kiss|abrazame|sal\s+conmigo|casate|quiero\s+ser\s+tu\s+(espos[ao]|pareja)|seamos\s+pareja)\b",
            text,
        ):
            return ""
        node = self.get(character_id)
        if not node:
            return ""
        partners = self.partner_ids_for(node)
        alive_partner_ids = [
            partner_id
            for partner_id in partners
            if partner_id in self.entries and not self.entries[partner_id].get("deceased")
        ]
        deceased_partner_ids = [
            partner_id
            for partner_id in partners
            if partner_id in self.entries and self.entries[partner_id].get("deceased")
        ]
        if player_id and player_id in alive_partner_ids:
            return (
                "Limite romantico: el jugador es la pareja/conyuge actual viva del NPC; puede responder con afecto "
                "segun personalidad, corazones, edad y orientacion."
            )
        if alive_partner_ids:
            labels = [self.labeled_partner_name(partner_id) for partner_id in alive_partner_ids]
            labels = [label for label in labels if label]
            return (
                "Limite romantico: el jugador esta coqueteando o intentando romance, pero el NPC tiene pareja/conyuge actual viva registrada: "
                + ", ".join(labels[:2])
                + ". Debe ser leal, rechazar el coqueteo con firmeza y puede mostrarse frio, molesto u hostil si el jugador insiste; no devuelvas besos ni romance."
            )
        if deceased_partner_ids:
            labels = [self.labeled_partner_name(partner_id) for partner_id in deceased_partner_ids]
            labels = [label for label in labels if label]
            return (
                "Limite romantico: el NPC tiene una pareja/conyuge registrada como fallecida: "
                + ", ".join(labels[:2])
                + ". No hables de esa persona como viva; puede responder desde duelo, memoria o incomodidad, sin inventar pareja actual."
            )
        return ""

    def effective_gender(self, node: dict[str, Any]) -> int:
        if node.get("player"):
            return 1
        gender = int(node.get("gender") or 0)
        if gender:
            return gender
        node_id = node.get("id")
        for other in self.entries.values():
            if other.get("mother") == node_id:
                return 2
            if other.get("father") == node_id:
                return 1
        return 0

    def shared_child_ids(self, first_id: str | None, second_id: str | None) -> list[str]:
        first = self.get(first_id)
        second = self.get(second_id)
        if not first or not second:
            return []
        first_children = set(self.child_ids_for(first))
        second_children = set(self.child_ids_for(second))
        shared = [child_id for child_id in first_children & second_children if child_id in self.entries]
        return sorted(shared, key=lambda child_id: self.entries[child_id].get("name", ""))

    def relationship_summary_between(self, first_id: str | None, second_id: str | None) -> str:
        first = self.get(first_id)
        second = self.get(second_id)
        if not first or not second:
            return ""
        facts: list[str] = []
        if second["id"] in self.partner_ids_for(first):
            second_label = self.labeled_partner_name(second["id"]) or self.display_name(second["id"], include_life=True)
            facts.append(f"{first['name']} esta casado/a o en pareja con {second_label}.")
        else:
            partners = [self.labeled_partner_name(partner_id) for partner_id in self.partner_ids_for(first)]
            partners = [partner for partner in partners if partner]
            if partners:
                facts.append(f"Pareja registrada de {first['name']}: " + ", ".join(partners[:2]) + ".")
        shared_children = [self.display_name(child_id, include_life=True) for child_id in self.shared_child_ids(first_id, second_id)]
        shared_children = [name for name in shared_children if name]
        if shared_children:
            facts.append("Hijos compartidos registrados: " + ", ".join(shared_children[:4]) + ".")
        else:
            facts.append("No hay hijos compartidos registrados entre ambos.")
        return " ".join(facts)

    def children_summary_for(self, entry_id: str | None, label: str) -> str:
        self.refresh()
        node = self.get(entry_id)
        if not node:
            return ""
        child_names = [
            self.display_name(child_id, include_life=True) for child_id in self.child_ids_for(node)
        ]
        child_names = [name for name in child_names if name]
        if not child_names:
            return f"{label}: no hay hijos registrados."
        return f"{label}: " + ", ".join(child_names[:4]) + "."

    def child_names_for(self, entry_id: str | None, limit: int = 4) -> list[str]:
        self.refresh()
        node = self.get(entry_id)
        if not node:
            return []
        names = [
            self.display_name(child_id, include_life=False) for child_id in self.child_ids_for(node)
        ]
        return [name for name in names if name][:limit]

    def family_claim_context(self, last_user: str, character_id: str | None, player_id: str | None) -> str:
        text = normalize_for_match(last_user)
        if not text:
            return ""
        mentions_shared_child = bool(
            re.search(r"\b(nuestr[oa]s?\s+hij[oa]s?|nuestro\s+bebe|nuestra\s+bebe)\b", text)
        )
        mentions_player_child = bool(
            re.search(r"\b(mi\s+hij[oa]s?|mis\s+hij[oa]s?|mi\s+bebe|mis\s+bebes)\b", text)
        )
        mentions_villager_child = bool(
            re.search(r"\b(tu\s+hij[oa]s?|tus\s+hij[oa]s?|tu\s+bebe|tus\s+bebes)\b", text)
        )
        mentions_spouse = bool(
            re.search(r"\b(mi\s+espos[ao]|mi\s+marid[oa]|mi\s+mujer|mi\s+pareja|mi\s+conyuge|tu\s+espos[ao]|tu\s+marid[oa]|tu\s+mujer|tu\s+pareja|tu\s+conyuge|somos\s+espos[oa]s?|estamos\s+casad[oa]s?|estas\s+casad[oa]|esta\s+casad[oa])\b", text)
        )
        asks_about_spouse = bool(
            re.search(r"\b(qui[eé]n\s+es\s+tu\s+(espos[ao]|marid[oa]|mujer|pareja)|con\s+qui[eé]n\s+estas\s+casad[oa]|con\s+qui[eé]n\s+esta\s+casad[oa]|que\s+opinas\s+de\s+tu\s+(espos[ao]|marid[oa]|mujer|pareja)|como\s+es\s+tu\s+(espos[ao]|marid[oa]|mujer|pareja)|amas\s+a\s+tu\s+(espos[ao]|marid[oa]|mujer|pareja))\b", text)
        )
        if not asks_about_spouse:
            asks_about_spouse = bool(
                re.search(r"\b(como\s+se\s+llama\s+tu\s+(espos[ao]|marid[oa]|mujer|pareja|conyuge)|quien\s+es\s+tu\s+conyuge|quien\s+es\s+tu\s+marido\s+o\s+esposo)\b", text)
            )
        asks_relationship = bool(
            re.search(r"\b(que\s+relacion\s+(tenemos|tienes\s+conmigo)|que\s+somos|somos\s+algo|me\s+quieres|me\s+odias|te\s+caigo)\b", text)
        )
        mentions_parent = bool(
            re.search(r"\b(tu\s+(madre|mama|padre|papa)|tus\s+(padres|papas)|mi\s+(madre|mama|padre|papa)|mis\s+(padres|papas)|cuales\s+son\s+los\s+nombres\s+de\s+tus\s+padres|como\s+se\s+llaman\s+tus\s+padres|quienes\s+son\s+tus\s+padres)\b", text)
        )
        mentions_memory = bool(
            re.search(r"\b(que\s+recuerdas\s+de\s+mi|te\s+acuerdas\s+de\s+mi|que\s+sabes\s+de\s+mi)\b", text)
        )
        if not any([mentions_shared_child, mentions_player_child, mentions_villager_child, mentions_spouse, asks_about_spouse, asks_relationship, mentions_parent, mentions_memory]):
            return ""
        summary = self.relationship_summary_between(character_id, player_id)
        node = self.get(character_id)
        player_node = self.get(player_id)
        if mentions_player_child:
            summary += " " + self.children_summary_for(player_id, "Hijos del jugador registrados")
            if node and player_node and node["id"] in self.child_ids_for(player_node):
                summary += f" El aldeano actual ({node['name']}) es hijo/a del jugador; llamalo por su nombre real, no por el nombre del jugador."
        if mentions_villager_child:
            summary += " " + self.children_summary_for(character_id, "Hijos del aldeano registrados")
        if mentions_parent and node:
            parent_names = [self.labeled_parent_name(parent_id) for parent_id in self.parent_ids_for(node)]
            parent_names = [name for name in parent_names if name]
            if parent_names:
                summary += " Padres del aldeano: " + ", ".join(parent_names) + "."
            else:
                summary += " Padres del aldeano: no registrados en el arbol familiar cargado."
        if mentions_shared_child and "No hay hijos compartidos" in summary:
            summary += " Si el jugador dice 'nuestro hijo', corrigelo con naturalidad porque no consta en el arbol."
        if (mentions_shared_child or mentions_player_child) and player_node:
            summary += f" El jugador actual se llama {player_node['name']}; no confundas ese nombre con el nombre de sus hijos."
        if asks_about_spouse and ("casado/a o en pareja" in normalize_for_match(summary) or "pareja registrada" in normalize_for_match(summary)):
            summary += " Si pregunta por tu esposo/pareja y el jugador es esa persona, responde con alegria en primera persona, por ejemplo reconociendo 'eres tu'."
        if (mentions_spouse or asks_about_spouse) and "casado/a o en pareja" not in normalize_for_match(summary) and "pareja registrada" not in normalize_for_match(summary):
            co_parent_summary = self.co_parent_summary_for(node) if node else ""
            summary += " Pareja/conyuge del aldeano: no registrada como matrimonio o pareja actual en el arbol familiar cargado."
            if co_parent_summary:
                summary += " " + co_parent_summary
            summary += " Si el jugador afirma matrimonio o pareja, corrigelo con naturalidad porque no consta en el arbol."
        if asks_relationship:
            summary += " Si pregunta por la relacion entre ustedes, combina este arbol familiar con corazones/relacion actual de MCA y no inventes matrimonio si no consta."
        if mentions_memory:
            summary += " Si pregunta que recuerdas de el, usa memoria esencial, lore y vinculos registrados; si falta memoria, admitelo sin inventar hechos concretos."
        return "Verificacion de la afirmacion familiar del jugador: " + summary

    def context_for(self, entry_id: str | None, label: str = "Familia MCA registrada") -> str:
        if not env_bool("MCA_FAMILY_CONTEXT", True):
            return ""
        node = self.get(entry_id)
        if not node:
            return ""

        facts: list[str] = []
        name = node["name"]
        state = self.RELATIONSHIP_STATES.get(node["relationship"], "estado civil desconocido")
        facts.append(f"{name}: {self.gender_label(node)}, {self.life_status(node)}, {state}.")

        alive_partners, deceased_partners = self.partner_life_groups_for(node)
        if alive_partners:
            facts.append("Pareja/conyuge actual viva: " + ", ".join(alive_partners[:2]) + ".")
        elif deceased_partners:
            facts.append("Pareja/conyuge registrada fallecida: " + ", ".join(deceased_partners[:2]) + ".")
        else:
            facts.append("Pareja/conyuge: no registrada en el arbol.")
            co_parent_summary = self.co_parent_summary_for(node)
            if co_parent_summary:
                facts.append(co_parent_summary)

        parents = [
            self.labeled_parent_name(parent_id) for parent_id in self.parent_ids_for(node)
        ]
        parents = [parent for parent in parents if parent]
        if parents:
            facts.append("Padres: " + ", ".join(parents) + ".")

        grandparent_names: list[str] = []
        for parent_id in self.parent_ids_for(node):
            parent = self.entries.get(parent_id or "")
            if not parent:
                continue
            for grandparent_id in self.parent_ids_for(parent):
                grandparent = self.display_name(grandparent_id, include_life=True)
                if grandparent and grandparent not in grandparent_names:
                    grandparent_names.append(grandparent)
        if grandparent_names:
            facts.append("Abuelos/as: " + ", ".join(grandparent_names[:4]) + ".")

        child_names = [
            self.display_name(child_id, include_life=True) for child_id in self.child_ids_for(node)
        ]
        child_names = [child for child in child_names if child]
        if child_names:
            facts.append("Hijos/as: " + ", ".join(child_names[:4]) + ".")
        else:
            facts.append("Hijos/as: no hay registrados en el arbol.")

        sibling_names = [
            self.display_name(sibling_id, include_life=True) for sibling_id in self.sibling_ids(node, 3)
        ]
        sibling_names = [sibling for sibling in sibling_names if sibling]
        if sibling_names:
            facts.append("Hermanos/as o familia cercana: " + ", ".join(sibling_names) + ".")

        if node["deceased"]:
            facts.append("Esta persona figura como fallecida en el arbol familiar.")

        max_facts = max(env_int("MCA_FAMILY_MAX_FACTS", 8), 8)
        return label + ": " + " ".join(facts[:max_facts])


class VillageCache:
    def __init__(self, data_dir: Path) -> None:
        self.path = data_dir / "mca_villages.dat"
        self.modified_at = 0.0
        self.villages: list[dict[str, Any]] = []
        self.by_resident: dict[str, dict[str, Any]] = {}
        self.last_loaded = 0.0

    def refresh(self) -> None:
        if not env_bool("MCA_VILLAGE_CONTEXT", True) or not self.path.exists():
            return
        now = time.time()
        if now - self.last_loaded < 10:
            return
        self.last_loaded = now
        modified_at = self.path.stat().st_mtime
        if modified_at == self.modified_at and self.villages:
            return
        try:
            root = read_nbt_file(self.path)
            data = root.get("data", {})
            raw_villages = data.get("villages", []) if isinstance(data, dict) else []
            villages: list[dict[str, Any]] = []
            by_resident: dict[str, dict[str, Any]] = {}
            for raw_village in raw_villages:
                if not isinstance(raw_village, dict):
                    continue
                resident_names = raw_village.get("residentNames", {})
                if not isinstance(resident_names, dict):
                    resident_names = {}
                reputation = raw_village.get("reputation", {})
                if not isinstance(reputation, dict):
                    reputation = {}
                village = {
                    "id": int(raw_village.get("id") or 0),
                    "name": str(raw_village.get("name") or "aldea sin nombre"),
                    "residents": {str(k): str(v) for k, v in resident_names.items()},
                    "reputation": reputation,
                }
                villages.append(village)
                for resident_id in village["residents"]:
                    by_resident[resident_id] = village
            self.villages = villages
            self.by_resident = by_resident
            self.modified_at = modified_at
        except Exception as exc:
            print(f"[MCA village] No pude leer {self.path.name}: {exc}")

    def village_count(self) -> int:
        self.refresh()
        return len(self.villages)

    def context_for(self, character_id: str | None, player_id: str | None) -> str:
        if not env_bool("MCA_VILLAGE_CONTEXT", True) or not character_id:
            return ""
        self.refresh()
        village = self.by_resident.get(character_id)
        if not village:
            return ""

        residents = village["residents"]
        max_names = env_int("MCA_VILLAGE_MAX_NAMES", 5)
        neighbor_names = [
            name for resident_id, name in residents.items() if resident_id != character_id
        ][:max_names]
        facts = [f"vive en {village['name']}"]
        if neighbor_names:
            facts.append("vecinos conocidos: " + ", ".join(neighbor_names))

        player_reputation = village.get("reputation", {}).get(player_id or "")
        if isinstance(player_reputation, dict):
            score = player_reputation.get(character_id)
            if isinstance(score, int):
                if score >= 1000:
                    facts.append("la confianza local con el jugador es muy alta")
                elif score >= 100:
                    facts.append("la confianza local con el jugador es buena")
                elif score <= -100:
                    facts.append("la confianza local con el jugador esta danada")

        return "Aldea MCA: " + "; ".join(facts) + "."


def compact_text(value: str, limit: int) -> str:
    text = re.sub(r"\s+", " ", value or "").strip()
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3].rstrip() + "..."


def salient_system_context(system_text: str, limit: int) -> str:
    raw = re.sub(r"\s+", " ", system_text or "").strip()
    if not raw:
        return ""

    important: list[str] = []
    tag_text = " ".join(re.findall(r"\[(?:world_id|player_id|character_id):[^\]]+\]", raw))
    if tag_text:
        important.append(tag_text)

    sentence_patterns = [
        r"\bconversation with\b",
        r"\bvillager named\b",
        r"\bPlayer named\b",
        r"\b(?:is|has|hates|dislikes|likes|knows|married|engaged|parent|spouse|child|children|father|mother)\b",
        r"\b(?:profession|job|occupation|oficio|trabajo|trait|mood|personality|hearts|gender|male|female)\b",
        r"\b(?:orientation|sexuality|homosexual|bisexual|asexual|heterosexual|gay|lesbian|straight)\b",
        r"\b(?:disease|condition|illness|sick|infected|diabetes|coeliac|celiac|lactose|albinism|heterochromia|dwarfism)\b",
        r"\b(?:raining|rainy|night|daytime|daylight|morning|afternoon|noon|dawn|dusk|sunrise|sunset|thundering|hurt|injured|pregnant)\b",
        r"\b(?:weather|time of day|hora|noche|dia|lluvia|trueno|amanecer|atardecer|anochecer)\b",
        r"\b(?:gift|gifted|gave|kiss|kissed|hug|hugged|joke|joked|laugh|laughed|interaction|interacted|regalo|regalar|beso|besar|abrazo|abrazar|chiste|broma|risa)\b",
    ]
    profession_aliases = sorted(PROFESSION_ALIASES, key=len, reverse=True)
    for sentence in re.split(r"(?<=[.!?])\s+", raw):
        clean = compact_text(sentence, 260)
        if not clean:
            continue
        text = normalize_for_match(clean)
        if any(re.search(pattern, text) for pattern in sentence_patterns):
            important.append(clean)
            continue
        if any(re.search(rf"\b{re.escape(alias)}\b", text) for alias in profession_aliases):
            important.append(clean)
    deduped = list(dict.fromkeys(important))
    if not deduped:
        return compact_text(raw, limit)
    combined = "Contexto MCA esencial preservado: " + " ".join(deduped) + "\n\nContexto MCA completo recortado: " + raw
    return compact_text(combined, limit)


def normalize_for_match(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value.casefold())
    return "".join(char for char in normalized if not unicodedata.combining(char))


def normalize_identifier_piece(value: str, fallback: str) -> str:
    normalized = normalize_for_match(value)
    normalized = re.sub(r"[:/\\]+", "_", normalized)
    normalized = re.sub(r"[^a-z0-9_ -]+", "", normalized)
    normalized = re.sub(r"[\s-]+", "_", normalized).strip("_")
    return normalized or fallback


def stable_fallback_id(prefix: str, value: str) -> str:
    clean = normalize_identifier_piece(value, prefix)
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:10]
    return f"{prefix}:{clean[:48]}:{digest}"


def apply_fallback_session_ids(
    ids: dict[str, str],
    villager_name: str,
    player_name: str,
    system_text: str,
) -> dict[str, str]:
    fixed = dict(ids)
    if fixed.get("world_id") == "unknown_world":
        world_hint = os.environ.get("MCA_WORLD_ID", "").strip() or "default_world"
        fixed["world_id"] = stable_fallback_id("world", world_hint)
    if fixed.get("player_id") == "unknown_player" and player_name:
        fixed["player_id"] = stable_fallback_id("player", player_name)
    if (
        fixed.get("character_id") == "unknown_character"
        and villager_name
        and env_bool("MCA_ALLOW_NAME_FALLBACK_MEMORY", False)
    ):
        profession = extract_current_profession(system_text) or "unknown_profession"
        fixed["character_id"] = stable_fallback_id("npc", f"{villager_name}:{profession}")
    return fixed


def extract_current_profession(system_text: str) -> str:
    text = normalize_for_match(system_text)
    if not text:
        return ""

    alias_pattern = "|".join(re.escape(alias) for alias in sorted(PROFESSION_ALIASES, key=len, reverse=True))
    profession_ref = rf"{PROFESSION_PREFIX_PATTERN}({alias_pattern})"
    explicit_patterns = [
        rf"\b(?:profession|job|occupation|career|work|oficio|trabajo|profesion)\s*(?:is|=|:|-)?\s*{profession_ref}\b",
        rf"\b(?:is|as|soy|es|como)\s+(?:a|an|un|una)?\s*{profession_ref}\b",
        rf"\b(?:minecraft:|profession[._:-]|mca[._:-]profession[._:-])({alias_pattern})\b",
    ]
    for pattern in explicit_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return PROFESSION_ALIASES.get(match.group(1), "")
    return ""


def current_profession_guidance(system_text: str) -> str:
    profession = extract_current_profession(system_text)
    if not profession:
        return (
            "Oficio actual: MCA no envio un oficio claro para este aldeano. "
            "Si el jugador pregunta a que te dedicas, no inventes una profesion concreta; responde que no tienes puesto claro ahora mismo o habla de una tarea comun de la aldea."
        )
    details = PROFESSION_DETAILS[profession]
    return (
        f"Oficio actual detectado: {details['label']}. "
        f"Cuando el jugador pregunte a que te dedicas, di ese oficio y usa detalles de {details['activities']}. "
        "Este oficio actual manda sobre la identidad persistente y sobre recuerdos antiguos si hay conflicto. "
        "No cambies de oficio por recuerdos antiguos, tareas ajenas o palabras sueltas del contexto."
    )


def detect_age_state(system_text: str) -> str:
    text = normalize_for_match(system_text)
    if re.search(r"\btoddler\b", text):
        return "toddler"
    if re.search(r"\bchild\b", text):
        return "child"
    if re.search(r"\bteen\b", text):
        return "teen"
    return "adulto/no infantil si MCA no indica lo contrario"


def detect_gender_state(system_text: str) -> str:
    text = normalize_for_match(system_text)
    if re.search(r"\b(?:female|woman|girl|mujer|femenin[oa]|aldeana)\b", text):
        return "mujer/femenino"
    if re.search(r"\b(?:male|man|boy|hombre|masculin[oa]|aldeano)\b", text):
        return "hombre/masculino"
    return "no especificado por MCA"


def gender_identity_guidance(system_text: str) -> str:
    gender = detect_gender_state(system_text)
    if gender == "no especificado por MCA":
        return (
            "Genero del NPC actual: MCA no envio genero claro. No inventes genero si el jugador pregunta; "
            "si debes hablar de ti, usa frases neutras o tu nombre."
        )
    return (
        f"Genero del NPC actual: {gender}. Habla de ti con pronombres y adjetivos coherentes con ese genero. "
        "No cambies tu genero por el genero del jugador, por recuerdos viejos ni por el oficio."
    )


def vital_trait_summary(system_text: str) -> str:
    text = normalize_for_match(system_text)
    items: list[str] = []
    checks = [
        (r"\b(lactose[ _-]?intolerance|intoleran\w*\s+a\s+la\s+lactosa)\b", "intolerancia a lactosa"),
        (r"\b(coeliac|celiac|coeliac[ _-]?disease|celiac[ _-]?disease|gluten|celiac[oa])\b", "celiaquia/gluten"),
        (r"\b(diabetes|diabetic[oa]?)\b", "diabetes"),
        (r"\b(dwarfism|dwarf|enanism|enan[oa])\b", "enanismo"),
        (r"\b(albinism|albino|albina|albinismo)\b", "albinismo"),
        (r"\b(heterochromia|heterocromia)\b", "heterocromia"),
        (r"\b(left[ _-]?handed|zurdo|zurda)\b", "zurdo/a"),
        (r"\b(vegetarian|vegetarian[oa])\b", "vegetarianismo"),
        (r"\b(homosexual|gay|lesbian|lesbiana)\b", "orientacion homosexual"),
        (r"\b(bisexual|biromantic)\b", "orientacion bisexual"),
        (r"\b(asexual|aromantic|arromantic[oa])\b", "orientacion asexual/arromantica"),
        (r"\b(heterosexual|straight)\b", "orientacion heterosexual"),
    ]
    for pattern, label in checks:
        if re.search(pattern, text):
            items.append(label)
    if not items:
        return ""
    return "Datos vitales/rasgos detectados: " + ", ".join(dict.fromkeys(items)) + "."


def mood_state_summary(system_text: str) -> str:
    text = normalize_for_match(system_text)
    moods = [
        (r"\b(happy|joyful|cheerful|alegre|feliz|content[oa])\b", "feliz/alegre"),
        (r"\b(sad|depressed|gloomy|triste|melancolic[oa])\b", "triste/melancolico"),
        (r"\b(angry|mad|furious|irate|annoyed|enojad[oa]|furios[oa]|irritad[oa])\b", "enojado/irritado"),
        (r"\b(anxious|nervous|afraid|scared|nervios[oa]|ansios[oa]|asustad[oa])\b", "ansioso/asustado"),
        (r"\b(tired|sleepy|exhausted|cansad[oa]|agotad[oa]|somnolient[oa])\b", "cansado"),
        (r"\b(hurt|injured|sick|herid[oa]|enferm[oa])\b", "herido/enfermo"),
        (r"\b(flirty|coqueto|coqueta|in love|enamorad[oa])\b", "coqueto/enamorado"),
        (r"\b(jealous|celos[oa]|celoso|celosa)\b", "celoso"),
        (r"\b(proud|orgullos[oa])\b", "orgulloso"),
        (r"\b(shy|timid|timid[oa]|vergonzos[oa])\b", "timido"),
    ]
    detected = [label for pattern, label in moods if re.search(pattern, text)]
    if not detected:
        return ""
    return "Estado de animo detectado: " + ", ".join(dict.fromkeys(detected)) + ". Debe notarse en el tono de la respuesta."


def self_awareness_context(
    system_text: str,
    villager_name: str,
    player_name: str,
    ids: dict[str, str],
) -> str:
    profession = extract_current_profession(system_text)
    parts: list[str] = [
        "Conciencia del NPC actual: estos datos describen al aldeano que esta hablando ahora y tienen prioridad sobre memorias antiguas."
    ]
    if villager_name:
        parts.append(f"Nombre propio actual: {villager_name}; no lo repitas como muletilla.")
    if player_name:
        parts.append(f"Jugador actual: {player_name}; tratalo como personaje masculino.")
    if ids.get("character_id") and ids["character_id"] != "unknown_character":
        parts.append(f"ID unico del aldeano: {ids['character_id']}. Usa sus recuerdos como propios, no como recuerdos de otros NPC.")
    else:
        parts.append("MCA no envio ID unico del aldeano; no uses ni guardes recuerdos personales persistentes para evitar mezclar NPCs.")
    if profession:
        details = PROFESSION_DETAILS[profession]
        parts.append(f"Oficio actual: {details['label']} ({details['activities']}).")
    else:
        parts.append("Oficio actual: no confirmado por MCA; no inventes oficio si te preguntan.")
    parts.append(gender_identity_guidance(system_text))
    parts.append(f"Edad/etapa detectada: {detect_age_state(system_text)}.")
    vital_traits = vital_trait_summary(system_text)
    if vital_traits:
        parts.append(vital_traits)
    mood_state = mood_state_summary(system_text)
    if mood_state:
        parts.append(mood_state)
    parts.append(
        "La personalidad, estado de animo, rasgos, relacion y entorno actuales vienen del contexto de MCA; obedecelos antes que cualquier recuerdo."
    )
    state_lines = current_mca_state_lines(system_text, villager_name)
    if state_lines:
        parts.append("Ficha actual textual enviada por MCA: " + " ".join(state_lines))
    return " ".join(parts)


def current_mca_state_lines(system_text: str, villager_name: str) -> list[str]:
    if not system_text:
        return []
    normalized_name = normalize_for_match(villager_name)
    lines: list[str] = []
    for raw in re.split(r"(?<=[.!?])\s+", system_text):
        sentence = compact_text(raw, 220).strip()
        if not sentence:
            continue
        text = normalize_for_match(sentence)
        if sentence.startswith("["):
            continue
        important_state = bool(
            re.search(
                r"\b(gender|male|female|profession|personality|mood|trait|hearts?|orientation|sexuality|"
                r"homosexual|bisexual|asexual|heterosexual|disease|condition|lactose|diabetes|coeliac|celiac|"
                r"spouse|married|engaged|children|child|father|mother|parent)\b",
                text,
            )
        )
        if (
            (normalized_name and normalized_name in text)
            or text.startswith("it is ")
            or "$villager" in text
            or important_state
        ):
            lines.append(sentence)
        if len(lines) >= 12:
            break
    return lines


def user_message_uses_own_name_as_vocative(text: str, villager_name: str) -> bool:
    if not text or not villager_name:
        return False
    commandish = normalize_for_match(text)
    if not re.search(
        r"\b(pegale|pega|golpea|ataca|atacale|mata|matalo|matalos|defiende|ayuda|sigueme|seguime|ven|quedate|espera|ponte|quita|vete|comerci)\b",
        commandish,
    ):
        return False
    for pattern in loose_name_patterns(villager_name):
        if not re.search(rf"\b{pattern}\b", text, flags=re.IGNORECASE):
            continue
        if re.search(rf"\b(?:a|al|contra|hacia|sobre)\s+{pattern}\b", text, flags=re.IGNORECASE):
            return False
        if re.search(rf"^\s*{pattern}\b", text, flags=re.IGNORECASE):
            return True
        if re.search(rf"\b{pattern}\s*[.!?]?\s*$", text, flags=re.IGNORECASE):
            return True
        if re.search(rf"[,;:]\s*{pattern}\b|\b{pattern}\s*[,;:]", text, flags=re.IGNORECASE):
            return True
    return False


def strip_own_name_vocative(text: str, villager_name: str) -> str:
    if not user_message_uses_own_name_as_vocative(text, villager_name):
        return text
    fixed = text
    for pattern in loose_name_patterns(villager_name):
        fixed = re.sub(rf"^\s*{pattern}\s*[,;:\-]?\s*", "", fixed, flags=re.IGNORECASE)
        fixed = re.sub(rf"\s*[,;:\-]?\s*{pattern}\s*[.!?]?\s*$", "", fixed, flags=re.IGNORECASE)
        fixed = re.sub(rf"\s+{pattern}\s+", " ", fixed, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", fixed).strip(" ,;:-") or text


def rewrite_vocative_messages(
    messages: list[dict[str, str]],
    villager_name: str,
) -> tuple[list[dict[str, str]], str, bool]:
    rewritten: list[dict[str, str]] = []
    last_user = ""
    changed = False
    for message in messages:
        copied = dict(message)
        if copied.get("role") == "user":
            original = copied.get("content", "")
            cleaned = strip_own_name_vocative(original, villager_name)
            if cleaned != original:
                copied["content"] = cleaned
                changed = True
            last_user = copied.get("content", "")
        rewritten.append(copied)
    return rewritten, last_user, changed


def self_name_reference_guidance(original_last_user: str, model_last_user: str, villager_name: str) -> str:
    if not villager_name:
        return ""
    if user_message_uses_own_name_as_vocative(original_last_user, villager_name):
        return (
            f"Interpretacion del ultimo mensaje: el jugador uso '{villager_name}' como llamada/vocativo. "
            f"{villager_name} eres tu, no otra persona ni el objetivo. "
            f"Lee '{original_last_user}' como si dijera '{model_last_user}'. "
            "Si era una orden de combate, entiende que quiere que ayudes contra el enemigo visible o la amenaza del contexto; no respondas que no vas a pegarle a ti mismo."
        )
    for pattern in loose_name_patterns(villager_name):
        if re.search(rf"\b(?:a|al|contra|hacia|sobre)\s+{pattern}\b", original_last_user, flags=re.IGNORECASE):
            return (
                f"El ultimo mensaje menciona tu propio nombre como objetivo gramatical. "
                f"Recuerda que {villager_name} eres tu; no lo trates como otro NPC con el mismo nombre."
            )
    return ""


CONSTRUCTION_MEMORY_TERMS = (
    "cantero",
    "mason",
    "piedra",
    "muro",
    "muros",
    "horno",
    "hornos",
    "ladrillo",
    "ladrillos",
    "construir",
    "construccion",
    "construcción",
    "obra",
    "obras",
    "puente",
    "puentes",
    "reparar caminos",
)


def filter_facts_for_current_context(facts: list[str], profession: str) -> list[str]:
    filtered: list[str] = []
    for fact in facts:
        text = normalize_for_match(fact)
        is_assistant_task = text.startswith("el aldeano dijo que iba a hacer esto")
        if not profession and is_assistant_task:
            continue
        if profession != "mason" and any(term in text for term in CONSTRUCTION_MEMORY_TERMS):
            continue
        filtered.append(fact)
    return filtered


def request_debug_snapshot(
    ids: dict[str, str],
    villager_name: str,
    player_name: str,
    system_text: str,
    last_user: str,
) -> dict[str, Any]:
    profession = extract_current_profession(system_text)
    data: dict[str, Any] = {
        "time": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "villager_name": villager_name,
        "player_name": player_name,
        "world_id": ids.get("world_id", ""),
        "player_id": ids.get("player_id", ""),
        "character_id": ids.get("character_id", ""),
        "profession": profession or "",
        "gender": detect_gender_state(system_text),
        "vital_traits": vital_trait_summary(system_text),
        "age_state": detect_age_state(system_text),
        "has_real_character_id": ids.get("character_id") != "unknown_character",
        "last_user_excerpt": compact_text(last_user, 120),
        "system_excerpt": compact_text(sanitize_system_text(system_text), 900)
        if env_bool("MCA_DEBUG_INCLUDE_SYSTEM", True)
        else "",
    }
    return data


def parse_session_ids(system_text: str) -> dict[str, str]:
    ids = {
        "world_id": "unknown_world",
        "player_id": "unknown_player",
        "character_id": "unknown_character",
    }
    for key, value in re.findall(r"\[(world_id|player_id|character_id):([^\]]+)\]", system_text):
        ids[key] = value.strip()
    return ids


def get_messages(payload: dict[str, Any]) -> list[dict[str, Any]]:
    messages = payload.get("messages")
    if isinstance(messages, list):
        return [m for m in messages if isinstance(m, dict)]
    raw_input = payload.get("input")
    if isinstance(raw_input, str):
        return [{"role": "user", "content": raw_input}]
    return []


def content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                text = item.get("text") or item.get("content")
                if isinstance(text, str):
                    parts.append(text)
        return "\n".join(parts)
    return ""


def split_messages(messages: list[dict[str, Any]]) -> tuple[str, list[dict[str, str]], str, str, str]:
    system_parts: list[str] = []
    conversation: list[dict[str, str]] = []
    last_user = ""
    villager_name = ""
    player_name = ""
    system_message_limit = max(env_int("MCA_MAX_SYSTEM_MESSAGE_CHARS", 12000), 12000)
    system_context_limit = max(env_int("MCA_MAX_SYSTEM_CHARS", 6000), 6000)

    for message in messages:
        role = str(message.get("role", "user")).lower()
        raw_text = content_to_text(message.get("content"))
        if role in {"system", "developer"}:
            text = compact_text(raw_text, system_message_limit)
        else:
            text = compact_text(raw_text, env_int("MCA_MAX_INPUT_CHARS", 700))
        if not text:
            continue
        name = str(message.get("name") or "").strip()
        if role == "assistant" and name:
            villager_name = name
        if role == "user" and name:
            player_name = name
        if role in {"system", "developer"}:
            system_parts.append(text)
            continue
        mapped_role = "assistant" if role == "assistant" else "user"
        conversation.append({"role": mapped_role, "content": text})
        if mapped_role == "user":
            last_user = text

    keep_messages = env_int("MCA_CONTEXT_MESSAGES", 1)
    if keep_messages >= 0:
        conversation = conversation[-keep_messages:] if keep_messages else []
    system_text = salient_system_context("\n".join(system_parts), system_context_limit)
    return system_text, conversation, last_user, villager_name, player_name


def extract_names_from_system(system_text: str) -> tuple[str, str]:
    patterns = [
        r"villager named\s+(.+?)\s+and the Player named\s+(.+?)(?:\.|$)",
        r"aldean[oa]\s+llamad[oa]\s+(.+?)\s+y\s+(?:el\s+)?jugador\s+llamad[oa]\s+(.+?)(?:\.|$)",
    ]
    for pattern in patterns:
        match = re.search(pattern, system_text, re.IGNORECASE)
        if match:
            villager = compact_text(match.group(1).strip(" ,;:"), 80)
            player = compact_text(match.group(2).strip(" ,;:"), 80)
            return villager, player
    return "", ""


def sanitize_system_text(system_text: str) -> str:
    text = system_text
    text = re.sub(r"[^.\n]*Mondongo[^.\n]*(?:\.|$)", "", text, flags=re.IGNORECASE)
    text = re.sub(r"[^.\n]*Chanchowapo[^.\n]*(?:\.|$)", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def player_name_rule(player_name: str) -> str:
    masculine_rule = "Todos los players/jugadores son personajes masculinos: el jugador actual es jugador masculino; usa el jugador, el, esposo, padre o hijo cuando corresponda; no los trates en femenino aunque el arbol no traiga genero."
    if player_name.lower() == "chanchowapo":
        return masculine_rule + " El jugador se llama Chanchowapo; puedes usar Mondongo solo si el contexto lo amerita o el jugador lo pide. No digas su nombre/apodo como muletilla. Usa los nombres familiares exactos del arbol; no pongas el nombre del jugador a sus hijos."
    if player_name:
        return masculine_rule + f" El jugador se llama {player_name}. No digas su nombre como muletilla y no lo llames Mondongo. Usa su nombre solo si el jugador lo pide, si corriges identidad/familia/lore o si es necesario para evitar confusion. Usa los nombres familiares exactos del arbol; no pongas el nombre del jugador a sus hijos."
    return masculine_rule + " No llames Mondongo al jugador salvo si su nombre exacto es Chanchowapo. No digas el nombre del jugador como muletilla. Usa los nombres familiares exactos del arbol."


def extract_mca_commands(system_text: str) -> dict[str, str]:
    commands: dict[str, str] = dict(KNOWN_MCA_COMMANDS)
    pattern = r"\*\s*([a-z0-9-]+)\s*:\s*(.*?)(?=\s+\*\s*[a-z0-9-]+\s*:|$)"
    for command, description in re.findall(pattern, system_text, re.IGNORECASE | re.DOTALL):
        commands[command.strip()] = compact_text(description.strip(), 120)
    return commands


def detect_requested_command(last_user: str, commands: dict[str, str] | None = None) -> str:
    text = normalize_for_match(last_user)
    command_source = commands or KNOWN_MCA_COMMANDS
    direct_checks = [
        ("follow-player", r"\b(sigueme|sigue me|seguime|ven conmigo|acompaname|camina conmigo|follow me)\b"),
        ("stay-here", r"\b(quedate|quiet[ao]|estate quiet[ao]|espera|esperame|espera aqui|no te muevas|no camines|stay here|wait here)\b"),
        ("move-freely", r"\b(puedes irte|vete|ya puedes moverte|deja de seguirme|sigue con lo tuyo|move freely|go away)\b"),
        ("wear-armor", r"\b(ponte armadura|equipa armadura|usa armadura|wear armor)\b"),
        ("remove-armor", r"\b(quitate la armadura|remove armor)\b"),
        ("try-go-home", r"\b(vete a casa|ve a casa|regresa a casa|go home)\b"),
        ("open-trade-window", r"\b(comerci|trade|precios|intercambi|inventario)\b"),
    ]
    for command, pattern in direct_checks:
        if command in command_source and re.search(pattern, text, re.IGNORECASE):
            return command
    checks = [
        ("follow-player", r"\b(sigueme|sigue me|sígueme|ven conmigo|acompaname|acompáñame|follow me)\b"),
        ("stay-here", r"\b(quedate|quédate|espera aqui|espera aquí|no te muevas|stay here|wait here)\b"),
        ("move-freely", r"\b(puedes irte|vete|ya puedes moverte|deja de seguirme|move freely|go away)\b"),
        ("wear-armor", r"\b(ponte armadura|equipa armadura|usa armadura|wear armor)\b"),
        ("remove-armor", r"\b(quitate la armadura|quítate la armadura|remove armor)\b"),
        ("try-go-home", r"\b(vete a casa|ve a casa|regresa a casa|go home)\b"),
        ("open-trade-window", r"\b(comerci|trade|precios|intercambi|inventario)\b"),
    ]
    for command, pattern in checks:
        if command in command_source and re.search(pattern, text, re.IGNORECASE):
            return command
    return ""


def command_instructions(commands: dict[str, str], requested_command: str) -> str:
    if not commands:
        return ""
    listed = "; ".join(f"{command}: {description}" for command, description in commands.items())
    parts = [
        "Comandos MCA disponibles: " + listed,
        "Si el jugador da una orden simple y el comando existe, obedece usando optionalCommand en JSON.",
        "Puedes negarte si el contexto dice que no hay confianza, estas herido, de mal humor o la orden es peligrosa; si te niegas, deja optionalCommand vacio.",
    ]
    if requested_command:
        parts.append(f"La orden actual parece corresponder a optionalCommand={requested_command}.")
    return " ".join(parts)


def is_direct_command(last_user: str, requested_command: str) -> bool:
    if not requested_command:
        return False
    text = normalize_for_match(last_user)
    if len(text) > env_int("MCA_DIRECT_COMMAND_MAX_CHARS", 90):
        return False
    patterns = {
        "follow-player": r"\b(sigueme|sigue me|seguime|ven conmigo|acompaname|camina conmigo|follow me)\b",
        "stay-here": r"\b(quedate|quiet[ao]|estate quiet[ao]|espera|esperame|espera aqui|no te muevas|no camines|stay here|wait here)\b",
        "move-freely": r"\b(puedes irte|vete|deja de seguirme|sigue con lo tuyo|move freely|go away)\b",
        "wear-armor": r"\b(ponte armadura|equipa armadura|usa armadura|wear armor)\b",
        "remove-armor": r"\b(quitate la armadura|remove armor)\b",
        "try-go-home": r"\b(vete a casa|ve a casa|regresa a casa|go home)\b",
        "open-trade-window": r"\b(comerci|trade|precios|intercambi|inventario)\b",
    }
    return bool(re.search(patterns.get(requested_command, r"$^"), text, re.IGNORECASE))


def local_command_reply(command: str) -> str:
    replies = {
        "follow-player": "Vale, voy contigo. Pero camina claro, que no pienso perseguir sombras.",
        "stay-here": "Esta bien, me quedo aqui. No tardes si esto importa.",
        "move-freely": "Perfecto, entonces me muevo a mi aire. Ya era hora de estirar las piernas.",
        "wear-armor": "Me pongo la armadura. Si hay pelea, prefiero no recibirla con la cara.",
        "remove-armor": "Me quito la armadura. Mas te vale que no sea una mala idea.",
        "try-go-home": "Voy a intentar volver a casa. Si el camino esta libre, llegare.",
        "open-trade-window": "Mira mis tratos y no me hagas perder tiempo si solo venias a curiosear.",
    }
    return replies.get(command, "Hecho.")


def find_character_profile(system_text: str, villager_name: str, profiles: dict[str, str]) -> str:
    if not profiles:
        return ""
    if villager_name and villager_name.lower() in profiles:
        return profiles[villager_name.lower()]
    lowered_system = system_text.lower()
    for name, profile in profiles.items():
        if re.search(rf"(?<!\w){re.escape(name)}(?!\w)", lowered_system):
            return profile
    return ""


def personality_guidance(system_text: str) -> str:
    text = normalize_for_match(system_text)
    hints: list[str] = []
    if "crabby" in text:
        hints.append("crabby: seco, grunon, cortante; puede mandar al jugador con el vecino o soltar groserias comunes.")
    if "greedy" in text:
        hints.append("greedy: interesado y oportunista; piensa en favores, regalos y ganancia.")
    if "gloomy" in text or "sensitive" in text or "anxious" in text:
        hints.append("gloomy/sensitive/anxious: vulnerable, sensible, inseguro; responde mas suave, sumiso o herido.")
    if "flirty" in text or "coquet" in text:
        relation = relationship_temperature_guidance(system_text)
        hints.append(
            "flirty/coqueta: que se note en voz, picardia y confianza corporal; no lo reduzcas a oficio ni a orden. "
            + (relation or "Con vinculo bajo coquetea ligero y con limites; con vinculo alto puede ser mas dulce y cercana.")
        )
    if "friendly" in text or "upbeat" in text or "extroverted" in text or "playful" in text:
        hints.append("friendly/upbeat/playful: abierto, expresivo y con humor.")
    if "introverted" in text or "relaxed" in text or "peaceful" in text:
        hints.append("introverted/relaxed/peaceful: tranquilo, sobrio y poco invasivo.")
    if "odd" in text:
        hints.append("odd: raro, impredecible y con asociaciones extranas pero entendibles.")
    if "hates" in text or "dislikes" in text:
        hints.append("relacion mala: no seas servicial; muestra distancia, fastidio o desconfianza.")
    if (
        "married to" in text
        or "in love with" in text
        or "engaged with" in text
        or ("likes" in text and "really well" in text)
    ):
        hints.append("vinculo fuerte: prioriza carino, lealtad y proteccion antes que brusquedad.")
    relation = relationship_temperature_guidance(system_text)
    if relation and not any(relation in hint for hint in hints):
        hints.append(relation)
    if not hints:
        return ""
    return "Guia de personalidad detectada: " + " ".join(hints[:3])


def life_stage_world_guidance(system_text: str) -> str:
    text = normalize_for_match(system_text)
    hints: list[str] = []
    if "toddler" in text:
        hints.append("edad toddler: habla simple, curioso y dependiente; nada de romance, coqueteo ni insultos adultos.")
    elif "child" in text:
        hints.append("edad child: tono infantil o inocente; nada de romance, coqueteo ni groserias fuertes.")
    elif "teen" in text:
        hints.append("edad teen: mas impulsivo, orgulloso o inseguro; evita romance adulto y respuestas demasiado maduras.")
    else:
        hints.append("edad adulta si MCA no marca child/teen/toddler: puede tratar romance, matrimonio, oficio y responsabilidades adultas.")

    profession = extract_current_profession(system_text)
    if profession:
        details = PROFESSION_DETAILS[profession]
        hints.append(f"oficio {details['label']}: piensa en {details['activities']}.")

    hints.extend(world_time_weather_hints(system_text))
    if len(hints) <= 1 and "minecraft" not in text:
        return ""
    return "Guia de edad/oficio/entorno: " + " ".join(hints[:6])


def trait_mood_guidance(system_text: str) -> str:
    text = normalize_for_match(system_text)
    hints: list[str] = []
    if re.search(r"\b(color\s*blind|colour\s*blind|colorblind|daltonic[oa]?|daltonismo)\b", text):
        hints.append("daltonismo: puede confundir colores; fijate en formas, textura, brillo o posicion antes que en color.")
    if re.search(r"\b(athletic|athlete|fit|strong|fuerte|atletic[oa]|deportista)\b", text):
        hints.append("atletico/fuerte: energia fisica, postura segura, habla de correr, cargar, entrenar o resistir.")
    if re.search(r"\b(clumsy|torpe|awkward)\b", text):
        hints.append("torpe: puede tropezar, dudar con herramientas o bromear de sus propios errores.")
    if re.search(r"\b(smart|intelligent|clever|genius|list[oa]|inteligente)\b", text):
        hints.append("inteligente: responde con observaciones practicas, planes o ironia precisa.")
    if re.search(r"\b(lazy|perezos[oa]|flojo|floja)\b", text):
        hints.append("perezoso: busca excusas, quiere ahorrar esfuerzo y negocia antes de ayudar.")
    if re.search(r"\b(brave|valiente|bold)\b", text):
        hints.append("valiente: protege antes de quejarse y acepta peligro con firmeza.")
    if re.search(r"\b(coward|cobarde|fearful)\b", text):
        hints.append("miedoso: duda ante monstruos, pide apoyo o prefiere refugio.")
    trait_hints = [
        (r"\b(lactose[ _-]?intolerance|intoleran\w*\s+a\s+la\s+lactosa)\b", "intolerancia a lactosa: evita leche, queso o bromas de comida lactea como si fueran mala idea."),
        (r"\b(coeliac|celiac|coeliac[ _-]?disease|celiac[ _-]?disease|gluten|celiac[oa])\b", "celiaquia/gluten: cuida lo que come y evita pan o trigo si el tema sale."),
        (r"\b(diabetes|diabetic[oa]?)\b", "diabetes: habla con cuidado de azucar, cansancio o cuidados de salud sin dramatizar."),
        (r"\b(dwarfism|dwarf|enanism|enan[oa])\b", "enanismo: reconoce su estatura como rasgo propio si importa; no lo conviertas en chiste."),
        (r"\b(albinism|albino|albina|albinismo)\b", "albinismo: piel/cabello claros y sensibilidad al sol si encaja con la escena."),
        (r"\b(heterochromia|heterocromia)\b", "heterocromia: tiene ojos de distinto color; puede mencionarlo si hablan de apariencia."),
        (r"\b(left[ _-]?handed|zurdo|zurda)\b", "zurdo: usa la mano izquierda como costumbre propia en gestos o trabajo."),
        (r"\b(electrified|electrico|electrica|electrizad[oa])\b", "electrificado: personalidad o cuerpo con energia inquieta; evita tocarlo como si fuera normal si el rasgo es literal."),
        (r"\b(tough|resilient|duro|dura|resistente)\b", "resistente: soporta dolor, trabajo duro o peligro con mas firmeza."),
        (r"\b(weak|debil|fragil)\b", "debil/fragil: evita esfuerzos grandes y pide apoyo antes de exponerse."),
        (r"\b(vegetarian|vegetarian[oa])\b", "vegetariano: evita carne y puede hablar de comida vegetal o animales con mas cuidado."),
    ]
    for pattern, hint in trait_hints:
        if re.search(pattern, text):
            hints.append(hint)
            if len(hints) >= 6:
                break
    if re.search(r"\b(homosexual|gay|lesbian|lesbiana)\b", text):
        hints.append("orientacion homosexual: su atraccion romantica va hacia su mismo genero; expresalo con naturalidad si el romance sale.")
    elif re.search(r"\b(bisexual|biromantic|bisexual)\b", text):
        hints.append("orientacion bisexual: puede sentir atraccion romantica por mas de un genero; no asumas rechazo automatico.")
    elif re.search(r"\b(asexual|aromantic|asexual|arromantic[oa])\b", text):
        hints.append("orientacion asexual/arromantica: puede ser afectuoso sin querer coqueteo o romance.")
    elif re.search(r"\b(heterosexual|straight)\b", text):
        hints.append("orientacion heterosexual: su atraccion romantica va hacia otro genero; respetalo si el romance sale.")

    mood_hints = [
        (r"\b(happy|joyful|cheerful|alegre|feliz|content[oa])\b", "animo alegre: mas calido, expresivo y dispuesto a bromear o ayudar."),
        (r"\b(sad|depressed|gloomy|triste|melancolic[oa])\b", "animo triste: voz baja, vulnerable, menos energia y mas necesidad de cuidado."),
        (r"\b(angry|mad|furious|irate|annoyed|enojad[oa]|furios[oa]|irritad[oa])\b", "animo enojado: frases cortantes, impacientes y con limites claros."),
        (r"\b(anxious|nervous|afraid|scared|nervios[oa]|ansios[oa]|asustad[oa])\b", "animo ansioso/asustado: cautela, dudas y atencion al peligro cercano."),
        (r"\b(tired|sleepy|exhausted|cansad[oa]|agotad[oa]|somnolient[oa])\b", "animo cansado: menos paciencia, ganas de sentarse, dormir o terminar rapido."),
        (r"\b(hurt|injured|sick|herid[oa]|enferm[oa])\b", "estado herido/enfermo: prioriza descanso, dolor, cuidado y puede negarse a riesgos."),
        (r"\b(flirty|coqueto|coqueta|in love|enamorad[oa])\b", "animo coqueto/enamorado: mas cercania y dulzura si el vinculo y orientacion encajan."),
        (r"\b(jealous|celos[oa]|celoso|celosa)\b", "celos: puede sonar posesivo, herido o desconfiado sin exagerar."),
        (r"\b(proud|orgullos[oa])\b", "orgullo: defiende su oficio, familia o reputacion con seguridad."),
        (r"\b(shy|timid|timid[oa]|vergonzos[oa])\b", "timidez: evita ser directo, baja el tono y tarda en aceptar afecto."),
    ]
    for pattern, hint in mood_hints:
        if re.search(pattern, text):
            hints.append(hint)
            if len(hints) >= 6:
                break
    if not hints:
        return ""
    return "Guia de rasgos y estado actual: " + " ".join(hints[:6])


def relationship_roleplay_guidance(family_context: str, system_text: str, player_name: str) -> str:
    text = normalize_for_match(family_context + " " + system_text)
    player = normalize_for_match(player_name)
    spouse_hint = bool(
        player
        and (("esposo/pareja " + player) in text or ("esposa/pareja " + player) in text)
    )
    if not spouse_hint:
        spouse_hint = "married to" in text or "in love with" in text or "engaged with" in text
    has_children = "hijos/as:" in normalize_for_match(family_context)
    if not spouse_hint and not has_children:
        return ""
    parts: list[str] = []
    if spouse_hint:
        parts.append(
            "Si eres conyuge o pareja del jugador, se mas cercano, meloso o protector segun tu personalidad; no lo trates como desconocido."
        )
    if has_children:
        parts.append(
            "Habla de tus hijos con afecto, preocupacion y orgullo cuando salgan en la conversacion; no los ignores."
        )
    return "Vinculos de rol: " + " ".join(parts)


def current_player_relationship_guidance(system_text: str, family_context: str, player_name: str) -> str:
    parts: list[str] = []
    relation = relationship_temperature_guidance(system_text)
    if relation:
        parts.append(relation)
    text = normalize_for_match(family_context + " " + system_text)
    player = normalize_for_match(player_name)
    if player and ("esposo/pareja " + player) in text:
        parts.append("El jugador actual figura como esposo/pareja del NPC; reconoce esa cercania si el tema sale.")
    elif player and "pareja/conyuge: no registrada" in text:
        parts.append("No hay pareja registrada para este NPC en el arbol; no inventes esposo/esposa.")
    if "hijos compartidos registrados" in text:
        parts.append("Hay hijos compartidos registrados; si hablan de familia, trata esos hijos como reales y usa sus nombres.")
    if not parts:
        return ""
    return "Relacion actual con el jugador: " + " ".join(parts)


def relationship_score_from_system(system_text: str) -> int | None:
    text = normalize_for_match(system_text)
    patterns = [
        r"(?<!\w)(-?\d+)\s*(?:hearts?|corazones?)\b",
        r"\b(?:hearts?|corazones?)\s*(?:is|=|:)?\s*(-?\d+)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None
    if "hates" in text or "dislikes" in text or "odia" in text:
        return -25
    if "likes" in text and "really well" in text:
        return 80
    if "married to" in text or "engaged with" in text or "in love with" in text:
        return 100
    return None


def relationship_temperature_guidance(system_text: str) -> str:
    score = relationship_score_from_system(system_text)
    text = normalize_for_match(system_text)
    if score is not None:
        if score < 0:
            return "Relacion actual: corazones negativos o rechazo; muestra distancia, filo o desconfianza aunque seas amable o coqueta."
        if score < 25:
            return "Relacion actual: confianza baja; puedes ser jugueton o educado, pero no actues como pareja ni como amigo intimo."
        if score >= 80:
            return "Relacion actual: confianza muy alta; permite mas carino, complicidad y proteccion si la personalidad lo permite."
    if "hates" in text or "dislikes" in text:
        return "Relacion actual: mala; no seas servicial por defecto y deja notar fastidio o distancia."
    return ""


def world_time_weather_hints(system_text: str) -> list[str]:
    text = normalize_for_match(system_text)
    hints: list[str] = []
    if re.search(r"\b(it is (?:currently )?night|nighttime|midnight|de noche|es noche|la noche)\b", text):
        hints.append("entorno de noche: puedes mencionar antorchas, cama, patrulla o mobs con cautela.")
    elif re.search(r"\b(dusk|sunset|atardecer|anochecer)\b", text):
        hints.append("entorno de atardecer/anochecer: puedes notar que baja la luz y conviene prepararse.")
    elif re.search(
        r"\b(it is (?:currently )?day|it is daytime|daytime|daylight|morning|afternoon|noon|sunrise|dawn|sunny|de dia|es de dia|amanecer|mediodia)\b",
        text,
    ):
        hints.append("entorno de dia: actua como si hay luz normal; no hables de dormir o mobs nocturnos salvo que el contexto los mencione.")
    if re.search(r"\b(it is raining|raining|rainy|rain|llueve|lluvia)\b", text):
        hints.append("entorno con lluvia: puedes mencionar refugio, barro, techo o herramientas mojadas.")
    if re.search(r"\b(it is thundering|thundering|thunder|storm|trueno|tormenta)\b", text):
        hints.append("entorno con trueno: puedes mostrar nervios, urgencia o prudencia.")
    return hints


def response_focus_context(last_user: str, system_text: str) -> str:
    text = normalize_for_match(last_user)
    focus_parts: list[str] = []
    if re.search(r"\b(personalidad|caracter|como\s+eres|que\s+tipo\s+de\s+persona|rasgos?)\b", text):
        relation = relationship_temperature_guidance(system_text)
        focus_parts.append(
            "Enfoque de respuesta: el jugador pregunta por tu personalidad o caracter. "
            "Responde desde tu personalidad, estado de animo, rasgos y relacion actual; el oficio solo puede ser un detalle secundario. "
            "Si MCA indica flirty/coqueta/coqueto, debe sentirse en el tono: jugueton, directo y con encanto. "
            "Si la relacion es baja o negativa, ese coqueteo debe sonar distante, mordaz o provocador, no entregado ni romantico. "
            + relation
        )
    if re.search(r"\b(genero|hombre|mujer|masculino|femenino|eres\s+chico|eres\s+chica)\b", text):
        focus_parts.append(
            "Enfoque de respuesta: el jugador pregunta por tu genero. Responde con el genero actual detectado por MCA; no respondas desde tu oficio."
        )
    if re.search(r"\b(orientacion|sexualidad|gay|lesbiana|bisexual|asexual|heterosexual|te\s+gustan|atraen)\b", text):
        focus_parts.append(
            "Enfoque de respuesta: el jugador pregunta por orientacion romantica/sexual. Usa solo la orientacion detectada por MCA si existe; si no existe, di que no lo tienes claro sin inventar."
        )
    if re.search(r"\b(enfermedad|padecimiento|condicion|condicion|salud|diabetes|lactosa|gluten|celiac|celiaquia|rasgo)\b", text):
        focus_parts.append(
            "Enfoque de respuesta: el jugador pregunta por padecimientos, condiciones o rasgos. Usa los rasgos detectados en MCA; no los conviertas en oficio ni los ignores."
        )
    if re.search(r"\b(familia|arbol|genealogic|hij[oa]s?|espos[ao]|marid[oa]|conyuge|pareja|padres?|madres?|papas?|madre|herman[oa]|abuel[oa])\b", text):
        focus_parts.append(
            "Enfoque de respuesta: el jugador pregunta por familia. Usa el arbol genealogico cargado, distingue vivos/fallecidos y corrige con naturalidad si el jugador inventa parentescos. Si no hay datos familiares cargados en el contexto, no inventes hijos, padres ni pareja."
        )
    if re.search(r"\b(animo|estado\s+de\s+animo|feliz|triste|enojad[oa]|cansad[oa]|como\s+te\s+sientes|estas\s+bien)\b", text):
        focus_parts.append(
            "Enfoque de respuesta: el jugador pregunta por tu estado de animo. Responde desde el animo actual detectado por MCA y deja que afecte tu tono."
        )
    if re.search(r"\b(relacion|corazones|confianza|amistad|me\s+quieres|me\s+odias|te\s+caigo|que\s+somos)\b", text):
        focus_parts.append(
            "Enfoque de respuesta: el jugador pregunta por la relacion entre ustedes. Usa corazones, confianza, matrimonio/familia y memoria compartida con este mismo aldeano; no inventes una relacion mas cercana de la que exista."
        )
    return " ".join(part.strip() for part in focus_parts if part.strip())


def memory_question_context(last_user: str) -> str:
    text = normalize_for_match(last_user)
    if re.search(
        r"\b(que\s+recuerdas\s+de\s+mi|te\s+acuerdas\s+de\s+mi|que\s+sabes\s+de\s+mi|que\s+fue\s+lo\s+ultimo\s+que\s+te\s+(dije|mencione|pregunte|conte)|que\s+fue\s+lo\s+.ltimo\s+que\s+te\s+(dije|mencione|pregunte|conte)|recuerdas\s+lo\s+ultimo|recuerdas\s+lo\s+.ltimo|te\s+acuerdas\s+de\s+lo\s+ultimo|te\s+acuerdas\s+de\s+lo\s+.ltimo|de\s+que\s+hablamos)\b",
        text,
    ):
        return (
            "El jugador pregunta que recuerdas o que fue lo ultimo que hablaron: responde con 1-3 recuerdos reales tomados de memoria, "
            "conversacion reciente, lore o familia. Si hay una propuesta romantica, cita, beso o acuerdo reciente, mencionalo con claridad. "
            "Si falta memoria, admitelo sin inventar hechos concretos."
        )
    return ""


def recent_turns_context(turns: list[tuple[str, str]]) -> str:
    if not turns:
        return ""
    lines: list[str] = []
    for role, content in turns[-env_int("MCA_RECENT_TURN_CONTEXT", 4) :]:
        label = "Jugador" if role == "user" else "Aldeano"
        lines.append(f"{label}: {compact_text(content, 180)}")
    return (
        "Conversacion reciente recordada con este mismo aldeano y jugador. "
        "Usala como memoria de continuidad si encaja, sin recitarla completa:\n"
        + "\n".join(lines)
    )


def detected_player_interactions(text: str) -> list[tuple[str, str, int]]:
    match_text = normalize_for_match(text)
    specs: list[tuple[str, str, str, int]] = [
        (
            "regalo",
            r"\b(regal(?:o|e|aste|ado|ar)|gift(?:ed)?|gave (?:you|him|her)|te di|le di|me diste|ofreci(?:o|ste|endo))\b",
            "el jugador le regalo u ofrecio algo al aldeano",
            9,
        ),
        (
            "beso",
            r"\b(bes(?:o|e|aste|ado|ar)|kiss(?:ed)?|smooch)\b",
            "hubo o intento un beso con el jugador",
            9,
        ),
        (
            "abrazo",
            r"\b(abraz(?:o|e|aste|ado|ar)|hug(?:ged)?)\b",
            "hubo o intento un abrazo con el jugador",
            8,
        ),
        (
            "chiste",
            r"\b(chiste|broma|brome(?:e|aste|amos|o)|joke(?:d)?|laugh(?:ed)?|reimos|risa|gracios[oa])\b",
            "el jugador conto un chiste o hizo una broma",
            8,
        ),
    ]
    events: list[tuple[str, str, int]] = []
    for label, pattern, description, weight in specs:
        if re.search(pattern, match_text, re.IGNORECASE):
            events.append((label, description, weight))
    return events


def recent_interaction_context(last_user: str, system_text: str) -> str:
    source = compact_text(" ".join(part for part in [last_user, system_text] if part), 260)
    events = detected_player_interactions(source)
    if not events:
        return ""
    descriptions = "; ".join(description for _label, description, _weight in events)
    return (
        "Interaccion actual/reciente detectada por MCA: "
        + descriptions
        + ". Reacciona ahora a esa interaccion con continuidad; si es regalo agradece o valora segun relacion, "
        + "si es beso/abrazo respeta edad, orientacion, consentimiento y corazones, y si es chiste responde con humor o incomodidad segun tu personalidad."
    )


def extract_recent_interaction_facts(
    user_text: str,
    assistant_text: str = "",
    system_text: str = "",
) -> list[tuple[str, int]]:
    assistant_clean = assistant_message_text(assistant_text)
    source = compact_text(" ".join(part for part in [user_text, assistant_clean, system_text] if part), 200)
    events = detected_player_interactions(source)
    facts: list[tuple[str, int]] = []
    for label, description, weight in events:
        facts.append(
            (
                f"Interaccion con este jugador ({label}): {description}. Reaccionar en futuras charlas segun relacion, personalidad, corazones y contexto. Detalle: {source}",
                weight,
            )
        )
    return facts


def extract_romance_memory_facts(user_text: str, assistant_text: str) -> list[tuple[str, int]]:
    user_clean = compact_text(user_text, 260)
    if not user_clean:
        return []
    user_match = normalize_for_match(user_clean)
    proposal_pattern = (
        r"\b("
        r"saldrias\s+conmigo|saldr.?as\s+conmigo|saldria\s+conmigo|saldr.?a\s+conmigo|saldras\s+conmigo|salir\s+conmigo|quieres\s+salir\s+conmigo|"
        r"te\s+gustaria\s+salir\s+conmigo|aceptas\s+salir\s+conmigo|salgamos|"
        r"tener\s+una\s+cita|ir\s+a\s+una\s+cita|una\s+cita\s+conmigo|date\s+with\s+me|"
        r"se\s+mi\s+novi[ao]|quieres\s+ser\s+mi\s+novi[ao]|seamos\s+pareja|"
        r"quiero\s+ser\s+tu\s+pareja|quiero\s+ser\s+tu\s+espos[ao]|casate\s+conmigo"
        r")\b"
    )
    if not re.search(proposal_pattern, user_match, re.IGNORECASE):
        return []

    assistant_clean = compact_text(assistant_message_text(assistant_text), 220)
    assistant_match = normalize_for_match(assistant_clean)
    refused = bool(
        re.search(
            r"\b(no|no\s+puedo|no\s+quiero|rechazo|no\s+estaria\s+bien|tengo\s+pareja|estoy\s+casad[oa]|mi\s+espos[ao]|mi\s+marid[oa]|mi\s+mujer)\b",
            assistant_match,
        )
    )
    accepted = bool(
        re.search(
            r"\b(si|s.?|claro|acepto|me\s+encantaria|saldria\s+contigo|saldr.?a\s+contigo|salgamos|quiero|vale|de\s+acuerdo|me\s+gustaria)\b",
            assistant_match,
        )
    ) and not refused

    if accepted:
        result = "el aldeano acepto o mostro interes en salir/tener una cita con el jugador"
    elif refused:
        result = "el aldeano rechazo o puso limites ante la propuesta romantica del jugador"
    else:
        result = "el jugador hizo una propuesta romantica o de cita; la respuesta exacta debe recordarse desde la conversacion reciente si esta disponible"
    detail = f"Jugador: {user_clean}"
    if assistant_clean:
        detail += f" | Aldeano: {assistant_clean}"
    return [(f"Recuerdo romantico con este jugador: {result}. {detail}", 10)]


def extract_important_facts(user_text: str, assistant_text: str) -> list[tuple[str, int]]:
    text = compact_text(user_text, 260)
    if not text:
        return []
    match_text = normalize_for_match(text)
    patterns: list[tuple[str, int]] = [
        (r"\b(recuerda|recuerdame|no olvides|acuerdate)\b", 10),
        (r"\b(me llamo|mi nombre es|me gusta|odio|amo|tengo miedo|prefiero)\b", 8),
        (r"\b(te amo|te quiero|bes[eoé]|abrazo|casad[oa]|espos[ao]|novi[ao]|prometid[ao]|enamorad[oa]|anillo|boda)\b", 7),
        (r"\b(saldrias\s+conmigo|saldr.?as\s+conmigo|salir\s+conmigo|quieres\s+salir\s+conmigo|cita\s+conmigo|seamos\s+pareja|se\s+mi\s+novi[ao]|casate\s+conmigo)\b", 10),
        (r"\b(regalo|te di|me diste|diamante|flor|vino|taberna|tesoro)\b", 5),
        (r"\b(chiste|broma|bromee|bromeamos|reimos|reir|risa|gracios[oa]|joke)\b", 7),
        (r"\b(perdon|perd[oó]n|pelea|golpe|traicion|salvaste|rescataste|promet[ií])\b", 6),
        (r"\b(isla|barco|naufragio|ruina|kraken|leviathan|leviat[aá]n|oceano|oc[eé]ano)\b", 4),
    ]
    patterns.extend(
        [
            (r"\b(soy|yo soy|trabajo como|soy el|soy la|constructor|arquitecto|cantina|bar|minero|mina)\b", 8),
            (r"\b(nuestro hij[oa]|nuestra hij[ao]|nuestros hijos|nuestras hijas)\b", 7),
            (r"\b(hable|hablamos|platique|platicamos|dije|me dijiste|carmen|jenner|kainolimits|chanchowapo|pan)\b", 6),
            (r"\b(posole|pozole|jojoposes|jojo\s*poses|reynas?\s+del\s+misisipi|misisipi|musica|morado|langosta|chalan|trueno|social\s+wars|pokemones|taticoso|milaneso|4k)\b", 7),
        ]
    )
    for pattern, weight in patterns:
        if re.search(pattern, match_text, re.IGNORECASE):
            return [(f"El jugador dijo algo importante: {text}", weight)]
    if re.search(r"\b(con|sobre|llamad[oa]|se llama|hable|platique)\b", match_text) and re.search(
        r"\b[A-Z][A-Za-z_]{2,}\b", text
    ):
        return [(f"El jugador menciono un nombre o conversacion importante: {text}", 5)]
    return []


def assistant_message_text(response_text: str) -> str:
    data = extract_json_object(response_text)
    if data is None:
        return response_text
    return str(data.get("message") or data.get("answer") or response_text)


def extract_assistant_facts(assistant_text: str) -> list[tuple[str, int]]:
    text = compact_text(assistant_message_text(assistant_text), 240)
    if not text:
        return []
    match_text = normalize_for_match(text)
    intent_pattern = r"\b(voy a|iba a|tengo que|debo|necesito|me retiro a|me voy a|seguire|estoy por)\b"
    task_pattern = (
        r"\b(afilar|pulir|picar|cortar|talar|pescar|cocinar|hornear|patrullar|vigilar|"
        r"reparar|construir|minar|servir|curar|sembrar|cosechar|forjar|leer|mapear|"
        r"volver a casa|dormir|trabajar|hacer pan|preparar|defender)\b"
    )
    if re.search(intent_pattern, match_text) and re.search(task_pattern, match_text):
        return [(f"El aldeano dijo que iba a hacer esto: {text}", 6)]
    return []


def build_instructions(
    system_text: str,
    facts: list[str],
    player_facts: list[str],
    player_lore: str,
    mentioned_lore: str,
    profile: str,
    npc_identity: str,
    player_rule: str,
    player_name: str,
    focus_context: str,
    command_hint: str,
    claim_context: str,
    memory_context: str,
    recent_context: str,
    family_context: str,
    village_context: str,
    self_context: str,
    name_reference_context: str,
) -> str:
    parts: list[str] = []
    if self_context:
        parts.append(self_context)
    if name_reference_context:
        parts.append(name_reference_context)
    parts.append(player_rule)
    if focus_context:
        parts.append(focus_context)
        if family_context:
            parts.append(
                family_context
                + " Usa estos datos con naturalidad solo cuando encajen; no recites todo el arbol de golpe. "
                + "Si un familiar esta vivo usa 'es/esta'; usa 'fue/estaba' solo cuando figure como fallecido/a. "
                + "Si hay pareja/conyuge actual viva registrada, recuerda su nombre y rechaza coqueteos de terceros con lealtad. "
                + "Si la pareja registrada esta fallecida, no hables como si estuviera viva ni inventes una pareja actual."
            )
    if player_lore:
        parts.append(player_lore)
    if mentioned_lore:
        parts.append(mentioned_lore)
    parts.append(current_profession_guidance(system_text))
    temperament = personality_guidance(system_text)
    if temperament:
        parts.append(temperament)
    life_guidance = life_stage_world_guidance(system_text)
    if life_guidance:
        parts.append(life_guidance)
    trait_guidance = trait_mood_guidance(system_text)
    if trait_guidance:
        parts.append(trait_guidance)
    relation_guidance = relationship_roleplay_guidance(family_context, system_text, player_name)
    if relation_guidance:
        parts.append(relation_guidance)
    player_relation = current_player_relationship_guidance(system_text, family_context, player_name)
    if player_relation:
        parts.append(player_relation)
    if memory_context:
        parts.append(memory_context)
    if recent_context:
        parts.append(recent_context)
    if facts:
        parts.append("Memoria esencial:\n" + "\n".join(f"- {fact}" for fact in facts))
    if player_facts:
        parts.append(
            "Memoria general del jugador compartida por la aldea:\n"
            + "\n".join(f"- {fact}" for fact in player_facts)
        )
    if profile:
        parts.append(
            "Perfil extra por nombre del aldeano: "
            "usalo como capa de roleplay e identidad, pero nunca sobrescribe nombre, familia, oficio, genero, rasgos, humor, relacion, orientacion, recuerdos ni comandos actuales enviados por MCA. "
            "Integra 1 detalle, vivencia o frase original del perfil solo cuando encaje; no recites la ficha completa. "
            + profile
        )
    if npc_identity:
        parts.append(
            "Identidad persistente del aldeano: "
            + npc_identity
            + " Usala como trasfondo; no la recites como ficha."
        )
    if claim_context:
        parts.append(claim_context)
    if village_context:
        parts.append(
            village_context
            + " Puedes mencionarlo como vida social o chisme breve, sin listar nombres porque si."
        )
    if command_hint:
        parts.append(command_hint)
    if system_text:
        parts.append("Contexto breve enviado por MCA:\n" + sanitize_system_text(system_text))
    parts.append("Reglas generales de estilo y formato:\n" + read_prompt())
    instructions = "\n\n".join(part for part in parts if part.strip())
    return compact_text(instructions, max(env_int("MCA_INSTRUCTIONS_MAX_CHARS", 5200), 5200))


def call_openai_responses(
    api_key: str,
    model: str,
    instructions: str,
    input_messages: list[dict[str, str]],
    max_output_tokens: int,
    store: bool,
) -> tuple[str | None, str | None]:
    base_url = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
    body = {
        "model": model,
        "instructions": instructions,
        "input": input_messages,
        "max_output_tokens": max_output_tokens,
        "store": store,
    }
    reasoning_effort = os.environ.get("OPENAI_REASONING_EFFORT", "").strip()
    if reasoning_effort:
        body["reasoning"] = {"effort": reasoning_effort}
    text_verbosity = os.environ.get("OPENAI_TEXT_VERBOSITY", "").strip()
    if text_verbosity:
        body["text"] = {"verbosity": text_verbosity}
    request = urllib.request.Request(
        f"{base_url}/responses",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            data = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        return None, extract_error(raw) or f"openai_http_{exc.code}"
    except Exception as exc:
        return None, f"openai_request_failed: {exc}"

    text = data.get("output_text")
    if isinstance(text, str) and text.strip():
        return text.strip(), None

    output = data.get("output")
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, dict):
                continue
            for content in item.get("content", []):
                if isinstance(content, dict) and isinstance(content.get("text"), str):
                    return content["text"].strip(), None

    status = data.get("status")
    incomplete = data.get("incomplete_details")
    if status == "incomplete":
        reason = "unknown"
        if isinstance(incomplete, dict):
            reason = str(incomplete.get("reason") or reason)
        return None, f"openai_incomplete_{reason}"

    return None, "openai_empty_response"


def extract_error(raw: str) -> str | None:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return compact_text(raw, 240) if raw else None
    error = data.get("error")
    if isinstance(error, dict):
        return compact_text(str(error.get("message") or error.get("code") or "openai_error"), 240)
    if isinstance(error, str):
        return compact_text(error, 240)
    return None


def chat_completion_response(model: str, content: str) -> dict[str, Any]:
    return {
        "id": "chatcmpl-mca-roleplay-proxy",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop",
            }
        ],
    }


def local_fallback_reply(last_user: str) -> str:
    lowered = normalize_for_match(last_user)
    if any(word in lowered for word in ("beso", "besar", "te amo", "te quiero")):
        return "Ahora mismo no encuentro las palabras, pero no te estoy apartando. Dame un momento, ¿si?"
    if any(word in lowered for word in ("ayuda", "tala", "ataca", "sigueme", "sígueme")):
        return "Te escucho, pero necesito pensarlo antes de moverme. No voy a prometer algo que no pueda cumplir."
    return "Perdona, perdi el hilo un segundo. Repitemelo mas simple."


def extract_json_object(text: str) -> dict[str, Any] | None:
    cleaned = text.strip().strip("`")
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end <= start:
        return None
    try:
        data = json.loads(cleaned[start : end + 1])
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def extract_malformed_message(text: str) -> str:
    cleaned = text.strip().strip("`")
    patterns = [
        r'"message"\s*:\s*"([^"]*)',
        r"'message'\s*:\s*'([^']*)",
        r"\bmessage\s*[:=]\s*(.*?)(?:,\s*(?:optionalCommand|optional_command|command)\s*[:=]|[}\n]|$)",
        r'"answer"\s*:\s*"([^"]*)',
        r"\banswer\s*[:=]\s*(.*?)(?:,\s*(?:optionalCommand|optional_command|command)\s*[:=]|[}\n]|$)",
    ]
    for pattern in patterns:
        match = re.search(pattern, cleaned, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return str(match.group(1)).strip(" \t\r\n\"'")
    return ""


def repair_common_mojibake(text: str) -> str:
    replacements = {
        "Â¿": "¿",
        "Â¡": "¡",
        "Ã¡": "á",
        "Ã©": "é",
        "Ã­": "í",
        "Ã³": "ó",
        "Ãº": "ú",
        "Ã±": "ñ",
        "Ã¼": "ü",
        "Ã": "Á",
        "Ã‰": "É",
        "Ã": "Í",
        "Ã“": "Ó",
        "Ãš": "Ú",
        "Ã‘": "Ñ",
    }
    for bad, good in replacements.items():
        text = text.replace(bad, good)
    return text


def strip_json_artifacts(text: str) -> str:
    text = re.sub(r"```(?:json)?|```", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\b(?:optionalCommand|optional_command|command|message|answer)\s*[:=]\s*", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\b(?:null|none|true|false)\b\s*,?", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"[{}\[\]`|^~\\]+", " ", text)
    text = re.sub(r"(?:[+=_/<>#]{2,}|[-=+_]{3,})", " ", text)
    text = re.sub(r"\s*[,;:]\s*(?=$)", "", text)
    return text


def limit_dialogue_length(text: str) -> str:
    limit = env_int("MCA_RESPONSE_MAX_CHARS", 0)
    if limit <= 0 or len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    cut = text[: limit - 3].rstrip()
    sentence_end = max(cut.rfind("."), cut.rfind("!"), cut.rfind("?"))
    if sentence_end >= max(20, limit // 3):
        return cut[: sentence_end + 1].strip()
    return cut + "..."


def looks_like_refusal(text: str) -> bool:
    lowered = normalize_for_match(text)
    return any(
        phrase in lowered
        for phrase in (
            "no puedo",
            "no quiero",
            "ahora no",
            "no estoy",
            "necesito descansar",
            "no confio",
            "no confío",
            "demasiado peligroso",
        )
    )


def clean_player_address(text: str, player_name: str) -> str:
    if player_name.lower() == "chanchowapo":
        return text
    return re.sub(r"\bMondongo\b", "oye", text, flags=re.IGNORECASE)


def user_asks_villager_name(last_user: str) -> bool:
    text = normalize_for_match(last_user)
    return bool(
        re.search(r"\b(como\s+te\s+llamas|cual\s+es\s+tu\s+nombre|dime\s+tu\s+nombre|tu\s+nombre)\b", text)
    )


def user_allows_player_name(last_user: str, player_name: str) -> bool:
    text = normalize_for_match(last_user)
    if not text:
        return False
    if re.search(r"\b(como\s+me\s+llamo|cual\s+es\s+mi\s+nombre|di\s+mi\s+nombre|dime\s+mi\s+nombre|quien\s+soy|me\s+conoces)\b", text):
        return True
    if re.search(r"\b(me\s+llamo|yo\s+soy|soy\s+el|soy\s+la)\b", text):
        return True
    if player_name and normalize_for_match(player_name) in text:
        return True
    return False


def clean_dialogue_style(text: str) -> str:
    text = repair_common_mojibake(text)
    text = strip_json_artifacts(text)
    text = text.replace("*", "")
    text = re.sub(r"[\U0001F300-\U0001FAFF\U00002700-\U000027BF]", "", text)
    text = re.sub(r"\s+([,.!?;:])", r"\1", text)
    text = re.sub(r"([¿¡])\s+", r"\1", text)
    text = re.sub(r"\s+", " ", text).strip(" \t\r\n\"' ,;:-")
    return limit_dialogue_length(text)


def clean_self_name_mentions(text: str, villager_name: str, last_user: str) -> str:
    if not villager_name or user_asks_villager_name(last_user):
        return text
    fixed = text
    for pattern in loose_name_patterns(villager_name):
        for _ in range(2):
            fixed = re.sub(rf"^\s*{pattern}\s*[,;:\-]\s*", "", fixed, flags=re.IGNORECASE)
            fixed = re.sub(
                rf"^\s*(?:yo\s+soy|soy|me\s+llamo)\s+{pattern}\s*[,;:\-]?\s*",
                "",
                fixed,
                flags=re.IGNORECASE,
            )
        fixed = re.sub(
            rf"\bme\s+llamo\s+{pattern}\b",
            "me conoces",
            fixed,
            flags=re.IGNORECASE,
        )
        fixed = re.sub(rf"\b{pattern}\s*,\s*{pattern}\b", lambda _match: villager_name, fixed, flags=re.IGNORECASE)
        fixed = re.sub(rf"[,;:\-]\s*{pattern}\s*([.!?])?\s*$", lambda m: m.group(1) or "", fixed, flags=re.IGNORECASE)
        fixed = re.sub(rf"\b{pattern}\b\s*(?=\b{pattern}\b)", "", fixed, flags=re.IGNORECASE)
    return fixed.strip() or text


def clean_player_name_mentions(text: str, player_name: str, last_user: str) -> str:
    if not player_name or user_allows_player_name(last_user, player_name):
        return text
    patterns = loose_name_patterns(player_name)
    if player_name.casefold() == "chanchowapo":
        patterns.extend([r"Mondongo"])
    fixed = text
    for pattern in patterns:
        for _ in range(2):
            fixed = re.sub(rf"^\s*{pattern}\s*[,;:\-]\s*", "", fixed, flags=re.IGNORECASE)
            fixed = re.sub(rf"[,;:\-]\s*{pattern}\s*([.!?])?\s*$", lambda m: m.group(1) or "", fixed, flags=re.IGNORECASE)
            fixed = re.sub(rf"\s*,\s*{pattern}\s*,\s*", ", ", fixed, flags=re.IGNORECASE)
            fixed = re.sub(rf"\s*,\s*{pattern}\b", "", fixed, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", fixed).strip(" ,;:-") or text


def loose_name_patterns(name: str) -> list[str]:
    name = name.strip()
    if not name:
        return []
    pieces = [part for part in re.split(r"[_\s-]+", name) if part]
    patterns = [re.escape(name)]
    if len(pieces) > 1:
        escaped_pieces: list[str] = []
        for index, piece in enumerate(pieces):
            if index > 0 and piece.casefold().startswith("ola"):
                escaped_pieces.append("h?" + re.escape(piece))
            else:
                escaped_pieces.append(re.escape(piece))
        patterns.append(r"[_\s-]*".join(escaped_pieces))
    return list(dict.fromkeys(patterns))


def correct_child_name_confusion(text: str, player_name: str, child_names: list[str]) -> str:
    child_name = next((name for name in child_names if name and normalize_for_match(name) != normalize_for_match(player_name)), "")
    if not player_name or not child_name:
        return text

    def fix_message(message: str) -> str:
        fixed = message
        for pattern in loose_name_patterns(player_name):
            after_child_label = (
                r"(\b(?:mi|tu|su|nuestro|nuestra|el|la|ese|esa)?\s*"
                r"(?:hijo|hija|bebe|nino|nina)\s*"
                r"(?:es|se llama|llamado|llamada|,|:)?\s*)"
                + pattern
                + r"\b"
            )
            fixed = re.sub(after_child_label, lambda match: match.group(1) + child_name, fixed, flags=re.IGNORECASE)
            before_child_label = (
                r"\b"
                + pattern
                + r"\b(\s+(?:es|era|seria)\s+"
                r"(?:mi|tu|su|nuestro|nuestra|el|la)?\s*(?:hijo|hija|bebe|nino|nina)\b)"
            )
            fixed = re.sub(before_child_label, lambda match: child_name + match.group(1), fixed, flags=re.IGNORECASE)
        return fixed

    data = extract_json_object(text)
    if data is None:
        return fix_message(text)
    message = str(data.get("message") or data.get("answer") or "")
    if message:
        data["message"] = fix_message(message)
    return json.dumps(data, ensure_ascii=False)


def normalize_mca_response(
    text: str,
    system_text: str,
    last_user: str,
    player_name: str,
    villager_name: str,
    commands: dict[str, str],
    requested_command: str,
) -> str:
    wants_json = "The reply MUST be in this JSON format" in system_text or bool(commands)
    text = clean_player_address(text, player_name)

    if not wants_json:
        return clean_dialogue_style(
            clean_player_name_mentions(
                clean_self_name_mentions(clean_dialogue_style(text), villager_name, last_user),
                player_name,
                last_user,
            )
        )

    data = extract_json_object(text)
    if data is None:
        data = {"message": extract_malformed_message(text) or text, "command": ""}

    message = clean_player_address(str(data.get("message") or data.get("answer") or text), player_name)
    message = clean_dialogue_style(
        clean_self_name_mentions(clean_dialogue_style(message), villager_name, last_user)
    )
    message = clean_dialogue_style(clean_player_name_mentions(message, player_name, last_user))
    command = str(
        data.get("optionalCommand") or data.get("command") or data.get("optional_command") or ""
    ).strip()

    known_commands = commands or KNOWN_MCA_COMMANDS
    allow_mechanical_command = (
        bool(requested_command)
        and requested_command in known_commands
        and is_direct_command(last_user, requested_command)
    )
    if not allow_mechanical_command:
        command = ""
    elif command and command != requested_command:
        command = ""

    if allow_mechanical_command and requested_command and not command and not looks_like_refusal(message):
        command = requested_command

    if command and command not in known_commands:
        command = ""

    return json.dumps({"message": message, "optionalCommand": command}, ensure_ascii=False)


class Handler(BaseHTTPRequestHandler):
    server_version = "MCA roleplay proxy"

    def authorized(self) -> bool:
        expected = os.environ.get("PROXY_SHARED_TOKEN", "").strip()
        if not expected:
            return True
        authorization = self.headers.get("Authorization", "").strip()
        token = self.headers.get("X-MCA-Proxy-Token", "").strip()
        if authorization.lower().startswith("bearer "):
            token = authorization[7:].strip()
        elif authorization:
            token = authorization
        return token == expected

    def authorized_query_or_header(self, parsed: urllib.parse.ParseResult) -> bool:
        if self.authorized():
            return True
        expected = os.environ.get("PROXY_SHARED_TOKEN", "").strip()
        if not expected:
            return True
        query = urllib.parse.parse_qs(parsed.query)
        return (query.get("token") or [""])[0].strip() == expected

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        if path in ("", "/"):
            self.send_json(
                {
                    "ok": True,
                    "service": "mca-roleplay-proxy",
                    "health": "/health",
                    "chat_completions": "/v1/chat/completions",
                }
            )
            return
        if path == "/health":
            family_count = self.server.family.entry_count()
            family_stats = self.server.family.relationship_stats()
            self.send_json(
                {
                    "ok": True,
                    "code_version": CODE_VERSION,
                    "model": os.environ.get("OPENAI_MODEL", "gpt-5-nano"),
                    "prompt_mode": os.environ.get("MCA_PROMPT_MODE", "minimal"),
                    "raw_turns": raw_turn_memory_enabled(),
                    "max_output_tokens": output_token_limit(),
                    "response_max_chars": env_int("MCA_RESPONSE_MAX_CHARS", 0),
                    "reasoning_effort": os.environ.get("OPENAI_REASONING_EFFORT", ""),
                    "text_verbosity": os.environ.get("OPENAI_TEXT_VERBOSITY", ""),
                    "memory_backend": getattr(self.server.memory, "backend_name", "unknown"),
                    "redis_namespace": getattr(self.server.memory, "namespace", ""),
                    "max_system_message_chars": max(env_int("MCA_MAX_SYSTEM_MESSAGE_CHARS", 12000), 12000),
                    "max_system_chars": max(env_int("MCA_MAX_SYSTEM_CHARS", 6000), 6000),
                    "instructions_max_chars": max(env_int("MCA_INSTRUCTIONS_MAX_CHARS", 5200), 5200),
                    "family_entries": family_count,
                    "family_data_loaded": family_count > 0,
                    **family_stats,
                    "world_data_dir": str(getattr(self.server, "world_data_dir", "")),
                    "village_count": self.server.village.village_count(),
                    "direct_commands_local": env_bool("MCA_DIRECT_COMMANDS_LOCAL", True),
                    "shared_player_memory": env_bool("MCA_SHARED_PLAYER_MEMORY", False),
                    "name_fallback_memory": env_bool("MCA_ALLOW_NAME_FALLBACK_MEMORY", False),
                    "debug_recent_enabled": env_bool("MCA_DEBUG_RECENT", False),
                    "max_player_facts": env_int("MCA_MAX_PLAYER_FACTS", 3),
                }
            )
            return
        if path == "/debug/recent":
            if not env_bool("MCA_DEBUG_RECENT", False):
                self.send_json({"error": "debug_disabled"}, status=404)
                return
            if not self.authorized_query_or_header(parsed):
                self.send_json({"error": "unauthorized"}, status=401)
                return
            self.send_json({"recent": list(self.server.recent_debug)})
            return
        self.send_json({"error": "not_found"}, status=404)

    def do_POST(self) -> None:
        if self.path.rstrip("/") != "/v1/chat/completions":
            self.send_json({"error": "not_found"}, status=404)
            return

        if not self.authorized():
            self.send_json({"error": "unauthorized"}, status=401)
            return

        api_key = os.environ.get("OPENAI_API_KEY", "").strip()
        if not api_key:
            self.send_json({"error": "OPENAI_API_KEY is missing in tools/mca_roleplay_proxy/.env"})
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
        except Exception:
            self.send_json({"error": "invalid_json"})
            return

        messages = get_messages(payload)
        system_text, input_messages, last_user, villager_name, player_name = split_messages(messages)
        system_villager_name, system_player_name = extract_names_from_system(system_text)
        villager_name = villager_name or system_villager_name
        player_name = player_name or system_player_name
        original_last_user = last_user
        input_messages, last_user, had_self_vocative = rewrite_vocative_messages(input_messages, villager_name)
        ids = parse_session_ids(system_text)
        ids = apply_fallback_session_ids(ids, villager_name, player_name, system_text)
        debug_snapshot = request_debug_snapshot(ids, villager_name, player_name, system_text, original_last_user)
        debug_snapshot["model_last_user_excerpt"] = compact_text(last_user, 120)
        debug_snapshot["self_vocative_rewrite"] = had_self_vocative
        self.server.recent_debug.appendleft(debug_snapshot)
        print(
            "[MCA request] "
            f"villager={debug_snapshot['villager_name']!r} "
            f"player={debug_snapshot['player_name']!r} "
            f"profession={debug_snapshot['profession']!r} "
            f"character_id={debug_snapshot['character_id']!r} "
            f"has_real_character_id={debug_snapshot['has_real_character_id']}"
        )
        if env_bool("OPENAI_ALLOW_REQUEST_MODEL", False):
            model = str(payload.get("model") or os.environ.get("OPENAI_MODEL", "gpt-5.4-nano"))
        else:
            model = os.environ.get("OPENAI_MODEL", "gpt-5.4-nano")
        if model == "default":
            model = os.environ.get("OPENAI_MODEL", "gpt-5.4-nano")

        profiles = load_profiles()
        lore = load_player_lore()
        profile = find_character_profile(system_text, villager_name, profiles)
        player_lore = player_lore_context(player_name, lore)
        mentioned_lore = mentioned_lore_context(last_user, lore, player_name)
        commands = extract_mca_commands(system_text)
        requested_command = detect_requested_command(last_user, commands)
        command_hint = command_instructions(commands, requested_command) if requested_command else ""
        if (
            env_bool("MCA_DIRECT_COMMANDS_LOCAL", True)
            and requested_command
            and requested_command in commands
            and is_direct_command(last_user, requested_command)
        ):
            reply = normalize_mca_response(
                local_command_reply(requested_command),
                system_text,
                last_user,
                player_name,
                villager_name,
                commands,
                requested_command,
            )
            self.server.memory.add_turn(ids, "user", last_user)
            self.server.memory.add_turn(ids, "assistant", reply)
            self.send_json(chat_completion_response(model, reply))
            return
        family_parts = [
            self.server.family.context_for(ids.get("character_id"), "Familia del aldeano")
        ]
        player_family = self.server.family.context_for(ids.get("player_id"), "Familia del jugador")
        if player_family and ids.get("player_id") != ids.get("character_id"):
            family_parts.append(player_family)
        player_node = self.server.family.get(ids.get("player_id"))
        character_node = self.server.family.get(ids.get("character_id"))
        registered_villager_name = villager_name or (str(character_node["name"]) if character_node else "")
        registered_player_name = player_name or (str(player_node["name"]) if player_node else "")
        player_child_names = self.server.family.child_names_for(ids.get("player_id"))
        player_children_summary = self.server.family.children_summary_for(
            ids.get("player_id"), "Hijos del jugador registrados"
        )
        if player_child_names and "no hay hijos registrados" not in normalize_for_match(player_children_summary):
            family_parts.append(
                player_children_summary
                + f" El jugador actual se llama {registered_player_name}; sus hijos se llaman "
                + ", ".join(player_child_names)
                + ". No sustituyas el nombre de ningun hijo por el nombre del jugador."
            )
        family_context = " ".join(part for part in family_parts if part)
        village_context = self.server.village.context_for(
            ids.get("character_id"), ids.get("player_id")
        )
        claim_context = self.server.family.family_claim_context(
            last_user, ids.get("character_id"), ids.get("player_id")
        )
        recall_context = memory_question_context(last_user)
        focus_context = response_focus_context(last_user, system_text)
        interaction_context = recent_interaction_context(last_user, system_text)
        if interaction_context:
            focus_context = " ".join(part for part in [focus_context, interaction_context] if part)
        romance_boundary = self.server.family.romance_boundary_context(
            ids.get("character_id"), ids.get("player_id"), last_user
        )
        if romance_boundary:
            focus_context = " ".join(part for part in [focus_context, romance_boundary] if part)
        current_profession = extract_current_profession(system_text)
        facts = filter_facts_for_current_context(
            self.server.memory.essential_facts(ids, env_int("MCA_MAX_MEMORY_FACTS", 4)),
            current_profession,
        )
        recent_context = recent_turns_context(
            self.server.memory.recent_turns(ids, env_int("MCA_RECENT_TURN_CONTEXT", 4))
        )
        npc_identity = self.server.memory.npc_identity(ids, registered_villager_name, system_text)
        shared_player_memory = env_bool("MCA_SHARED_PLAYER_MEMORY", False)
        if player_lore and shared_player_memory:
            self.server.memory.add_player_fact(ids, player_lore, 9)
        player_facts = (
            self.server.memory.player_facts(ids, env_int("MCA_MAX_PLAYER_FACTS", 3))
            if shared_player_memory
            else []
        )
        instructions = build_instructions(
            system_text=system_text,
            facts=facts,
            player_facts=player_facts,
            player_lore=player_lore,
            mentioned_lore=mentioned_lore,
            profile=profile,
            npc_identity=npc_identity,
            player_rule=player_name_rule(player_name),
            player_name=player_name,
            focus_context=focus_context,
            command_hint=command_hint,
            claim_context=claim_context,
            memory_context=recall_context,
            recent_context=recent_context,
            family_context=family_context,
            village_context=village_context,
            self_context=self_awareness_context(system_text, registered_villager_name, registered_player_name, ids),
            name_reference_context=self_name_reference_guidance(
                original_last_user, last_user, registered_villager_name
            ),
        )
        text, error = call_openai_responses(
            api_key=api_key,
            model=model,
            instructions=instructions,
            input_messages=input_messages,
            max_output_tokens=output_token_limit(),
            store=env_bool("OPENAI_STORE_RESPONSES", False),
        )
        if error:
            if error.startswith("openai_empty_response") or error.startswith("openai_incomplete_"):
                fallback = normalize_mca_response(
                    local_fallback_reply(last_user),
                    system_text,
                    last_user,
                    player_name,
                    registered_villager_name,
                    commands,
                    requested_command,
                )
                fallback = correct_child_name_confusion(fallback, registered_player_name, player_child_names)
                self.send_json(chat_completion_response(model, fallback))
                return
            self.send_json({"error": error})
            return

        assert text is not None
        text = normalize_mca_response(
            text, system_text, last_user, player_name, registered_villager_name, commands, requested_command
        )
        text = correct_child_name_confusion(text, registered_player_name, player_child_names)
        for fact, weight in extract_recent_interaction_facts(last_user, text, system_text):
            self.server.memory.add_fact(ids, fact, weight)
        for fact, weight in extract_romance_memory_facts(last_user, text):
            self.server.memory.add_fact(ids, fact, weight)
        for fact, weight in extract_important_facts(last_user, text):
            self.server.memory.add_fact(ids, fact, weight)
            if shared_player_memory:
                self.server.memory.add_player_fact(ids, fact, weight)
        for fact, weight in extract_assistant_facts(text):
            self.server.memory.add_fact(ids, fact, weight)
        self.server.memory.add_turn(ids, "user", last_user)
        self.server.memory.add_turn(ids, "assistant", text)
        self.send_json(chat_completion_response(model, text))

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {fmt % args}")

    def send_json(self, data: dict[str, Any], status: int = 200) -> None:
        raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


class Server(ThreadingHTTPServer):
    memory: MemoryStore
    family: FamilyTreeCache
    village: VillageCache
    world_data_dir: Path
    recent_debug: deque[dict[str, Any]]


def main() -> None:
    load_env_file(ROOT / ".env")
    load_env_file(ROOT / "api-keys.env")
    host = os.environ.get("PROXY_HOST", "127.0.0.1")
    port = env_int("PROXY_PORT", env_int("PORT", 8765))
    db_path = ROOT / os.environ.get("MCA_MEMORY_DB", "memory.sqlite3")
    data_dir_raw = os.environ.get("MCA_WORLD_DATA_DIR", "../../world/data")
    data_dir = (ROOT / data_dir_raw).resolve()
    server = Server((host, port), Handler)
    server.memory = create_memory_store(db_path)
    server.family = FamilyTreeCache(data_dir)
    server.village = VillageCache(data_dir)
    server.world_data_dir = data_dir
    server.recent_debug = deque(maxlen=RECENT_DEBUG_LIMIT)
    print(f"MCA roleplay proxy escuchando en http://{host}:{port}/v1/chat/completions")
    print(f"Modelo configurado: {os.environ.get('OPENAI_MODEL', 'gpt-5.4-nano')}")
    print(f"Modo prompt: {os.environ.get('MCA_PROMPT_MODE', 'minimal')}")
    print("API key: " + ("configurada" if os.environ.get("OPENAI_API_KEY") else "faltante en .env"))
    server.serve_forever()


if __name__ == "__main__":
    main()
