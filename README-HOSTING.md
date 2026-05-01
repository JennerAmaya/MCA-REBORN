# MCA Roleplay Proxy - Hosting liviano

Este paquete corre un endpoint compatible con OpenAI Chat Completions para que
MCA Reborn pueda hablar con NPCs usando el proxy de rol.

No incluye claves reales. Debes configurar variables de entorno en el hosting.

## Requisitos

- Python 3.10 o superior.
- No requiere paquetes externos; usa solo librerias estandar de Python.
- Un puerto HTTP publico o una URL HTTPS publica.

## Archivos importantes

- `server.py`: servidor HTTP del proxy.
- `.env.example`: plantilla de variables.
- `prompts/ocean_roleplay.txt`: tono/personaje del mundo oceanico.
- `profiles/characters.json`: perfiles manuales por nombre de NPC.
- `profiles/player_lore.json`: lore breve de jugadores.
- `memory.sqlite3`: no se incluye; se crea automaticamente.

## Variables de entorno recomendadas

```env
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-5.4-nano
OPENAI_ALLOW_REQUEST_MODEL=false
OPENAI_BASE_URL=https://api.openai.com/v1
OPENAI_STORE_RESPONSES=false
OPENAI_MAX_OUTPUT_TOKENS=90
OPENAI_REASONING_EFFORT=none
OPENAI_TEXT_VERBOSITY=low

PROXY_HOST=0.0.0.0
PROXY_PORT=8765
PROXY_SHARED_TOKEN=pon_un_token_largo_random

MCA_MEMORY_DB=memory.sqlite3
MCA_PROMPT_MODE=minimal
MCA_CONTEXT_MESSAGES=2
MCA_MAX_INPUT_CHARS=700
MCA_MAX_SYSTEM_CHARS=2800
MCA_MAX_MEMORY_FACTS=4
MCA_MAX_PLAYER_FACTS=3
MCA_PLAYER_FACT_LIMIT=24
MCA_STORE_RAW_TURNS=false
MCA_DIRECT_COMMANDS_LOCAL=true
MCA_DIRECT_COMMAND_MAX_CHARS=90
MCA_FAMILY_CONTEXT=true
MCA_FAMILY_MAX_FACTS=8
MCA_FAMILY_REFRESH_SECONDS=5
MCA_WORLD_DATA_DIR=world_data
MCA_VILLAGE_CONTEXT=true
MCA_VILLAGE_MAX_NAMES=5
MCA_ROLEPLAY_PROMPT=prompts/ocean_roleplay.txt
MCA_CHARACTER_PROFILES=profiles/characters.json
MCA_PLAYER_LORE=profiles/player_lore.json
MCA_PLAYER_LORE_MAX_CHARS=360
MCA_NPC_IDENTITIES=true
MCA_NPC_IDENTITY_MAX_CHARS=260
```

Si el hosting asigna el puerto con una variable `PORT`, usa ese valor como
`PROXY_PORT` o configura el panel para iniciar con ese puerto.

## Comando de inicio

```bash
python server.py
```

En algunos hostings:

```bash
python3 server.py
```

## Configuracion en el servidor Minecraft

En `config/mca.json` del servidor de Minecraft:

```json
"enableVillagerChatAI": true,
"villagerChatAIEndpoint": "https://TU-DOMINIO-O-IP/v1/chat/completions",
"villagerChatAIUseTools": true,
"villagerChatAIToken": "el_mismo_valor_de_PROXY_SHARED_TOKEN",
"villagerChatAIModel": "gpt-5.4-nano"
```

Si tu hosting solo da HTTP y no HTTPS, prueba con `http://...`, pero HTTPS es
preferible.

## Prueba rapida

Abre en navegador:

```text
https://TU-DOMINIO-O-IP/health
```

Debe responder JSON con `ok: true`.

## Importante

- No subas `.env`, `api-keys.env`, `memory.sqlite3`, logs ni crash reports.
- No dejes `PROXY_SHARED_TOKEN` vacio si el endpoint sera publico.
- No pegues la API key en `config/mca.json`.
- Si quieres que el proxy lea familia/aldea de MCA, sube copias periodicas de
  `world/data/MCA-FamilyTree.dat` y `world/data/mca_villages.dat` a la carpeta
  indicada por `MCA_WORLD_DATA_DIR`. Sin esos archivos, el chat funciona igual,
  solo con menos contexto familiar/de aldea.
