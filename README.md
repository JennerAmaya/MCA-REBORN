# MCA Roleplay Proxy

Proxy local para que MCA Reborn use OpenAI con gasto minimo de tokens.

## Donde poner la API key

Edita `api-keys.env`:

```env
OPENAI_API_KEY=tu_key_nueva
```

No pegues la key en `config/mca.json`.

## Arranque y cierre con el servidor

`start-server.bat` ejecuta `ensure-mca-roleplay-proxy.ps1` antes de iniciar
Minecraft. Ese script arranca el proxy si no existe y guarda su PID en
`mca-roleplay-proxy.pid`.

Cuando el servidor Java se cierra normalmente, `start-server.bat` llama a
`stop-mca-roleplay-proxy.ps1` para apagar el proxy y limpiar el PID. Si cierras
la ventana a la fuerza, puede quedar vivo; en ese caso ejecuta manualmente:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tools\mca_roleplay_proxy\stop-mca-roleplay-proxy.ps1
```

## Modo ultra ahorro

Valores importantes de `.env`:

```env
OPENAI_MODEL=gpt-5.4-nano
OPENAI_MAX_OUTPUT_TOKENS=90
MCA_PROMPT_MODE=minimal
MCA_CONTEXT_MESSAGES=2
MCA_MAX_MEMORY_FACTS=4
MCA_MAX_PLAYER_FACTS=3
MCA_STORE_RAW_TURNS=false
MCA_DIRECT_COMMANDS_LOCAL=true
MCA_FAMILY_CONTEXT=true
MCA_VILLAGE_CONTEXT=true
MCA_PLAYER_LORE=profiles/player_lore.json
MCA_NPC_IDENTITIES=true
```

- `gpt-5.4-nano`: modelo barato para frases simples.
- `OPENAI_MAX_OUTPUT_TOKENS`: limita respuestas largas.
- `MCA_CONTEXT_MESSAGES=2`: manda el ultimo intercambio breve para que puedan seguir la conversacion sin gastar mucho.
- `MCA_MAX_MEMORY_FACTS=4`: incluye pocos recuerdos esenciales.
- `MCA_MAX_PLAYER_FACTS=3`: comparte pocos recuerdos importantes del jugador con otros aldeanos.
- `MCA_STORE_RAW_TURNS=false`: no guarda conversaciones completas.
- `MCA_DIRECT_COMMANDS_LOCAL=true`: ejecuta ordenes simples como seguir/quedarse sin gastar tokens.
- `MCA_FAMILY_CONTEXT=true`: agrega conyuge, padres e hijos desde `world/data/MCA-FamilyTree.dat`.
- `MCA_VILLAGE_CONTEXT=true`: agrega aldea y pocos vecinos desde `world/data/mca_villages.dat`.
- `MCA_PLAYER_LORE`: agrega lore breve por nombre de jugador.
- `MCA_NPC_IDENTITIES=true`: crea una identidad persistente local por NPC sin llamar a la API.

## Perfiles por nombre

Edita `profiles/characters.json`.

Si un aldeano se llama `Killjoy`, `Ichigo`, `Xokas`, etc., el proxy agrega ese
perfil al prompt. Usa descripciones de personalidad, no textos largos.

## Lore por jugador

Edita `profiles/player_lore.json`.

Ese lore es esencial: si el ultimo mensaje menciona `Jenner_Ola`, `Kainolimits`
o `Chanchowapo`, el proxy mete ese dato en el prompt aunque no sea el jugador
actual, para que el NPC responda con algun detalle concreto. El NPC debe elegir
1-2 detalles, variar cuales usa y no recitar toda la ficha.

Ejemplos actuales:

- `Jenner_Ola`: Jenner, constructor de la aldea.
- `Kainolimits`: el del bar; su cantina se elogia, la Reyna del Misisipi es su mejor bebida, Trueno es su chalan/empleado del mes, le gustan Social Wars y buenos pokemones.
- `Chanchowapo`: viejo minero temperamental; le gusta el posole y las jojoposes, su mejor amigo es Taticoso, ve todo a 4K y se rumora que tiene un perro llamado Milaneso.

## Identidad de NPC

Si un NPC no tiene perfil manual en `profiles/characters.json`, el proxy crea
una identidad breve en `memory.sqlite3`: rasgo estable, gustos, molestias y un
habito de conversacion. Esa identidad se reutiliza en futuros prompts para que
el aldeano no se sienta generico, sin guardar chats completos.

## Memoria esencial

El proxy guarda recuerdos solo cuando detecta cosas importantes:

- "recuerda que..."
- regalos, promesas, boda, beso, disculpa, pelea, rescate
- detalles de identidad o preferencias
- eventos oceanicos importantes como islas, barcos, tesoros o monstruos
- intenciones cortas del propio NPC, por ejemplo "tengo que afilar piedra" o
  "voy a patrullar", para que pueda retomarlo despues
- datos de jugadores como oficio, gustos, familiares, promesas o anecdotas
  importantes

La base local queda en `memory.sqlite3`.
Los recuerdos importantes del jugador tambien quedan en una memoria general
ligera para que otros NPC puedan comentarlos sin cargar conversaciones enteras.

## Familia y aldea MCA

El proxy lee los `.dat` del mundo cada pocos segundos y manda solo un resumen
compacto al modelo. No guarda todo el arbol en el prompt.

Los familiares vivos se marcan como `vivo/a` y los muertos como `fallecido/a`,
para que el NPC use presente o pasado correctamente. Si dices "nuestro hijo" o
afirmas un matrimonio/lazo que no consta en MCA, el proxy agrega una verificacion
compacta para que el NPC pueda corregirte en personaje.

Si preguntas algo como "que opinas de tu esposo?" y tu eres ese esposo/pareja
segun el arbol, el proxy agrega una pista para que responda en primera persona
y te reconozca directamente.

El proxy tambien manda siempre un resumen compacto de los hijos registrados del
jugador cuando existen. Esto evita que el modelo confunda el nombre del jugador
con el nombre del hijo; si aun intenta decir "mi hijo Jenner_Ola", el proxy
corrige la frase antes de enviarla a MCA.

Valores utiles:

```env
MCA_FAMILY_MAX_FACTS=8
MCA_FAMILY_REFRESH_SECONDS=5
MCA_VILLAGE_MAX_NAMES=5
```

Subelos solo si quieres mas contexto por respuesta; bajarlos reduce tokens.
`MCA_FAMILY_REFRESH_SECONDS` controla cada cuanto revisa cambios de divorcio,
muerte, hijos o pareja guardados por MCA.

## Ordenes escritas

El proxy devuelve `optionalCommand` para que MCA ejecute seguir/quedarse/etc.
Esto funciona cuando el texto llega a la conversacion AI de MCA. Si el mensaje
se escribe en el chat global normal de Minecraft, MCA no lo envia al proxy; para
eso haria falta un addon Fabric aparte que escuche chat cercano.

## Edad, oficio y entorno

MCA suele mandar al proxy datos como edad (`toddler`, `child`, `teen`), oficio
y clima. Tambien puede mandar rasgos, emociones o estados de animo. El proxy los
convierte en pistas compactas:

- ninos y adolescentes no usan romance adulto ni groserias fuertes.
- oficios como mason, farmer, fisherman o toolsmith generan actividades propias.
- noche, lluvia o truenos pueden cambiar el tono de la respuesta.
- rasgos como daltonismo, atletismo, torpeza, valentia u orientacion romantica
  afectan detalles y limites sin convertirse en caricatura.
- estados como alegria, enojo, tristeza, cansancio o miedo cambian el tono de la
  respuesta actual.

Saludos espontaneos al pasar, escuchar chat cercano sin abrir la interfaz de MCA
o detectar un monstruo real junto al aldeano no se pueden hacer solo desde este
proxy; eso requiere un addon Fabric que observe el mundo y dispare la charla.
