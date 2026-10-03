"""KiwD&D — motor independiente de campaña persistente para Telegram.
No importa ni modifica KiwRPG. main.py solo inyecta helpers de Telegram/DB.
"""
import json, random, time, re

_SEND=None; _SEND_TOPIC=None; _DICE=None; _DB=None; _ADMIN=None; _PHOTO=None; _NARRATE=None

def configure(send_message, send_dice, get_db, is_admin=None, generate_portrait=None, narrate=None, send_topic=None):
    global _SEND,_SEND_TOPIC,_DICE,_DB,_ADMIN,_PHOTO,_NARRATE
    _SEND,_SEND_TOPIC,_DICE,_DB,_ADMIN,_PHOTO,_NARRATE=send_message,send_topic,send_dice,get_db,is_admin,generate_portrait,narrate

def _S(chat_id,thread,text,reply_markup=None):
    if _SEND_TOPIC:
        return _SEND_TOPIC(chat_id,thread,text,reply_markup=reply_markup)
    if _SEND:
        return _SEND(chat_id,text,reply_markup=reply_markup)
    return None

CLASSES = {
 "guerrero":("Guerrero","Fuerza y resistencia; domina el frente de batalla."),
 "mago":("Mago","Magia versátil, conocimiento y control del campo."),
 "picaro":("Pícaro","Sigilo, engaño, precisión y soluciones inesperadas."),
 "paladin":("Paladín","Juramentos, defensa, presencia y poder sagrado."),
 "bardo":("Bardo","Carisma, inspiración, historias y magia social."),
 "druida":("Druida","Naturaleza, transformación y magia ancestral."),
 "monje":("Monje","Disciplina, movilidad y combate sin depender de armas."),
 "hechicero":("Hechicero","Poder innato, peligroso y explosivo."),
 "nigromante":("Nigromante","Muerte, espíritus y secretos prohibidos."),
 "cazador_demonios":("Cazador de Demonios","Rastrea, estudia y combate criaturas infernales."),
 "caballero_sangre":("Caballero de Sangre","Convierte sacrificio y heridas en poder."),
 "invocador":("Invocador","Pactos, familiares y criaturas convocadas."),
 "exorcista":("Exorcista","Sellos, protección y conocimiento de entidades."),
 "domador_bestias":("Domador de Bestias","Vínculos con criaturas y combate coordinado."),
 "espadachin_arcano":("Espadachín Arcano","Esgrima y magia en una misma disciplina."),
 "artificiero":("Artificiero","Ingenio, mecanismos, alquimia y artefactos."),
 "bruja":("Bruja","Pactos, maldiciones, intuición y magia ritual."),
 "explorador":("Explorador","Supervivencia, rastreo y precisión a distancia."),
}

OPENING_SCENES = [
 ("campanas", "Las campanas de Aeternus suenan trece veces. Nadie recuerda que la torre tenga trece campanas. Brok deja caer una jarra. Eira no mira la torre: mira la puerta.\n\nTres golpes.\n\n—Sé que Orin está aquí —dice una voz infantil desde afuera.\n\nOrin palidece. —No abran.\n\n¿Qué hacen?", ["Abrir la puerta","Interrogar a Orin","Mirar por una ventana","Preparar una emboscada"]),
 ("ceniza", "Una nieve de ceniza cae sobre el pueblo en pleno verano. Mara encuentra una pluma negra dentro de una olla que llevaba horas cerrada. Nox la reconoce y, por primera vez, pierde la sonrisa.\n\n—Si esto llegó hasta aquí, tenemos menos tiempo del que creía.\n\n¿Qué hacen?", ["Exigir respuestas a Nox","Examinar la pluma","Avisar a Eira","Salir a buscar el origen"]),
]

SCENE_BRANCHES = {
 "Abrir la puerta":("La puerta se abre. No hay ejército ni monstruo: hay una muchacha empapada sosteniendo una llave que Orin juró haber destruido hace años.\n\n—Hola, papá —dice ella. Orin retrocede.", "orin_daughter_seen"),
 "Interrogar a Orin":("Orin intenta bromear, pero nadie ríe. Finalmente coloca tres llaves sobre la mesa.\n\n—Una abre una tumba. Otra abre una prisión. La tercera... no debería abrir nada. Elegí cuál quieren que explique primero.", "orin_keys_revealed"),
 "Mirar por una ventana":("No ves a nadie frente a la puerta. La voz vuelve a hablar... pero ahora llega desde el piso superior de la Taberna.\n\nBrok susurra: —Arriba no hay habitaciones.", "impossible_floor"),
 "Preparar una emboscada":("Apagan las lámparas y toman posiciones. La puerta nunca se abre. En cambio, algo deja de respirar debajo de una de las mesas. Algo que estaba allí desde antes de los golpes.", "hidden_guest"),
 "Exigir respuestas a Nox":("Nox tarda demasiado en contestar. —Esa pluma pertenece a alguien que murió delante de mí hace nueve años. Si volvió, no viene por el pueblo. Viene por mí.", "nox_past_open"),
 "Examinar la pluma":("La pluma está tibia. Cuando la giras, tu nombre aparece escrito en el raquis con una letra que reconoces aunque nunca la hayas visto.", "marked_by_ash"),
 "Avisar a Eira":("Eira no pregunta qué ocurrió. Cierra su clínica y comienza a quemar documentos. —Reúnan a todos. Y no permitan que Elías vea esa pluma.", "eira_hides_truth"),
 "Salir a buscar el origen":("Las huellas en la ceniza salen del pueblo, no entran. Alguien —o algo— estuvo aquí antes de que empezara a caer.", "ash_tracks"),
}


CAMPAIGN_ARCS = [
 (1,"Las Campanas de Aeternus",12,"El pueblo descubre que su historia tiene huecos y alguien los está observando desde antes de la primera sesión."),
 (2,"Lo que duerme debajo",12,"Túneles, reliquias y pactos antiguos convierten el subsuelo en un segundo mapa del pueblo."),
 (3,"Sangre sobre Valdren",14,"Las decisiones locales empiezan a afectar ciudades enteras y aparecen las primeras rutas mutuamente excluyentes."),
 (4,"Los que nunca regresaron",12,"Expediciones desaparecidas, familiares perdidos y capítulos personales cambian quién confía en quién."),
 (5,"La Guerra sin Rey",15,"Facciones rivales obligan al grupo a negociar, traicionar, unir o destruir alianzas."),
 (6,"El Nombre Prohibido",12,"La verdad sobre Aeternus deja de ser un misterio arqueológico y se vuelve un peligro inmediato."),
 (7,"El Invierno de Ceniza",14,"El mundo cambia físicamente; recursos, refugios y viajes importan tanto como combatir."),
 (8,"Los Siete Juramentos",12,"Cada héroe enfrenta una deuda, promesa o secreto capaz de romper al grupo."),
 (9,"La Ciudad que Recuerda",13,"Una ciudad imposible reacciona a recuerdos y decisiones de los primeros arcos."),
 (10,"El Cielo Partido",14,"La escala pasa de reinos y facciones a amenazas que alteran las reglas conocidas del mundo."),
 (11,"Antes del Último Amanecer",12,"Regresan consecuencias sembradas durante toda la campaña; aliados y enemigos cobran sus deudas."),
 (12,"Las Cenizas de Aeternus",16,"Final modular de la primera gran saga: guerras, sacrificios y reconciliaciones según el mundo construido."),
 (13,"Después de las Cenizas",14,"El mundo sobrevivió, pero sobrevivir no significa quedar entero. Viejos aliados reclaman territorios y nacen nuevas amenazas."),
 (14,"El Mar sin Estrellas",16,"Viajes, islas imposibles, monstruos antiguos y decisiones donde retirarse también puede cambiar continentes."),
 (15,"Los Herederos del Vacío",15,"Legados, descendientes, discípulos y objetos de personajes caídos regresan al centro de la historia."),
 (16,"Donde Terminan los Dioses",18,"Saga final opcional para campañas largas: el mundo juzga dos años de decisiones y construye finales personales y colectivos."),
]
SIDE_QUESTS = [
("La casa que aparece los martes","Una casa inexistente aparece al anochecer; cada habitación recuerda a un miembro distinto del grupo."),
("El perro de la armadura","Un perro roba una pieza de equipo y conduce al grupo hasta un soldado que oficialmente murió hace veinte años."),
("Cartas para nadie","Llegan cartas dirigidas a personajes que todavía no han conocido."),
("La deuda de Brok","Brok pide ayuda y se niega a explicar a quién le debe exactamente la vida."),
("La paciente de Eira","Eira esconde a una paciente cuya existencia podría iniciar una guerra."),
("Tres tumbas vacías","Tres lápidas tienen nombres de personas que siguen vivas."),
("El mercado de los recuerdos","Un mercader compra recuerdos auténticos y paga demasiado bien por los dolorosos."),
("La campana bajo el lago","Cada medianoche suena una campana desde el fondo; alguien responde desde la orilla."),
("El impostor amable","Dos personas aseguran ser el mismo viajero y ambas conocen secretos imposibles."),
("Cena con el enemigo","Una facción hostil invita al grupo a cenar bajo tregua y cumple escrupulosamente cada regla."),
("El niño que recuerda mañana","Un niño describe decisiones que el grupo aún no ha tomado."),
("La espada cobarde","Un arma legendaria habla, pero se niega a entrar en lugares peligrosos."),
("El bosque que cambia nombres","Quien duerme allí despierta recordando un nombre distinto para alguien querido."),
("El último retrato","Una pintora puede retratar cómo morirás, aunque asegura que el futuro puede cambiar."),
("La boda equivocada","El grupo llega a una boda donde uno de ellos figura como contrayente."),
("Siete minutos de oscuridad","Durante siete minutos desaparece toda luz y algo cambia de lugar en el pueblo."),
("La posada repetida","El camino devuelve al grupo a la misma posada sin importar la dirección."),
("El coleccionista de promesas","Una criatura exige promesas en lugar de monedas."),
("El fantasma que no murió","Un supuesto fantasma insiste en que su cuerpo todavía está vivo en alguna parte."),
("El juicio de Nox","Nox es acusado de un crimen que quizá sí cometió, pero por razones que nadie conoce."),
("La mina de cristal","Mineros oyen sus propias voces pidiendo auxilio desde túneles todavía sin excavar."),
("La biblioteca hambrienta","Los libros solo revelan información si reciben otra verdad a cambio."),
("La máscara de Mara","Mara encuentra una máscara que conoce todos sus recuerdos menos uno."),
("Un día sin muertos","Durante un día nadie puede morir; las consecuencias al terminar son peores de lo esperado."),
("El pueblo que olvidó el fuego","Una aldea no reconoce las llamas y teme a quienes saben encenderlas."),
("La bestia que pide abogado","Una criatura capturada exige ser juzgada en vez de ejecutada."),
("El puente de los arrepentidos","Para cruzar, cada viajero debe abandonar algo que realmente valore."),
("La moneda de dos reyes","Una moneda imposible convierte una disputa histórica en una amenaza presente."),
("El cadáver que vota","Un consejo local incluye a un muerto que sigue participando mediante cartas selladas."),
("La noche de las cien puertas","Aparecen puertas por todo Aeternus; algunas llevan a habitaciones, otras a años distintos."),
("El músico sin sombra","Un músico cura pesadillas, pero cada canción hace desaparecer una sombra del pueblo."),
("La hija del monstruo","Una niña busca al grupo para pedir que no maten a la criatura que todos están cazando."),
("El mapa tatuado","Un desconocido lleva tatuado un mapa que cambia cuando el grupo toma decisiones."),
("La taberna vacía","Todos desaparecen de la taberna salvo los jugadores y una persona que asegura ser Brok."),
("El ladrón de cicatrices","Alguien roba cicatrices y con ellas los recuerdos de cómo fueron obtenidas."),
("El funeral anticipado","El grupo recibe invitaciones para el funeral de uno de sus miembros dentro de tres días."),
("Los seis juramentos menores","Seis favores aparentemente pequeños pueden convertirse en aliados o enemigos meses después."),
("La criatura debajo de la cama","Una misión absurda empieza como broma y termina conectada con un antagonista principal."),
("El torneo sin armas","Una ciudad resuelve conflictos con pruebas sociales, acertijos, carreras y engaños."),
("Flores para un caído","Un NPC deja flores en una tumba del cementerio y revela una relación que el grupo desconocía."),
]

TRAVEL_ENCOUNTERS = [
"Una carreta abandonada todavía está caliente, pero no hay huellas alrededor.",
"Un cuervo repite exactamente la última frase privada de uno de los aventureros.",
"Dos caminos tienen el mismo letrero y ambos aseguran ser el original.",
"Un caballero herido pide agua antes de admitir de qué ejército forma parte.",
"Una lluvia breve envejece únicamente las flores.",
"Un mercader ofrece comprar un objeto que el grupo todavía no posee.",
"Una criatura pequeña sigue al grupo y solo huye cuando alguien pronuncia el nombre de Nox.",
"Encuentran una fogata encendida con tantas sillas como viajeros hay en el grupo.",
"Un puente cobra peaje en historias verdaderas, no en dinero.",
"Una estatua cambia de postura cada vez que nadie la mira.",
]

CLASS_PROLOGUES = {
"nigromante":"Una voz conocida pronuncia tu nombre desde una tumba cerrada. El problema: reconoces a alguien que aún está vivo.",
"cazador_demonios":"El demonio que perseguías deja de huir y te entrega voluntariamente el nombre de una persona de Aeternus.",
"paladin":"Tu juramento responde por primera vez con una pregunta en lugar de una orden.",
"picaro":"Robas una llave sin saber que abre una puerta que todavía no existe.",
"mago":"Una página aparece en tu grimorio escrita con tu letra, fechada un año en el futuro.",
"bruja":"Algo acepta un pacto que nunca recuerdas haber ofrecido.",
"guerrero":"Tu vieja arma aparece clavada frente a Aeternus aunque jurarías tenerla contigo.",
"bardo":"Escuchas una canción sobre tu muerte y el último verso todavía no ha sido escrito.",
}

CAMPAIGN_TOTAL_CHAPTERS=sum(x[2] for x in CAMPAIGN_ARCS)

def _db(): return _DB()

def _schema():
    c=_db()
    c.execute("""CREATE TABLE IF NOT EXISTS dnd_campaigns(id BIGSERIAL PRIMARY KEY,chat_id BIGINT NOT NULL,thread_id BIGINT NOT NULL DEFAULT 0,name TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'active',arc BIGINT NOT NULL DEFAULT 1,chapter BIGINT NOT NULL DEFAULT 1,scene BIGINT NOT NULL DEFAULT 1,scene_key TEXT DEFAULT '',scene_text TEXT DEFAULT '',flags TEXT NOT NULL DEFAULT '{}',created_by BIGINT NOT NULL,created_at BIGINT NOT NULL,updated_at BIGINT NOT NULL,UNIQUE(chat_id,thread_id))""")
    c.execute("""CREATE TABLE IF NOT EXISTS dnd_characters(id BIGSERIAL PRIMARY KEY,campaign_id BIGINT NOT NULL,user_id BIGINT NOT NULL,telegram_name TEXT DEFAULT '',name TEXT NOT NULL,class_key TEXT NOT NULL,class_name TEXT NOT NULL,background TEXT DEFAULT '',appearance TEXT DEFAULT '',level BIGINT NOT NULL DEFAULT 1,xp BIGINT NOT NULL DEFAULT 0,hp BIGINT NOT NULL DEFAULT 20,max_hp BIGINT NOT NULL DEFAULT 20,str BIGINT NOT NULL DEFAULT 10,dex BIGINT NOT NULL DEFAULT 10,con BIGINT NOT NULL DEFAULT 10,intel BIGINT NOT NULL DEFAULT 10,wis BIGINT NOT NULL DEFAULT 10,cha BIGINT NOT NULL DEFAULT 10,status TEXT NOT NULL DEFAULT 'active',joined_at BIGINT NOT NULL,updated_at BIGINT NOT NULL,UNIQUE(campaign_id,user_id))""")
    c.execute("""CREATE TABLE IF NOT EXISTS dnd_memories(id BIGSERIAL PRIMARY KEY,campaign_id BIGINT NOT NULL,user_id BIGINT NOT NULL DEFAULT 0,npc_key TEXT DEFAULT '',kind TEXT NOT NULL,mem_key TEXT NOT NULL,value TEXT DEFAULT '',weight BIGINT NOT NULL DEFAULT 0,created_at BIGINT NOT NULL,UNIQUE(campaign_id,user_id,npc_key,mem_key))""")
    c.execute("""CREATE TABLE IF NOT EXISTS dnd_journal(id BIGSERIAL PRIMARY KEY,campaign_id BIGINT NOT NULL,chapter BIGINT NOT NULL,actor_id BIGINT NOT NULL DEFAULT 0,event_type TEXT NOT NULL,text TEXT NOT NULL,created_at BIGINT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS dnd_pending_rolls(campaign_id BIGINT PRIMARY KEY,user_id BIGINT NOT NULL,reason TEXT NOT NULL,stat TEXT DEFAULT '',needed BIGINT NOT NULL DEFAULT 1,current BIGINT NOT NULL DEFAULT 0,rolls TEXT NOT NULL DEFAULT '[]',difficulty BIGINT NOT NULL DEFAULT 10,created_at BIGINT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS dnd_inventory(id BIGSERIAL PRIMARY KEY,campaign_id BIGINT NOT NULL,user_id BIGINT NOT NULL,item_key TEXT NOT NULL,name TEXT NOT NULL,qty BIGINT NOT NULL DEFAULT 1,rarity TEXT NOT NULL DEFAULT 'comun',description TEXT DEFAULT '',equipped BIGINT NOT NULL DEFAULT 0,created_at BIGINT NOT NULL,UNIQUE(campaign_id,user_id,item_key))""")
    c.execute("""CREATE TABLE IF NOT EXISTS dnd_relationships(campaign_id BIGINT NOT NULL,user_id BIGINT NOT NULL,npc_key TEXT NOT NULL,trust BIGINT NOT NULL DEFAULT 0,affection BIGINT NOT NULL DEFAULT 0,respect BIGINT NOT NULL DEFAULT 0,fear BIGINT NOT NULL DEFAULT 0,debt BIGINT NOT NULL DEFAULT 0,last_event TEXT DEFAULT '',updated_at BIGINT NOT NULL,PRIMARY KEY(campaign_id,user_id,npc_key))""")
    c.execute("""CREATE TABLE IF NOT EXISTS dnd_secrets(id BIGSERIAL PRIMARY KEY,campaign_id BIGINT NOT NULL,owner_id BIGINT NOT NULL DEFAULT 0,secret_key TEXT NOT NULL,text TEXT NOT NULL,revealed BIGINT NOT NULL DEFAULT 0,created_at BIGINT NOT NULL,UNIQUE(campaign_id,owner_id,secret_key))""")
    c.execute("""CREATE TABLE IF NOT EXISTS dnd_quests(id BIGSERIAL PRIMARY KEY,campaign_id BIGINT NOT NULL,quest_key TEXT NOT NULL,title TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'active',description TEXT DEFAULT '',stakes TEXT DEFAULT '',updated_at BIGINT NOT NULL,UNIQUE(campaign_id,quest_key))""")
    c.execute("""CREATE TABLE IF NOT EXISTS dnd_world_clock(campaign_id BIGINT PRIMARY KEY,day BIGINT NOT NULL DEFAULT 1,hour BIGINT NOT NULL DEFAULT 18,weather TEXT NOT NULL DEFAULT 'nublado',danger BIGINT NOT NULL DEFAULT 1,updated_at BIGINT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS dnd_death_saves(campaign_id BIGINT NOT NULL,user_id BIGINT NOT NULL,successes BIGINT NOT NULL DEFAULT 0,failures BIGINT NOT NULL DEFAULT 0,status TEXT NOT NULL DEFAULT 'stable',cause TEXT DEFAULT '',updated_at BIGINT NOT NULL,PRIMARY KEY(campaign_id,user_id))""")
    c.execute("""CREATE TABLE IF NOT EXISTS dnd_graveyard(id BIGSERIAL PRIMARY KEY,campaign_id BIGINT NOT NULL,user_id BIGINT NOT NULL,character_name TEXT NOT NULL,class_name TEXT NOT NULL,level BIGINT NOT NULL DEFAULT 1,cause TEXT DEFAULT '',chapter BIGINT NOT NULL DEFAULT 1,arc BIGINT NOT NULL DEFAULT 1,world_day BIGINT NOT NULL DEFAULT 1,campaign_days BIGINT NOT NULL DEFAULT 0,last_words TEXT DEFAULT '',legacy TEXT DEFAULT '',died_at BIGINT NOT NULL,revived BIGINT NOT NULL DEFAULT 0,UNIQUE(campaign_id,user_id,character_name,died_at))""")
    c.execute("""CREATE TABLE IF NOT EXISTS dnd_injuries(id BIGSERIAL PRIMARY KEY,campaign_id BIGINT NOT NULL,user_id BIGINT NOT NULL,severity TEXT NOT NULL,name TEXT NOT NULL,description TEXT DEFAULT '',healed BIGINT NOT NULL DEFAULT 0,created_at BIGINT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS dnd_reactions(id BIGSERIAL PRIMARY KEY,campaign_id BIGINT NOT NULL,actor_id BIGINT NOT NULL,target_id BIGINT NOT NULL,action_text TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',created_at BIGINT NOT NULL,resolved_at BIGINT NOT NULL DEFAULT 0)""")
    c.execute("""CREATE TABLE IF NOT EXISTS dnd_sidequest_pool(campaign_id BIGINT NOT NULL,quest_key TEXT NOT NULL,title TEXT NOT NULL,description TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'locked',created_at BIGINT NOT NULL,PRIMARY KEY(campaign_id,quest_key))""")
    c.commit(); c.close()

def _topic(message): return int(message.get('message_thread_id') or 0)
def _campaign(chat_id,thread_id):
    _schema(); c=_db(); r=c.execute("SELECT * FROM dnd_campaigns WHERE chat_id=? AND thread_id=? AND status='active'",(int(chat_id),int(thread_id))).fetchone(); c.close(); return r

def _campaign_any(chat_id,thread_id):
    _schema(); c=_db(); r=c.execute("SELECT * FROM dnd_campaigns WHERE chat_id=? AND thread_id=?",(int(chat_id),int(thread_id))).fetchone(); c.close(); return r

def _char(cid,uid):
    c=_db(); r=c.execute("SELECT * FROM dnd_characters WHERE campaign_id=? AND user_id=?",(int(cid),int(uid))).fetchone(); c.close(); return r

def _mention(user):
    u=(user or {}).get('username'); name=(user or {}).get('first_name') or 'Aventurero'
    return '@'+u if u else name

def _menu():
    return {"inline_keyboard":[
      [{"text":"🎭 Mi ficha","callback_data":"dnd:sheet"},{"text":"👥 Grupo","callback_data":"dnd:party"}],
      [{"text":"📖 Historia","callback_data":"dnd:journal"},{"text":"🌍 Estado del mundo","callback_data":"dnd:world"}],
      [{"text":"🪦 Cementerio","callback_data":"dnd:graveyard"}],
      [{"text":"🎒 Inventario","callback_data":"dnd:inventory"},{"text":"🗡️ Misiones","callback_data":"dnd:quests"}],
      [{"text":"🤝 Relaciones","callback_data":"dnd:relations"},{"text":"🔐 Secretos","callback_data":"dnd:secrets"}],
      [{"text":"🎨 Retrato IA","callback_data":"dnd:portrait"},{"text":"🛏️ Descansar","callback_data":"dnd:rest"}],
      [{"text":"📚 Manual","callback_data":"dnd:manual"},{"text":"⚙️ Controles","callback_data":"dnd:controls"}],
      [{"text":"🔄 Reiniciar prueba","callback_data":"dnd:reset"}]
    ]}

def _choice_kb(options):
    rows=[]
    for i,opt in enumerate(options[:8]): rows.append([{"text":opt,"callback_data":f"dnd:choice:{i}"}])
    rows.append([{"text":"✍️ Escribir otra acción","callback_data":"dnd:free"}])
    return {"inline_keyboard":rows}

def _manual():
    return ("📚 KIW D&D — MANUAL RÁPIDO\n\n"
      "🎯 Aquí no ganas por encontrar el botón correcto. Describe lo que tu personaje intenta hacer. Los botones son atajos; escribir siempre sigue permitido.\n\n"
      "🎲 DADOS: el Director puede pedir 1, 2, 3 o más dados reales de Telegram. El bot anuncia DADO 1/N y no acepta el siguiente hasta registrar el anterior. No toda acción necesita tirada.\n\n"
      "🧠 DECISIONES: pueden cambiar relaciones, lugares, NPC, misiones, secretos y capítulos futuros. Algunas consecuencias son inmediatas; otras pueden volver meses después.\n\n"
      "👥 1 O MUCHOS JUGADORES: funciona desde una sola persona. Si juegas solo, el Director reduce presión y puede ofrecer compañeros NPC. /dndunirme incorpora gente después sin reiniciar la historia. Las @menciones permiten acciones conjuntas o contra otros personajes, pero nadie decide automáticamente por otro jugador.\n\n"
      "🎭 PERSONAJE: clase, atributos, trasfondo, apariencia, heridas, recuerdos y secretos son persistentes. Puedes elegir una clase del catálogo o escribir una propia; las clases personalizadas se normalizan para no romper el balance.\n\n"
      "⚔️ COMBATE: iniciativa, acciones, defensa, habilidades y consecuencias narrativas. Fracasar no siempre significa morir: puede abrir otra ruta.\n\n"
      "☠️ MUERTE: una tirada peligrosa puede herirte o llevarte a 0 HP. A 0 HP entras en AGONÍA y haces salvaciones con el dado real. Tres éxitos estabilizan; tres fallos causan muerte definitiva. Los compañeros pueden intervenir antes del final.\n\n"
      f"📚 CAMPAÑA: {len(CAMPAIGN_ARCS)} arcos y {CAMPAIGN_TOTAL_CHAPTERS} capítulos estructurales, además de rutas alternativas, capítulos personales y secundarias. Está pensada para aproximadamente 18–24 meses a ritmo regular.\n\n"
      "🕯️ INFORMACIÓN: tu personaje solo sabe lo que ha aprendido. El grupo no recibe automáticamente tus secretos.\n\n"
      "🧭 MUNDO VIVO: el tiempo, amenazas y misiones pueden avanzar. Ignorar algo también cuenta como decisión.\n\n🤝 RELACIONES: confianza, afecto, respeto, miedo y deudas son independientes. Un NPC puede odiarte y aun así respetar tu palabra.\n\n🔐 SECRETOS: hay información personal que solo conoce su dueño hasta que decida revelarla o la historia la exponga.\n\n🪦 CEMENTERIO: cada muerte definitiva crea una lápida permanente con personaje, clase, nivel, causa, capítulo y días vividos en la campaña. La historia no borra a sus muertos.\n\n⚡ COMANDOS: /dnd · /dndcrear · /dndunirme · /dndficha · /dndgrupo · /dndhistoria · /dndestado · /dndinventario · /dndmisiones · /dndrelaciones · /dndsecretos · /dndcementerio · /dnddescansar · /dndmanual · /dndpausa · /dndreanudar · /dndsecundarias · /dndreiniciar")

def _sheet(ch):
    if not ch: return "🎭 Aún no tienes personaje en esta campaña. Usa /dndunirme."
    return (f"🎭 {ch['name']}\n👤 {ch['telegram_name']}\n⚔️ {ch['class_name']} · Nv. {ch['level']}\n📜 {ch.get('background') or 'Trasfondo por descubrir'}\n\n"
      f"❤️ {ch['hp']}/{ch['max_hp']} HP\n💪 FUE {ch['str']} · 🏹 DES {ch['dex']} · ❤️ CON {ch['con']}\n🧠 INT {ch['intel']} · 👁️ SAB {ch['wis']} · 🎭 CAR {ch['cha']}\n\n"
      f"🎨 Apariencia: {ch.get('appearance') or 'Aún no descrita.'}")

def _party(camp):
    c=_db(); rows=c.execute("SELECT * FROM dnd_characters WHERE campaign_id=? AND status='active' ORDER BY joined_at",(int(camp['id']),)).fetchall(); c.close()
    if not rows: return "👥 Todavía no hay aventureros."
    return "👥 GRUPO\n\n"+"\n".join(f"• {r['telegram_name']} — {r['name']} · {r['class_name']} Nv.{r['level']}" for r in rows)

def _journal(camp):
    c=_db(); rows=c.execute("SELECT text FROM dnd_journal WHERE campaign_id=? ORDER BY id DESC LIMIT 12",(int(camp['id']),)).fetchall(); c.close()
    return "📖 CRÓNICA RECIENTE\n\n"+("\n\n".join('• '+r['text'] for r in reversed(rows)) if rows else "La historia apenas está comenzando.")

def _graveyard(camp):
    c=_db(); rows=c.execute("SELECT * FROM dnd_graveyard WHERE campaign_id=? ORDER BY died_at DESC",(int(camp['id']),)).fetchall(); c.close()
    if not rows: return "🪦 CEMENTERIO DE LA CAMPAÑA\n\nTodavía no hay lápidas. Que siga así... mientras puedan."
    out=["🪦 CEMENTERIO DE LA CAMPAÑA\n", f"Aquí descansan quienes formaron parte de {camp['name']}. Sus decisiones siguen siendo canon.\n"]
    for r in rows[:30]:
        mark=" ✨ REGRESÓ" if int(r.get('revived') or 0) else ""
        out.append(f"🕯️ {r['character_name']}{mark}\n⚔️ {r['class_name']} — Nivel {r['level']}\n☠️ {r.get('cause') or 'Causa perdida entre las crónicas'}\n📖 Arco {r['arc']} · Capítulo {r['chapter']} · Día del mundo {r['world_day']}\n⏳ {r['campaign_days']} días en la campaña" + (f"\n💬 Últimas palabras: «{r['last_words']}»" if r.get('last_words') else ""))
    return "\n\n".join(out)

def _world(camp):
    flags=json.loads(camp.get('flags') or '{}')
    return (f"🌍 ESTADO DEL MUNDO\n\n📖 Arco {camp['arc']} · Capítulo {camp['chapter']} · Escena {camp['scene']}\n"
            f"🧭 Campaña: {camp['name']}\n🕯️ Huellas persistentes: {len(flags)}\n📚 Plan maestro: {CAMPAIGN_TOTAL_CHAPTERS} capítulos / {len(CAMPAIGN_ARCS)} arcos\n\nEl mundo recuerda lo que hicieron, lo que prometieron y también lo que decidieron ignorar.")

def _inventory(camp,uid):
    c=_db(); rows=c.execute("SELECT * FROM dnd_inventory WHERE campaign_id=? AND user_id=? ORDER BY rarity DESC,name",(int(camp['id']),int(uid))).fetchall(); c.close()
    if not rows: return "🎒 INVENTARIO\n\nTodavía no llevas objetos especiales. El equipo de D&D nunca usa el inventario de KiwRPG."
    return "🎒 INVENTARIO\n\n"+"\n".join(f"{'🟢' if r['equipped'] else '▫️'} {r['name']} x{r['qty']} · {r['rarity']}\n   {r.get('description') or ''}" for r in rows)

def _quests(camp):
    c=_db(); rows=c.execute("SELECT * FROM dnd_quests WHERE campaign_id=? AND status='active' ORDER BY id",(int(camp['id']),)).fetchall(); c.close()
    if not rows: return "🗡️ MISIONES\n\nNo hay una misión marcada como activa. Eso no significa que el mundo esté quieto."
    return "🗡️ MISIONES ACTIVAS\n\n"+"\n\n".join(f"• {r['title']}\n{r.get('description') or ''}\n⚠️ {r.get('stakes') or 'Consecuencias desconocidas'}" for r in rows)

def _relations(camp,uid):
    c=_db(); rows=c.execute("SELECT * FROM dnd_relationships WHERE campaign_id=? AND user_id=? ORDER BY npc_key",(int(camp['id']),int(uid))).fetchall(); c.close()
    if not rows: return "🤝 RELACIONES\n\nTus vínculos todavía se están formando. El juego guarda confianza, afecto, respeto, temor y deudas por separado."
    def mood(r):
        bits=[]
        if r['trust']>=3: bits.append('confía en ti')
        elif r['trust']<=-3: bits.append('desconfía de ti')
        if r['respect']>=3: bits.append('te respeta')
        if r['affection']>=3: bits.append('te aprecia')
        if r['fear']>=3: bits.append('te teme')
        if r['debt']>0: bits.append('te debe algo')
        return ', '.join(bits) or 'relación incierta'
    return "🤝 RELACIONES\n\n"+"\n".join(f"• {r['npc_key'].title()}: {mood(r)}" for r in rows)

def _secrets(camp,uid):
    c=_db(); rows=c.execute("SELECT text FROM dnd_secrets WHERE campaign_id=? AND owner_id=? AND revealed=0 ORDER BY id",(int(camp['id']),int(uid))).fetchall(); c.close()
    return "🔐 TUS SECRETOS\n\n"+("\n\n".join('• '+r['text'] for r in rows) if rows else "No tienes secretos personales registrados... todavía.")

def _rest(camp,ch):
    if not ch: return "🎭 Primero necesitas un personaje."
    heal=max(1,int(ch['max_hp'])//3); newhp=min(int(ch['max_hp']),int(ch['hp'])+heal); now=int(time.time())
    c=_db(); c.execute("UPDATE dnd_characters SET hp=?,updated_at=? WHERE id=?",(newhp,now,int(ch['id'])))
    c.execute("INSERT INTO dnd_journal(campaign_id,chapter,actor_id,event_type,text,created_at) VALUES(?,?,?,?,?,?)",(int(camp['id']),int(camp['chapter']),int(ch['user_id']),'rest',f"{ch['telegram_name']} descansó y recuperó fuerzas.",now)); c.commit(); c.close()
    return f"🛏️ DESCANSO\n\nRecuperas {newhp-int(ch['hp'])} HP.\n❤️ {newhp}/{ch['max_hp']}\n\nDescansar puede hacer avanzar el mundo; no siempre será gratis narrativamente."

def _seed_campaign(camp):
    now=int(time.time()); c=_db()
    c.execute("INSERT INTO dnd_world_clock(campaign_id,day,hour,weather,danger,updated_at) VALUES(?,1,22,'lluvia de ceniza',1,?) ON CONFLICT(campaign_id) DO NOTHING",(int(camp['id']),now))
    c.execute("INSERT INTO dnd_quests(campaign_id,quest_key,title,status,description,stakes,updated_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(campaign_id,quest_key) DO NOTHING",(int(camp['id']),'first_grieta','La primera grieta','active','Descubrir por qué Aeternus está mostrando señales que contradicen su propia historia.','Lo que ignoren seguirá avanzando sin ustedes.',now))
    for i,(title,desc) in enumerate(SIDE_QUESTS):
        c.execute("INSERT INTO dnd_sidequest_pool(campaign_id,quest_key,title,description,status,created_at) VALUES(?,?,?,?,?,?) ON CONFLICT(campaign_id,quest_key) DO NOTHING",(int(camp['id']),f'sq_{i+1:03d}',title,desc,'available' if i<3 else 'locked',now))
    c.commit(); c.close()

def _create_character(camp,user,class_key='guerrero',custom_name=None):
    now=int(time.time()); ck=class_key if class_key in CLASSES else 'custom'; cn=CLASSES.get(class_key,(custom_name or 'Clase personalizada',''))[0]
    # Perfiles distintos sin convertir la creación en min-max automático.
    stats={'str':10,'dex':10,'con':10,'intel':10,'wis':10,'cha':10}; hp=20
    boosts={'guerrero':('str','con'),'mago':('intel','wis'),'picaro':('dex','cha'),'paladin':('str','cha'),'bardo':('cha','dex'),'druida':('wis','con'),'monje':('dex','wis'),'hechicero':('cha','con'),'nigromante':('intel','wis'),'cazador_demonios':('dex','wis'),'caballero_sangre':('str','con'),'invocador':('intel','cha'),'exorcista':('wis','cha'),'domador_bestias':('wis','dex'),'espadachin_arcano':('dex','intel'),'artificiero':('intel','dex'),'bruja':('cha','wis'),'explorador':('dex','wis')}
    a,b=boosts.get(class_key,('dex','cha')); stats[a]=15; stats[b]=14; stats['con']=max(stats['con'],12); hp=24 if a=='str' or b=='con' else 20
    tg=_mention(user); pname=(user.get('first_name') or 'Aventurero')
    c=_db(); c.execute("""INSERT INTO dnd_characters(campaign_id,user_id,telegram_name,name,class_key,class_name,level,hp,max_hp,str,dex,con,intel,wis,cha,joined_at,updated_at) VALUES(?,?,?,?,?,?,1,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(campaign_id,user_id) DO UPDATE SET telegram_name=EXCLUDED.telegram_name,updated_at=EXCLUDED.updated_at""",(int(camp['id']),int(user['id']),tg,pname,ck,cn,hp,hp,stats['str'],stats['dex'],stats['con'],stats['intel'],stats['wis'],stats['cha'],now,now)); c.commit(); c.close()
    return _char(camp['id'],user['id'])

def _class_keyboard():
    keys=list(CLASSES); rows=[]
    for i in range(0,len(keys),2):
        rows.append([{"text":CLASSES[k][0],"callback_data":f"dnd:class:{k}"} for k in keys[i:i+2]])
    rows.append([{"text":"✍️ Quiero otra clase","callback_data":"dnd:customclass"}])
    return {"inline_keyboard":rows}

def _reset_keyboard():
    return {"inline_keyboard":[[{"text":"⚠️ Sí, borrar SOLO esta campaña D&D","callback_data":"dnd:resetconfirm"}],[{"text":"❌ Cancelar","callback_data":"dnd:controls"}]]}

def _reset_campaign(camp):
    cid=int(camp['id']); c=_db()
    tables=['dnd_pending_rolls','dnd_death_saves','dnd_injuries','dnd_reactions','dnd_inventory','dnd_relationships','dnd_secrets','dnd_quests','dnd_sidequest_pool','dnd_memories','dnd_journal','dnd_graveyard','dnd_characters','dnd_world_clock']
    for table in tables: c.execute(f"DELETE FROM {table} WHERE campaign_id=?",(cid,))
    scene=random.choice(OPENING_SCENES); now=int(time.time())
    c.execute("UPDATE dnd_campaigns SET status='active',arc=1,chapter=1,scene=1,scene_key=?,scene_text=?,flags='{}',updated_at=? WHERE id=?",(scene[0],scene[1],now,cid)); c.commit(); c.close()
    fresh=_campaign(int(camp['chat_id']),int(camp.get('thread_id') or 0)); _seed_campaign(fresh)

def _find_mentioned_characters(camp,text,actor_id):
    names={m.lower() for m in re.findall(r'@([A-Za-z0-9_]{3,})', text or '')}
    if not names: return []
    c=_db(); rows=c.execute("SELECT * FROM dnd_characters WHERE campaign_id=? AND status IN ('active','dying')",(int(camp['id']),)).fetchall(); c.close()
    out=[]
    for r in rows:
        tg=str(r.get('telegram_name') or '').lstrip('@').lower()
        if tg in names and int(r['user_id'])!=int(actor_id): out.append(r)
    return out

def _is_interactive_action(text):
    low=(text or '').lower()
    verbs=('aviento','abiento','empujo','agarro','arrastro','golpeo','ataco','curo','protejo','escondo detrás','le paso','entrego','lanzo a','tiro a','quito','robo a','cargo a','salvo a')
    return any(v in low for v in verbs)

def _solo_note(camp):
    c=_db(); n=c.execute("SELECT COUNT(*) n FROM dnd_characters WHERE campaign_id=? AND status='active'",(int(camp['id']),)).fetchone(); c.close()
    return int((n or {}).get('n') or 0)<=1

def handle_command(message,text):
    cmd=(text.split()[0].split('@')[0].lower() if text else '')
    if not cmd.startswith('/dnd'): return False
    chat=message.get('chat') or {}; uid=int((message.get('from') or {}).get('id') or 0); chat_id=int(chat.get('id') or 0); thread=_topic(message); user=message.get('from') or {}
    if cmd in ('/dndcrear','/dndiniciar'):
        if chat.get('type')=='private': _S(chat_id,thread,"🐉 Crea la campaña dentro del tema del grupo donde quieran jugar."); return True
        _schema(); now=int(time.time()); c=_db(); existing=c.execute("SELECT * FROM dnd_campaigns WHERE chat_id=? AND thread_id=?",(chat_id,thread)).fetchone()
        if existing and existing['status']=='active': c.close(); _S(chat_id,thread,"🐉 Ya existe una campaña KiwD&D activa EN ESTE TEMA.\n\nUsa /dnd para abrir sus controles.",reply_markup=_menu()); return True
        name='Las Cenizas de Aeternus'; scene=random.choice(OPENING_SCENES)
        if existing:
            c.execute("UPDATE dnd_campaigns SET status='active',name=?,scene_key=?,scene_text=?,updated_at=? WHERE id=?",(name,scene[0],scene[1],now,int(existing['id'])))
        else:
            c.execute("INSERT INTO dnd_campaigns(chat_id,thread_id,name,scene_key,scene_text,created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",(chat_id,thread,name,scene[0],scene[1],uid,now,now))
        c.commit(); c.close(); camp=_campaign(chat_id,thread)
        _seed_campaign(camp)
        _S(chat_id,thread,f"🐉 KIW D&D — {name}\n\nEsta campaña queda vinculada EXCLUSIVAMENTE a este tema. Sus decisiones y controles no avanzarán desde otra pestaña.\n\nPrimero crea tu aventurero con /dndunirme.",reply_markup=_menu()); return True
    if cmd=='/dndreanudar':
        anycamp=_campaign_any(chat_id,thread)
        if not anycamp: _S(chat_id,thread,"🐉 No existe una campaña en este tema para reanudar."); return True
        if uid!=int(anycamp['created_by']): _S(chat_id,thread,"⚙️ Solo quien creó la campaña puede reanudarla."); return True
        c=_db(); c.execute("UPDATE dnd_campaigns SET status='active',updated_at=? WHERE id=?",(int(time.time()),int(anycamp['id']))); c.commit(); c.close(); _S(chat_id,thread,"▶️ Campaña reanudada. El mundo vuelve a moverse.",reply_markup=_menu()); return True
    camp=_campaign(chat_id,thread)
    if not camp:
        _S(chat_id,thread,"🐉 No hay una campaña KiwD&D activa en ESTE tema.\n\nUsa /dndcrear aquí para convertir este tema en la mesa de juego."); return True
    if cmd in ('/dnd','/dndmenu'): _S(chat_id,thread,f"🐉 {camp['name']}\n📖 Arco {camp['arc']} · Capítulo {camp['chapter']}\n\nElige un control o escribe una acción cuando la escena esté activa.",reply_markup=_menu()); return True
    if cmd in ('/dndunirme','/dndcrearpersonaje'):
        if _char(camp['id'],uid): _S(chat_id,thread,"🎭 Ya tienes personaje en esta campaña.",reply_markup=_menu()); return True
        _S(chat_id,thread,f"🎭 {_mention(user)}, elige tu clase.\n\nEsto define capacidades y conocimientos, no tus decisiones. También puedes crear una clase propia.",reply_markup=_class_keyboard()); return True
    if cmd=='/dndficha': _S(chat_id,thread,_sheet(_char(camp['id'],uid)),reply_markup=_menu()); return True
    if cmd=='/dndgrupo': _S(chat_id,thread,_party(camp),reply_markup=_menu()); return True
    if cmd=='/dndhistoria': _S(chat_id,thread,_journal(camp),reply_markup=_menu()); return True
    if cmd in ('/dndcementerio','/dndtumbas'): _S(chat_id,thread,_graveyard(camp),reply_markup=_menu()); return True
    if cmd in ('/dndestado','/dndmundo'): _S(chat_id,thread,_world(camp),reply_markup=_menu()); return True
    if cmd=='/dndinventario': _S(chat_id,thread,_inventory(camp,uid),reply_markup=_menu()); return True
    if cmd=='/dndmisiones': _S(chat_id,thread,_quests(camp),reply_markup=_menu()); return True
    if cmd=='/dndrelaciones': _S(chat_id,thread,_relations(camp,uid),reply_markup=_menu()); return True
    if cmd=='/dndsecretos': _S(chat_id,thread,_secrets(camp,uid),reply_markup=_menu()); return True
    if cmd=='/dnddescansar': _S(chat_id,thread,_rest(camp,_char(camp['id'],uid)),reply_markup=_menu()); return True
    if cmd=='/dndmanual': _S(chat_id,thread,_manual(),reply_markup=_menu()); return True
    if cmd in ('/dndcampana','/dndarcos'): _S(chat_id,thread,_campaign_plan(),reply_markup=_menu()); return True
    if cmd in ('/dndsecundarias','/dndsidequests'):
        c=_db(); rows=c.execute("SELECT title,description,status FROM dnd_sidequest_pool WHERE campaign_id=? AND status IN ('available','active') ORDER BY quest_key LIMIT 12",(int(camp['id']),)).fetchall(); c.close(); txt='🧭 MISIONES SECUNDARIAS\n\n'+('\n\n'.join(f"• {r['title']} — {r['description']}" for r in rows) if rows else 'No hay secundarias visibles ahora.'); _S(chat_id,thread,txt,reply_markup=_menu()); return True
    if cmd=='/dndreiniciar':
        if uid!=int(camp['created_by']): _S(chat_id,thread,'⚙️ Solo quien creó esta campaña puede reiniciarla.'); return True
        _S(chat_id,thread,'⚠️ REINICIAR CAMPAÑA DE PRUEBA\n\nEsto borrará SOLO los datos KiwD&D de ESTE tema: personajes, progreso, tumbas, secretos, relaciones, misiones y tiradas. No toca KiwRPG, PiPesos ni otros temas.\n\n¿Seguro?',reply_markup=_reset_keyboard()); return True
    if cmd=='/dndretrato':
        ch=_char(camp['id'],uid)
        if not ch: _S(chat_id,thread,"🎭 Primero crea tu personaje con /dndunirme."); return True
        if not _PHOTO: _S(chat_id,thread,"🎨 El generador de retratos no está configurado."); return True
        try: _PHOTO(chat_id,ch,thread)
        except Exception as e: _S(chat_id,thread,f"🎨 No pude generar el retrato: {e}")
        return True
    if cmd=='/dndcomenzar':
        ch=_char(camp['id'],uid)
        if not ch: _S(chat_id,thread,"Primero usa /dndunirme."); return True
        scene=next((s for s in OPENING_SCENES if s[0]==camp['scene_key']),OPENING_SCENES[0]); _S(chat_id,thread,"🎬 CAPÍTULO I — LA PRIMERA GRIETA\n\n"+camp['scene_text'],reply_markup=_choice_kb(scene[2])); return True
    if cmd=='/dndpausa':
        if uid!=int(camp['created_by']): _S(chat_id,thread,"⚙️ Solo quien creó la campaña puede pausarla."); return True
        c=_db(); c.execute("UPDATE dnd_campaigns SET status='paused',updated_at=? WHERE id=?",(int(time.time()),int(camp['id']))); c.commit(); c.close(); _S(chat_id,thread,"⏸️ Campaña pausada. Nada de este tema avanzará hasta reanudarla."); return True
    _S(chat_id,thread,"🐉 Comando D&D reconocido, pero esa función aún no está disponible. Usa /dndmanual."); return True

def handle_callback(query):
    data=str(query.get('data') or '')
    if not data.startswith('dnd:'): return False
    msg=query.get('message') or {}; chat_id=int((msg.get('chat') or {}).get('id') or 0); thread=int(msg.get('message_thread_id') or 0); user=query.get('from') or {}; uid=int(user.get('id') or 0); camp=_campaign(chat_id,thread)
    if not camp: _S(chat_id,thread,"🐉 Esta campaña no pertenece a este tema."); return True
    action=data.split(':',1)[1]
    if action=='sheet': _S(chat_id,thread,_sheet(_char(camp['id'],uid)),reply_markup=_menu()); return True
    if action=='party': _S(chat_id,thread,_party(camp),reply_markup=_menu()); return True
    if action=='journal': _S(chat_id,thread,_journal(camp),reply_markup=_menu()); return True
    if action=='graveyard': _S(chat_id,thread,_graveyard(camp),reply_markup=_menu()); return True
    if action=='world': _S(chat_id,thread,_world(camp),reply_markup=_menu()); return True
    if action=='manual': _S(chat_id,thread,_manual(),reply_markup=_menu()); return True
    if action=='inventory': _S(chat_id,thread,_inventory(camp,uid),reply_markup=_menu()); return True
    if action=='quests': _S(chat_id,thread,_quests(camp),reply_markup=_menu()); return True
    if action=='relations': _S(chat_id,thread,_relations(camp,uid),reply_markup=_menu()); return True
    if action=='secrets': _S(chat_id,thread,_secrets(camp,uid),reply_markup=_menu()); return True
    if action=='rest': _S(chat_id,thread,_rest(camp,_char(camp['id'],uid)),reply_markup=_menu()); return True
    if action=='portrait':
        ch=_char(camp['id'],uid)
        if not ch: _S(chat_id,thread,"🎭 Primero crea tu personaje."); return True
        if not _PHOTO: _S(chat_id,thread,"🎨 El generador de retratos no está configurado."); return True
        try: _PHOTO(chat_id,ch,thread)
        except Exception as e: _S(chat_id,thread,f"🎨 No pude generar el retrato: {e}")
        return True
    if action=='reset':
        if uid!=int(camp['created_by']): _S(chat_id,thread,'⚙️ Solo quien creó esta campaña puede reiniciarla.'); return True
        _S(chat_id,thread,'⚠️ REINICIAR CAMPAÑA DE PRUEBA\n\nBorrará SOLO KiwD&D de este tema. No toca KiwRPG ni PiPesos. ¿Seguro?',reply_markup=_reset_keyboard()); return True
    if action=='resetconfirm':
        if uid!=int(camp['created_by']): _S(chat_id,thread,'⚙️ Reinicio cancelado: no eres quien creó la campaña.'); return True
        _reset_campaign(camp); _S(chat_id,thread,'🔄 KiwD&D reiniciado. Este tema volvió al día 1 sin tocar nada fuera de esta campaña.\n\nUsa /dndunirme para comenzar otra prueba.',reply_markup=_menu()); return True
    if action=='controls': _S(chat_id,thread,"⚙️ CONTROLES\n\n▶️ /dndcomenzar\n🎭 /dndficha\n👥 /dndgrupo\n📖 /dndhistoria\n🪦 /dndcementerio\n🌍 /dndestado\n🎒 /dndinventario\n🗡️ /dndmisiones\n🤝 /dndrelaciones\n🔐 /dndsecretos\n🛏️ /dnddescansar\n📚 /dndmanual\n🗺️ /dndcampana\n⏸️ /dndpausa\n▶️ /dndreanudar\n🧭 /dndsecundarias\n🔄 /dndreiniciar\n\nTodo funciona únicamente en este tema. El reinicio requiere confirmación y solo borra esta campaña D&D.",reply_markup=_menu()); return True
    if action=='free': _S(chat_id,thread,f"✍️ {_mention(user)}, escribe exactamente lo que tu personaje quiere intentar. No necesitas usar una frase especial."); return True
    if action=='customclass': _S(chat_id,thread,f"✍️ {_mention(user)}, escribe: `Clase: <lo que quieras>`\nEjemplo: Clase: samurái que utiliza magia de sangre.\n\nKiwD&D conservará el concepto pero lo llevará a estadísticas jugables."); return True
    if action.startswith('class:'):
        key=action.split(':',1)[1]; ch=_create_character(camp,user,key)
        _S(chat_id,thread,f"✨ {_mention(user)} entra en la campaña.\n\n{_sheet(ch)}\n\nAhora describe tu apariencia escribiendo:\nApariencia: <descripción>\n\nCuando estén listos: /dndcomenzar",reply_markup=_menu()); return True
    if action.startswith('choice:'):
        ch=_char(camp['id'],uid)
        if not ch: _S(chat_id,thread,"🎭 Primero crea tu personaje con /dndunirme."); return True
        try: idx=int(action.split(':')[1])
        except: idx=-1
        scene=next((s for s in OPENING_SCENES if s[0]==camp['scene_key']),OPENING_SCENES[0]); opts=scene[2]
        if idx<0 or idx>=len(opts): return True
        choice=opts[idx]; consequence,flag=SCENE_BRANCHES.get(choice,("La decisión cambia el rumbo de la escena.","choice_"+str(idx)))
        flags=json.loads(camp.get('flags') or '{}'); flags[flag]={'by':uid,'at':int(time.time()),'choice':choice}
        now=int(time.time()); c=_db(); c.execute("UPDATE dnd_campaigns SET flags=?,scene=scene+1,updated_at=? WHERE id=?",(json.dumps(flags,ensure_ascii=False),now,int(camp['id']))); c.execute("INSERT INTO dnd_journal(campaign_id,chapter,actor_id,event_type,text,created_at) VALUES(?,?,?,?,?,?)",(int(camp['id']),int(camp['chapter']),uid,'decision',f"{_mention(user)} decidió: {choice}.",now)); c.commit(); c.close()
        _S(chat_id,thread,f"🕯️ {_mention(user)} — {choice}\n\n{consequence}\n\nLa campaña ha guardado esta decisión. Sus consecuencias no tienen por qué terminar en esta escena.\n\n✍️ Pueden escribir qué hacen ahora.",reply_markup=_menu()); return True
    if action.startswith('react:'):
        parts=action.split(':'); reaction=parts[1] if len(parts)>1 else 'resist'; actor=int(parts[2]) if len(parts)>2 and parts[2].isdigit() else 0
        c=_db(); r=c.execute("SELECT * FROM dnd_reactions WHERE campaign_id=? AND actor_id=? AND target_id=? AND status='pending' ORDER BY id DESC LIMIT 1",(int(camp['id']),actor,uid)).fetchone()
        if not r: c.close(); _S(chat_id,thread,'🎭 Esa reacción ya no está pendiente o no te corresponde.'); return True
        c.execute("UPDATE dnd_reactions SET status=?,resolved_at=? WHERE id=?",(reaction,int(time.time()),int(r['id']))); c.commit(); c.close()
        labels={'accept':'acepta participar','avoid':'intenta apartarse','resist':'se resiste'}; _S(chat_id,thread,f"🎭 {_mention(user)} {labels.get(reaction,'reacciona')}.\n\nLa escena respeta ambas decisiones. Si hay oposición física o riesgo, el Director puede pedir una tirada enfrentada; también pueden escribir cualquier otra reacción.",reply_markup=_menu()); return True
    return True

def _death_state(camp,uid):
    c=_db(); r=c.execute("SELECT * FROM dnd_death_saves WHERE campaign_id=? AND user_id=?",(int(camp['id']),int(uid))).fetchone(); c.close(); return r

def _enter_agony(camp,ch,cause):
    now=int(time.time()); c=_db()
    c.execute("UPDATE dnd_characters SET hp=0,status='dying',updated_at=? WHERE id=?",(now,int(ch['id'])))
    c.execute("INSERT INTO dnd_death_saves(campaign_id,user_id,successes,failures,status,cause,updated_at) VALUES(?,?,0,0,'dying',?,?) ON CONFLICT(campaign_id,user_id) DO UPDATE SET successes=0,failures=0,status='dying',cause=EXCLUDED.cause,updated_at=EXCLUDED.updated_at",(int(camp['id']),int(ch['user_id']),cause[:500],now))
    c.execute("INSERT INTO dnd_journal(campaign_id,chapter,actor_id,event_type,text,created_at) VALUES(?,?,?,?,?,?)",(int(camp['id']),int(camp['chapter']),int(ch['user_id']),'agony',f"{ch['telegram_name']} cayó en agonía: {cause[:300]}",now)); c.commit(); c.close()

def _resolve_death_die(camp,user,val):
    uid=int(user['id']); st=_death_state(camp,uid)
    if not st or st['status']!='dying': return None
    succ=int(st['successes']); fail=int(st['failures'])
    if val==6: succ+=2
    elif val>=4: succ+=1
    elif val==1: fail+=2
    else: fail+=1
    now=int(time.time()); c=_db()
    if succ>=3:
        c.execute("UPDATE dnd_death_saves SET successes=?,failures=?,status='stable',updated_at=? WHERE campaign_id=? AND user_id=?",(succ,fail,now,int(camp['id']),uid))
        c.execute("UPDATE dnd_characters SET hp=1,status='active',updated_at=? WHERE campaign_id=? AND user_id=?",(now,int(camp['id']),uid)); c.commit(); c.close()
        return f"🕯️ SALVACIÓN — {val}\n\n✨ TRES ÉXITOS. {_mention(user)} se estabiliza con 1 HP. Sobrevive, pero la escena puede dejar una herida o cicatriz permanente."
    if fail>=3:
        c.execute("UPDATE dnd_death_saves SET successes=?,failures=?,status='dead',updated_at=? WHERE campaign_id=? AND user_id=?",(succ,fail,now,int(camp['id']),uid))
        ch=c.execute("SELECT * FROM dnd_characters WHERE campaign_id=? AND user_id=?",(int(camp['id']),uid)).fetchone()
        c.execute("UPDATE dnd_characters SET hp=0,status='dead',updated_at=? WHERE campaign_id=? AND user_id=?",(now,int(camp['id']),uid))
        wc=c.execute("SELECT day FROM dnd_world_clock WHERE campaign_id=?",(int(camp['id']),)).fetchone(); world_day=int((wc or {}).get('day') or 1)
        lived=max(0,(now-int(camp.get('created_at') or now))//86400)
        cause=str(st.get('cause') or 'Murió durante la aventura')[:500]
        if ch:
            c.execute("INSERT INTO dnd_graveyard(campaign_id,user_id,character_name,class_name,level,cause,chapter,arc,world_day,campaign_days,died_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",(int(camp['id']),uid,ch['name'],ch['class_name'],int(ch['level']),cause,int(camp['chapter']),int(camp['arc']),world_day,lived,now))
        c.execute("INSERT INTO dnd_journal(campaign_id,chapter,actor_id,event_type,text,created_at) VALUES(?,?,?,?,?,?)",(int(camp['id']),int(camp['chapter']),uid,'death',f"{_mention(user)} murió durante el capítulo {camp['chapter']}. Su lápida fue añadida al cementerio y su historia permanece en el canon.",now)); c.commit(); c.close()
        nm=(ch['name'] if ch else _mention(user)); cl=(ch['class_name'] if ch else 'Aventurero'); lv=(ch['level'] if ch else '?')
        return f"☠️ TERCER FALLO.\n\n{_mention(user)} ha muerto.\n\n🪦 UNA NUEVA LÁPIDA APARECE EN EL CEMENTERIO\n\n🕯️ {nm}\n⚔️ {cl} — Nivel {lv}\n☠️ {cause}\n📖 Arco {camp['arc']} · Capítulo {camp['chapter']}\n⏳ {lived} días en la campaña\n\nSu personaje queda para siempre en la historia. Sus promesas, relaciones, objetos y consecuencias no desaparecen. Usa /dndcementerio para visitar a los caídos."
    c.execute("UPDATE dnd_death_saves SET successes=?,failures=?,updated_at=? WHERE campaign_id=? AND user_id=?",(succ,fail,now,int(camp['id']),uid)); c.commit(); c.close()
    return f"💀 SALVACIÓN CONTRA LA MUERTE — {val}\n\n✨ Éxitos: {succ}/3\n☠️ Fallos: {fail}/3\n\n🎲 {_mention(user)}, todavía estás en agonía. Lanza otro dado cuando llegue tu siguiente salvación. Tus compañeros pueden intentar ayudarte antes."

def _campaign_plan():
    lines=[f"📚 CAMPAÑA DE LARGO RECORRIDO\n\n{CAMPAIGN_TOTAL_CHAPTERS} capítulos estructurales · {len(CAMPAIGN_ARCS)} arcos · objetivo 18–24 meses.\n"]
    for n,name,chap,desc in CAMPAIGN_ARCS:
        lines.append(f"\n{n}. {name} — {chap} capítulos\n{desc}")
    lines.append("\n\nCada capítulo tiene rutas alternativas y no todas vuelven al mismo punto. Además existen capítulos personales, secundarias, consecuencias diferidas y escenas generadas desde el estado persistente del mundo.")
    return ''.join(lines)

def _request_roll(camp,user,reason,needed=1,difficulty=10,stat=''):
    c=_db(); c.execute("""INSERT INTO dnd_pending_rolls(campaign_id,user_id,reason,stat,needed,current,rolls,difficulty,created_at) VALUES(?,?,?,?,?,0,'[]',?,?) ON CONFLICT(campaign_id) DO UPDATE SET user_id=EXCLUDED.user_id,reason=EXCLUDED.reason,stat=EXCLUDED.stat,needed=EXCLUDED.needed,current=0,rolls='[]',difficulty=EXCLUDED.difficulty,created_at=EXCLUDED.created_at""",(int(camp['id']),int(user['id']),reason,stat,int(needed),int(difficulty),int(time.time()))); c.commit(); c.close()
    _S(int(camp['chat_id']),int(camp.get('thread_id') or 0),f"🎲 {_mention(user)} — {reason}\nNecesito {needed} dado{'s' if needed!=1 else ''}.\n\n🎲 DADO 1/{needed} — lánzalo en Telegram.")

def handle_message(message,text):
    chat_id=int((message.get('chat') or {}).get('id') or 0); thread=_topic(message); user=message.get('from') or {}; uid=int(user.get('id') or 0); camp=_campaign(chat_id,thread)
    if not camp: return False
    # Dados pendientes de D&D tienen prioridad dentro de SU tema.
    if message.get('dice'):
        val=int((message.get('dice') or {}).get('value') or 0)
        ds=_death_state(camp,uid)
        if ds and ds['status']=='dying':
            result=_resolve_death_die(camp,user,val); _S(chat_id,thread,result,reply_markup=_menu()); return True
        c=_db(); p=c.execute("SELECT * FROM dnd_pending_rolls WHERE campaign_id=?",(int(camp['id']),)).fetchone()
        if not p: c.close(); return False
        if int(p['user_id'])!=uid: c.close(); _S(chat_id,thread,f"🎲 Ese dado no era para ti. Estamos esperando a otro aventurero."); return True
        rolls=json.loads(p.get('rolls') or '[]'); rolls.append(val); cur=len(rolls); needed=int(p['needed'])
        if cur<needed:
            c.execute("UPDATE dnd_pending_rolls SET current=?,rolls=? WHERE campaign_id=?",(cur,json.dumps(rolls),int(camp['id']))); c.commit(); c.close(); _S(chat_id,thread,f"🎲 DADO {cur}/{needed}: {val}\n\n🎲 DADO {cur+1}/{needed} — {_mention(user)}, lánzalo."); return True
        c.execute("DELETE FROM dnd_pending_rolls WHERE campaign_id=?",(int(camp['id']),)); c.commit(); c.close(); total=sum(rolls); target=int(p['difficulty'])*needed; success=total>=target
        outcome=("La situación se inclina a tu favor, pero el mundo registra cómo lo lograste." if success else "No sale como esperabas. No es un GAME OVER: la historia toma una ruta más peligrosa.")
        if _NARRATE:
            try: outcome=_NARRATE(camp,_char(camp['id'],uid),str(p.get('reason') or ''),{"success":success,"rolls":rolls,"total":total,"target":target}) or outcome
            except Exception: pass
        now=int(time.time()); cj=_db(); cj.execute("INSERT INTO dnd_journal(campaign_id,chapter,actor_id,event_type,text,created_at) VALUES(?,?,?,?,?,?)",(int(camp['id']),int(camp['chapter']),uid,'roll_result',f"{_mention(user)} intentó {str(p.get('reason') or '')[:350]} — {'éxito' if success else 'consecuencia'} ({total}/{target}).",now)); cj.commit(); cj.close()
        reason_low=str(p.get('reason') or '').lower(); lethal=any(w in reason_low for w in ('salto al vac','me sacrific','recibo el golpe','boss','lava','abismo','explos','caigo','caída','veneno mortal'))
        if (not success) and lethal and (all(v<=2 for v in rolls) or total<=needed*2):
            ch_now=_char(camp['id'],uid); _enter_agony(camp,ch_now,str(p.get('reason') or 'tirada letal'))
            _S(chat_id,thread,f"🎲 TIRADA COMPLETA — {' + '.join(map(str,rolls))} = {total}\n\n💀 EL FALLO ES LETAL. {_mention(user)} cae a 0 HP y entra en AGONÍA.\n\n🎲 En tu siguiente dado harás una SALVACIÓN CONTRA LA MUERTE. Tres éxitos estabilizan; tres fallos significan muerte. El grupo puede intentar salvarte antes.",reply_markup=_menu()); return True
        _S(chat_id,thread,f"🎲 TIRADA COMPLETA — {' + '.join(map(str,rolls))} = {total}\n🎯 Umbral narrativo: {target}\n\n{'✨ ÉXITO' if success else '⚠️ CONSECUENCIA'}\n\n{outcome}",reply_markup=_menu()); return True
    t=(text or '').strip()
    if not t or t.startswith('/'): return False
    ch=_char(camp['id'],uid)
    low=t.lower()
    if not ch and low.startswith('clase:'):
        concept=t.split(':',1)[1].strip()[:120]
        if not concept:
            _S(chat_id,thread,"✍️ Escribe algo después de `Clase:`."); return True
        ch=_create_character(camp,user,'custom',concept.title())
        _S(chat_id,thread,f"✨ Clase personalizada creada: {concept.title()}\n\nLa idea se conserva como identidad narrativa y usa una base equilibrada de atributos.\n\n{_sheet(ch)}\n\nAhora puedes escribir: Apariencia: <cómo quieres verte>",reply_markup=_menu()); return True
    if not ch: return False
    if low.startswith('apariencia:'):
        appearance=t.split(':',1)[1].strip()[:1000]; c=_db(); c.execute("UPDATE dnd_characters SET appearance=?,updated_at=? WHERE campaign_id=? AND user_id=?",(appearance,int(time.time()),int(camp['id']),uid)); c.commit(); c.close(); _S(chat_id,thread,"🎨 Apariencia guardada. Será la referencia canónica para el retrato de tu personaje."); return True
    # @menciones: entendemos acciones sociales incluso con ortografía imperfecta, pero un jugador nunca decide por otro.
    mentioned=_find_mentioned_characters(camp,t,uid)
    if mentioned and _is_interactive_action(t):
        target=mentioned[0]; now=int(time.time()); c=_db(); c.execute("INSERT INTO dnd_reactions(campaign_id,actor_id,target_id,action_text,status,created_at) VALUES(?,?,?,?, 'pending',?)",(int(camp['id']),uid,int(target['user_id']),t[:700],now)); c.commit(); c.close()
        kb={"inline_keyboard":[[{"text":"🤼 Resistirme","callback_data":f"dnd:react:resist:{uid}"},{"text":"🤝 Aceptar","callback_data":f"dnd:react:accept:{uid}"}],[{"text":"🏃 Apartarme","callback_data":f"dnd:react:avoid:{uid}"},{"text":"✍️ Otra reacción","callback_data":"dnd:free"}]]}
        _S(chat_id,thread,f"🎭 {_mention(user)} intenta: «{t[:350]}»\n\n🎯 {target['telegram_name']} está involucrado. Nadie controla automáticamente a otro personaje: le toca reaccionar.",reply_markup=kb); return True
    # Acción libre: heurística tolerante. Errores comunes como abiento/aviento no invalidan la intención.
    risky=any(w in low for w in ('ataco','golpeo','salto','trepo','robo','fuerzo','rompo','persigo','seduz','engaño','miento','intimido','convenzo','escapo','escondo','desarmo','aviento','abiento','empujo'))
    if risky:
        needed=2 if any(w in low for w in ('seduz','engaño','intimido','persigo','desarmo')) else 1
        _S(chat_id,thread,f"🎭 {_mention(user)} intenta: «{t[:350]}»\n\nLa intención es válida y puede cambiar la escena.")
        _request_roll(camp,user,t[:500],needed=needed,difficulty=3 if needed==1 else 4,stat=''); return True
    now=int(time.time()); c=_db(); c.execute("INSERT INTO dnd_journal(campaign_id,chapter,actor_id,event_type,text,created_at) VALUES(?,?,?,?,?,?)",(int(camp['id']),int(camp['chapter']),uid,'free_action',f"{_mention(user)}: {t[:700]}",now)); c.commit(); c.close()
    narration="La escena registra tu acción. Los demás pueden responder, tomar otra ruta o contradecirte; no están obligados a seguirla."
    if _NARRATE:
        try: narration=_NARRATE(camp,ch,t[:700],None) or narration
        except Exception: pass
    _S(chat_id,thread,f"🎭 {_mention(user)}\n\n{narration}",reply_markup=_menu()); return True
