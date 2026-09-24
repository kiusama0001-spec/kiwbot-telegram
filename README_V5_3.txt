KiwRPG V5.3 — Universo + sistema visual

Sube TODO el contenido de esta carpeta a la raíz del proyecto, conservando:
main.py
rpg_catalog.json
assets_manifest.json
assets/

Qué hace:
- Mantiene V5.2.
- /rpg muestra V5.3.
- Sistema de assets con cache de file_id de Telegram.
- /encuentro intenta mostrar la imagen del enemigo si existe en el manifest.
- Creación de personaje intenta mostrar arte de clase.
- Mini App ya puede mostrar arte del Guerrero.
- Si una imagen todavía no existe, el bot cae automáticamente al mensaje normal.
- No requiere cambios manuales en Supabase.

Para añadir arte:
1. Copia la imagen dentro de assets/.
2. Añade su ID y ruta a assets_manifest.json.
Ejemplo:
"enemy:lobo_ceniza": "enemies/lobo_ceniza.png"
"enemy:lobo_ceniza:rare": "enemies/lobo_ceniza_raro.png"

Telegram guardará el file_id tras el primer envío.
