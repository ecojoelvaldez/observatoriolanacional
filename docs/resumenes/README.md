# Resúmenes de noticias en documento (días anteriores al observatorio)

Los resúmenes de julio de 2026 y anteriores no se produjeron con este sitio, así
que se publican como archivo suelto: **PDF o Word (.docx)**. El calendario de
**Resúmenes pasados** los enlaza automáticamente.

## Cómo subirlos

1. Nombra cada archivo con la fecha del resumen:

   ```
   Resumen de Noticias. 2026-07-16.pdf
   Resumen de Noticias. 2026-07-17.docx
   ```

   Es decir: `Resumen de Noticias. AAAA-MM-DD` + `.pdf` o `.docx`.
2. Copia los archivos en esta misma carpeta (`docs/resumenes/`) y haz commit.
3. Listo. No hace falta tocar el HTML ni el JSON.

Puedes mezclar formatos sin problema: unos días en PDF y otros en Word.
Los PDF se muestran con visor incrustado dentro del sitio; los `.docx` no se
pueden previsualizar en el navegador, así que se ofrecen para descarga.

## El nombre no tiene que ser exacto

El sitio prueba varias formas del nombre antes de descartar un día, así que
estas también funcionan:

| Variante | Ejemplo |
| --- | --- |
| Con punto (la acordada) | `Resumen de Noticias. 2026-07-16.pdf` |
| Sin punto | `Resumen de Noticias 2026-07-16.pdf` |
| Sin espacio | `Resumen de Noticias.2026-07-16.pdf` |
| Con guion | `Resumen de Noticias - 2026-07-16.pdf` |
| Con guion bajo | `Resumen de Noticias_2026-07-16.docx` |
| Minúscula en "noticias" | `Resumen de noticias. 2026-07-16.pdf` |
| Todo en minúscula y con guiones | `resumen-de-noticias-2026-07-16.pdf` |

Lo que **sí** tiene que ser exacto es la fecha, en formato `AAAA-MM-DD`, y la
extensión (`.pdf` o `.docx`). En cuanto un día resuelve, el sitio recuerda esa
forma y la prueba primero en los demás días.

Además, **solo se marcan en el calendario los días cuyo archivo existe de
verdad**: el sitio lo comprueba antes de pintar. Puedes ir subiendo los
resúmenes de a poco y nunca habrá un botón que lleve a un archivo inexistente.

## Días declarados

`data/resumenes_pdf.json` trae los 23 días hábiles de julio de 2026
(1–3, 6–10, 13–17, 20–24 y 27–31). Los fines de semana no se incluyen.

Para agregar otro mes, añade sus fechas a la lista `dates`:

```json
{
  "base": "docs/resumenes/",
  "dates": ["2026-06-29", "2026-06-30", "2026-07-01"],
  "files": {}
}
```

## Si un archivo se sale del patrón

Solo en ese caso hace falta declararlo en `files`, con el nombre exacto (o una
lista de nombres a probar):

```json
{
  "files": {
    "2026-07-08": "Resumen semanal (8 de julio).pdf",
    "2026-07-09": ["Resumen de Noticias. 2026-07-09 v2.docx", "Resumen 09-07.docx"]
  }
}
```

## Si prefieres alojarlos en Supabase Storage

Sube los archivos al bucket público y pon en `data/resumenes_pdf.json` la URL
base del bucket:

```json
{
  "base": "https://<tu-proyecto>.supabase.co/storage/v1/object/public/resumenes/",
  "dates": ["2026-07-01"],
  "files": {}
}
```

También puedes poner una URL completa en cada entrada de `files` si los archivos
no siguen ningún patrón.
