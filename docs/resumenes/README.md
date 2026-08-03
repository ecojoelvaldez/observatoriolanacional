# Resúmenes de noticias en PDF (días anteriores al observatorio)

Los resúmenes de julio de 2026 y anteriores no se produjeron con este sitio, así
que se publican como PDF. El calendario de **Resúmenes pasados** los enlaza
automáticamente.

## Cómo subirlos

1. Nombra cada archivo con la fecha del resumen, en formato `resumen-AAAA-MM-DD.pdf`.
   Ejemplo: el resumen del jueves 16 de julio de 2026 es `resumen-2026-07-16.pdf`.
2. Copia los PDF en esta misma carpeta (`docs/resumenes/`) y haz commit.
3. Listo. No hace falta tocar el HTML.

El calendario comprueba cada archivo antes de marcar el día: **solo se marcan y
se hacen clicables los días cuyo PDF esté realmente subido.** Si un día no tiene
resumen, simplemente no lo subas y el calendario lo dejará apagado.

## Qué días están declarados

`data/resumenes_pdf.json` trae los 23 días hábiles de julio de 2026
(1–3, 6–10, 13–17, 20–24 y 27–31). Los fines de semana no se incluyen.

Para agregar otro mes, añade sus fechas al bloque `files` de ese archivo:

```json
{
  "base": "docs/resumenes/",
  "files": {
    "2026-06-30": "resumen-2026-06-30.pdf",
    "2026-07-01": "resumen-2026-07-01.pdf"
  }
}
```

## Si prefieres alojarlos en Supabase Storage

En vez de subir los archivos al repositorio, sube los PDF al bucket público y
pon en `data/resumenes_pdf.json` la URL base del bucket:

```json
{
  "base": "https://<tu-proyecto>.supabase.co/storage/v1/object/public/resumenes/",
  "files": { "2026-07-01": "resumen-2026-07-01.pdf" }
}
```

También puedes poner una URL completa en cada entrada si los nombres no siguen
un patrón fijo:

```json
{ "files": { "2026-07-01": "https://.../loquesea.pdf" } }
```
