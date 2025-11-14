#!/usr/bin/env python3
# gen_forms.py
import os, json
from jinja2 import Environment, FileSystemLoader, select_autoescape
import psycopg2
import psycopg2.extras

# ---------------- CONFIG - Rellena con tus datos ----------------
DB = {
    "host": "172.23.0.8",
    "port": 5432,
    "dbname": "EIEL",
    "user": "cguillen",
    "password": "passSV8"
}
TEMPLATE_DIR = "templates"
TEMPLATE_AGUA = "form-agua-template.html.j2"
TEMPLATE_OBRAS = "form-obras-template.html.j2"
# Ruta de salida final que me diste (Windows). Asegúrate de que existe o se creará.
OUT_DIR = r"C:\Users\cguillen.GEONET\Documents\GitHub\eiel-prototipo\formularios"
# Opcional: mapping fichero (code[TAB]name). Si no existe, se usa el código como nombre.
MUNICIPIOS_TSV = "municipios.tsv"

# Opcional: URLs que se inyectarán en la plantilla (deja en blanco o pon las tuyas)
URL_APPS_SCRIPT = "https://script.google.com/macros/s/AKfycbwZqswRuGBHfzPV1CwoGVW8QMRZBW5KJ4WVJ68gRVxfmn9N9BO5_VyDo4n25NiSXXwfUw/exec"
URL_GOOGLE_FORMS = "https://docs.google.com/forms/d/e/1FAIpQLSc84PLY4O2wM9ek3v6L14DzZ8jcqDtFeKOK01i38s7ttPt0Ng/formResponse"
# ---------------- END CONFIG -----------------------------------

env = Environment(
    loader=FileSystemLoader(TEMPLATE_DIR, encoding="utf-8"),
    autoescape=select_autoescape(['html','xml'])
)
template_agua = env.get_template(TEMPLATE_AGUA)
template_obras = env.get_template(TEMPLATE_OBRAS)

def conectar():
    conn = psycopg2.connect(
        host=DB["host"], port=DB["port"], dbname=DB["dbname"],
        user=DB["user"], password=DB["password"], client_encoding='UTF8'
    )
    return conn

def cargar_mapado_municipios(path):
    d = {}
    if not os.path.exists(path):
        return d
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line=line.strip()
            if not line or line.startswith("#"): continue
            parts = line.split("\t")
            if len(parts)>=2:
                code = parts[0].strip()
                name = parts[1].strip()
                d[code] = name
    return d

def obtener_municipios(conn):
    try:
        cur = conn.cursor()
        # Obtenemos los códigos de municipios presentes en deposito_enc (fase último)
        cur.execute("""
            SELECT DISTINCT mun
            FROM municipio
            WHERE fase = (SELECT max(fase) FROM geonet_fase) and prov = '03'
            ORDER BY mun;
        """)
        rows = cur.fetchall()
        cur.close()
        return [r[0] for r in rows]
    except psycopg2.Error as e:
        print(f"❌ Error BD en municipio {mun}: {e}")
        return []  # Devolver lista vacía si falla

def obtener_depositos(conn, mun):
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
            SELECT d.mun, d.orden_depo, d.nombre, de.limpieza
            FROM deposito d
            LEFT JOIN deposito_enc de USING (fase, mun, orden_depo)
            WHERE d.fase = (SELECT max(fase) FROM geonet_fase) AND d.mun = %s
            ORDER BY d.orden_depo;
        """, (mun,))
        rows = cur.fetchall()
        cur.close()
        
        depositos = []
        for r in rows:
            depositos.append({
                "nombre": r["nombre"] if r["nombre"] is not None else "",
                "limpieza": str(r["limpieza"]) if r["limpieza"] is not None else ""
            })
        return depositos
        
    except psycopg2.Error as e:
        print(f"❌ Error BD en municipio {mun}: {e}")
        return []  # Devolver lista vacía si falla

def obtener_obras(conn, mun):
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Condición 1
        cur.execute("""
            SELECT mun, orden, nombre, plan_obra, 1 as cond
            FROM geonet_obras
            WHERE fase = (SELECT max(fase) FROM geonet_fase) 
            AND (estado IS NULL OR estado NOT IN ('FI','AN'))
            AND (equipamientos IS NULL OR equipamientos = 'SI'
                OR alumbrado IS NULL OR alumbrado = 'SI'
                OR infra_viaria IS NULL OR infra_viaria = 'SI'
                OR abastecimiento IS NULL OR abastecimiento = 'SI'
                OR saneamiento IS NULL OR saneamiento = 'SI')
            AND (proyecto IS NULL OR proyecto <> 'SI')
            AND mun = %s
        """, (mun,))
        c1 = cur.fetchall()

        # Condición 2
        cur.execute("""
            SELECT mun, orden, nombre, plan_obra, 2 as cond
            FROM geonet_obras
            WHERE fase = (SELECT max(fase) FROM geonet_fase) 
            AND estado = 'FI'
            AND (equipamientos IS NULL OR equipamientos = 'SI'
                OR alumbrado IS NULL OR alumbrado = 'SI'
                OR infra_viaria IS NULL OR infra_viaria = 'SI'
                OR abastecimiento IS NULL OR abastecimiento = 'SI'
                OR saneamiento IS NULL OR saneamiento = 'SI')
            AND (proyecto IS NULL OR proyecto <> 'SI')
            AND mun = %s
        """, (mun,))
        c2 = cur.fetchall()
        cur.close()

        obras = []
        for r in (c1 + c2):
            obras.append({
                "nombre": r["nombre"],
                "plan_obra": r["plan_obra"],
                "cond": r["cond"]
            })
        return obras
    
    except psycopg2.Error as e:
        print(f"❌ Error BD en municipio {mun}: {e}")
        return []  # Devolver lista vacía si falla

    
def asegurar_carpeta(path):
    os.makedirs(path, exist_ok=True)

def main():
    asegurar_carpeta(OUT_DIR)
    mmap = cargar_mapado_municipios(MUNICIPIOS_TSV)
    print("Mapa municipios cargado:", len(mmap), "entradas (si hay)")

    conn = conectar()
    try:
        municipios = obtener_municipios(conn)
        print("Municipios detectados en BD:", municipios)
        for mun in municipios:
            # formatea codigo con 3 dígitos si es numérico corto
            mun_code = str(mun).zfill(3)
            muni_display = mmap.get(mun_code, mun_code)  # nombre si existe en TSV, si no usa código

            # ---- AGUA ----
            depositos = obtener_depositos(conn, mun)
            depositos_json = json.dumps(depositos, ensure_ascii=False)
            
            # ---- OBRAS ----
            obras = obtener_obras(conn, mun)
            #print(f"DEBUG {mun_code} obras:", obras)
            obras_json = json.dumps(obras, ensure_ascii=False)

            rendered_agua = template_agua.render(
                muni_code = mun_code,
                muni_display = muni_display,
                depositos_json = depositos_json,
                url_apps_script = URL_APPS_SCRIPT,
                url_google_forms = URL_GOOGLE_FORMS
            )
            
            fname_agua = f"agua_{mun_code}.html"
            outpath_agua = os.path.join(OUT_DIR, fname_agua)
            with open(outpath_agua, "w", encoding="utf-8") as f:
                f.write(rendered_agua)
            print("Generado:", outpath_agua)
            
            rendered_obras = template_obras.render(
                muni_code = mun_code,
                muni_display = muni_display,
                obras = obras,
                obras_json = obras_json,
                url_apps_script = URL_APPS_SCRIPT,
                url_google_forms = URL_GOOGLE_FORMS
            )
           
            fname_obras = f"obras_{mun_code}.html"
            outpath_obras = os.path.join(OUT_DIR, fname_obras)
            with open(outpath_obras, "w", encoding="utf-8") as f:
                f.write(rendered_obras)
            print("Generado:", outpath_obras)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
