"""
RiesgoECU - Verificación de integridad y reporte
"""

import json
import psycopg2
from datetime import datetime

DB_CONFIG = {
    "host": "aws-1-us-east-2.pooler.supabase.com",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres.vkqqveyaaijuwidpxyhq",
    "password": "1002432845",
}


def verificar():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    reporte = {"fecha": datetime.now().isoformat(), "tablas": {}, "integridad": {}, "estadisticas": {}}

    # ============================
    # 1. Conteo de registros
    # ============================
    print("=" * 60)
    print("  RiesgoECU - VERIFICACIÓN DE DATOS")
    print("=" * 60)

    tablas = [
        "parroquias", "eventos_historicos", "amenaza_sismica",
        "amenaza_volcanica", "amenaza_hidro", "poblacion_inec", "indice_riesgo",
    ]

    total_registros = 0
    for tabla in tablas:
        cur.execute(f"SELECT COUNT(*) FROM {tabla}")
        count = cur.fetchone()[0]
        reporte["tablas"][tabla] = count
        total_registros += count
        print(f"  {tabla:25s} {count:>8,} registros")

    print(f"  {'TOTAL':25s} {total_registros:>8,} registros")
    print()

    # ============================
    # 2. Integridad referencial
    # ============================
    print("INTEGRIDAD REFERENCIAL:")

    # Eventos sin parroquia válida
    cur.execute("""
        SELECT COUNT(*) FROM eventos_historicos e
        LEFT JOIN parroquias p ON e.codigo_parroquia = p.codigo_parroquia
        WHERE p.codigo_parroquia IS NULL
    """)
    huerfanos_eventos = cur.fetchone()[0]
    reporte["integridad"]["eventos_sin_parroquia"] = huerfanos_eventos
    print(f"  Eventos sin parroquia válida: {huerfanos_eventos}")

    # Parroquias con geometría
    cur.execute("SELECT COUNT(*) FROM parroquias WHERE geom IS NOT NULL")
    con_geom = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM parroquias")
    total_parr = cur.fetchone()[0]
    reporte["integridad"]["parroquias_con_geometria"] = con_geom
    reporte["integridad"]["parroquias_total"] = total_parr
    print(f"  Parroquias con geometría: {con_geom}/{total_parr}")

    # Cobertura de índice de riesgo
    cur.execute("SELECT COUNT(*) FROM indice_riesgo")
    con_indice = cur.fetchone()[0]
    reporte["integridad"]["cobertura_indice"] = f"{con_indice}/{total_parr}"
    print(f"  Cobertura índice de riesgo: {con_indice}/{total_parr}")
    print()

    # ============================
    # 3. Estadísticas detalladas
    # ============================
    print("ESTADÍSTICAS:")

    # Provincias
    cur.execute("SELECT COUNT(DISTINCT provincia) FROM parroquias")
    n_prov = cur.fetchone()[0]
    reporte["estadisticas"]["provincias"] = n_prov
    print(f"  Provincias: {n_prov}")

    # Tipos de evento
    cur.execute("""
        SELECT tipo_evento, COUNT(*), SUM(muertos), SUM(heridos), SUM(viviendas_afectadas)
        FROM eventos_historicos
        GROUP BY tipo_evento ORDER BY COUNT(*) DESC
    """)
    print(f"\n  {'TIPO EVENTO':25s} {'EVENTOS':>8s} {'MUERTOS':>8s} {'HERIDOS':>8s} {'VIVIENDAS':>10s}")
    print("  " + "-" * 65)
    eventos_stats = {}
    for tipo, cnt, muertos, heridos, viv in cur.fetchall():
        print(f"  {tipo:25s} {cnt:>8,} {muertos:>8,} {heridos:>8,} {viv:>10,}")
        eventos_stats[tipo] = {"eventos": cnt, "muertos": muertos, "heridos": heridos, "viviendas": viv}
    reporte["estadisticas"]["eventos_por_tipo"] = eventos_stats

    # Distribución de riesgo
    cur.execute("""
        SELECT clasificacion, COUNT(*),
               ROUND(AVG(riesgo_total)::numeric, 2),
               ROUND(MIN(riesgo_total)::numeric, 2),
               ROUND(MAX(riesgo_total)::numeric, 2)
        FROM indice_riesgo GROUP BY clasificacion ORDER BY AVG(riesgo_total) DESC
    """)
    print(f"\n  {'CLASIFICACIÓN':15s} {'PARROQUIAS':>11s} {'PROMEDIO':>9s} {'MÍN':>6s} {'MÁX':>6s}")
    print("  " + "-" * 50)
    riesgo_stats = {}
    for clasif, cnt, avg, mn, mx in cur.fetchall():
        print(f"  {clasif:15s} {cnt:>11,} {float(avg):>9.2f} {float(mn):>6.2f} {float(mx):>6.2f}")
        riesgo_stats[clasif] = {"parroquias": cnt, "promedio": float(avg), "min": float(mn), "max": float(mx)}
    reporte["estadisticas"]["riesgo_distribucion"] = riesgo_stats

    # Amenaza sísmica por nivel
    cur.execute("SELECT nivel, COUNT(*) FROM amenaza_sismica GROUP BY nivel ORDER BY nivel")
    print(f"\n  AMENAZA SÍSMICA:")
    for nivel, cnt in cur.fetchall():
        print(f"    {nivel}: {cnt} parroquias")

    # Top 5 parroquias más riesgosas
    cur.execute("""
        SELECT p.nombre, p.provincia, ir.riesgo_total, ir.clasificacion
        FROM indice_riesgo ir
        JOIN parroquias p ON ir.codigo_parroquia = p.codigo_parroquia
        ORDER BY ir.riesgo_total DESC LIMIT 5
    """)
    print(f"\n  TOP 5 PARROQUIAS MÁS RIESGOSAS:")
    top5 = []
    for nombre, prov, riesgo, clasif in cur.fetchall():
        print(f"    {nombre} ({prov}) - Riesgo: {riesgo:.2f} [{clasif}]")
        top5.append({"nombre": nombre, "provincia": prov, "riesgo": float(riesgo), "clasificacion": clasif})
    reporte["estadisticas"]["top5_riesgo"] = top5

    # Población total
    cur.execute("SELECT SUM(poblacion_total), SUM(viviendas), ROUND(AVG(nbi_porcentaje)::numeric, 1) FROM poblacion_inec")
    pob_total, viv_total, nbi_avg = cur.fetchone()
    print(f"\n  POBLACIÓN:")
    print(f"    Total: {pob_total:,}")
    print(f"    Viviendas: {viv_total:,}")
    print(f"    NBI promedio: {nbi_avg}%")
    reporte["estadisticas"]["poblacion"] = {
        "total": int(pob_total) if pob_total else 0,
        "viviendas": int(viv_total) if viv_total else 0,
        "nbi_promedio": float(nbi_avg) if nbi_avg else 0,
    }

    cur.close()
    conn.close()

    # Guardar reporte JSON
    import os
    report_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reporte_verificacion.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(reporte, f, indent=2, ensure_ascii=False, default=str)

    print(f"\n  Reporte guardado: {report_path}")
    print("=" * 60)
    print("  VERIFICACIÓN COMPLETADA")
    print("=" * 60)

    return reporte


if __name__ == "__main__":
    verificar()
