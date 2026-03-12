"""
RiesgoECU - Optimizar RPCs para carga rapida
1. geom_simple con ST_Simplify(0.001)
2. Tabla provincias_geom (cache contornos disueltos)
3. RPCs: get_provincias_geojson, get_parroquias_by_provincia, get_parroquias_lista
"""
import psycopg2
import time

DB = {
    "host": "aws-1-us-east-2.pooler.supabase.com",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres.vkqqveyaaijuwidpxyhq",
    "password": "1002432845",
}


def main():
    print("=" * 60)
    print("  Optimizar RPCs para carga rapida")
    print("=" * 60)

    conn = psycopg2.connect(**DB)
    conn.autocommit = True
    cur = conn.cursor()
    t0 = time.time()

    # 1. Simplified geometry column
    print("\n--- geom_simple ---")
    cur.execute("ALTER TABLE parroquias ADD COLUMN IF NOT EXISTS geom_simple geometry")
    cur.execute("""
        UPDATE parroquias
        SET geom_simple = ST_Simplify(geom, 0.001)
        WHERE geom IS NOT NULL
    """)
    print(f"  Simplificadas: {cur.rowcount}")

    cur.execute("DROP INDEX IF EXISTS idx_parroquias_geom_simple")
    cur.execute(
        "CREATE INDEX idx_parroquias_geom_simple ON parroquias USING GIST(geom_simple)"
    )
    print("  Indice GIST creado")

    # 2. Province outlines cache table
    print("\n--- provincias_geom ---")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS provincias_geom (
            provincia TEXT PRIMARY KEY,
            parroquia_count INTEGER,
            geojson TEXT
        )
    """)
    cur.execute("""
        INSERT INTO provincias_geom (provincia, parroquia_count, geojson)
        SELECT
            provincia,
            COUNT(*)::integer,
            ST_AsGeoJSON(ST_Simplify(ST_Union(geom), 0.01))
        FROM parroquias
        WHERE geom IS NOT NULL
        GROUP BY provincia
        ON CONFLICT (provincia) DO UPDATE SET
            parroquia_count = EXCLUDED.parroquia_count,
            geojson = EXCLUDED.geojson
    """)
    print(f"  Insertadas: {cur.rowcount}")

    # RLS + permisos
    cur.execute("ALTER TABLE provincias_geom ENABLE ROW LEVEL SECURITY")
    cur.execute("DROP POLICY IF EXISTS provincias_geom_read ON provincias_geom")
    cur.execute(
        "CREATE POLICY provincias_geom_read ON provincias_geom FOR SELECT USING (true)"
    )
    cur.execute("GRANT SELECT ON provincias_geom TO anon, authenticated")

    cur.execute("SELECT COUNT(*) FROM provincias_geom")
    print(f"  Provincias: {cur.fetchone()[0]}")

    # 3. RPC: Province outlines (from cache - instant)
    print("\n--- RPC get_provincias_geojson ---")
    cur.execute("""
        CREATE OR REPLACE FUNCTION get_provincias_geojson()
        RETURNS TABLE(provincia text, parroquia_count integer, geojson json)
        LANGUAGE sql STABLE SECURITY DEFINER AS $$
            SELECT provincia, parroquia_count, geojson::json FROM provincias_geom;
        $$;
    """)
    cur.execute(
        "GRANT EXECUTE ON FUNCTION get_provincias_geojson() TO anon, authenticated"
    )
    cur.execute("SELECT COUNT(*) FROM get_provincias_geojson()")
    print(f"  Test: {cur.fetchone()[0]} provincias")

    # 4. RPC: Parroquias by province (simplified geom + risk)
    print("\n--- RPC get_parroquias_by_provincia ---")
    cur.execute("""
        CREATE OR REPLACE FUNCTION get_parroquias_by_provincia(p_provincia text)
        RETURNS TABLE(codigo text, nombre text, canton text, provincia text,
                      geojson json, clasificacion text, riesgo_total numeric)
        LANGUAGE sql STABLE AS $$
            SELECT
                p.codigo_parroquia::text,
                p.nombre::text,
                p.canton::text,
                p.provincia::text,
                ST_AsGeoJSON(COALESCE(p.geom_simple, p.geom))::json,
                ir.clasificacion::text,
                ir.riesgo_total
            FROM parroquias p
            LEFT JOIN indice_riesgo ir ON p.codigo_parroquia = ir.codigo_parroquia
            WHERE p.provincia = p_provincia AND p.geom IS NOT NULL;
        $$;
    """)
    cur.execute(
        "GRANT EXECUTE ON FUNCTION get_parroquias_by_provincia(text) TO anon, authenticated"
    )
    cur.execute("SELECT COUNT(*) FROM get_parroquias_by_provincia('PICHINCHA')")
    print(f"  Test PICHINCHA: {cur.fetchone()[0]}")

    # 5. RPC: Lightweight search list (no geometry)
    print("\n--- RPC get_parroquias_lista ---")
    cur.execute("""
        CREATE OR REPLACE FUNCTION get_parroquias_lista()
        RETURNS TABLE(codigo text, nombre text, canton text, provincia text,
                      clasificacion text, riesgo_total numeric)
        LANGUAGE sql STABLE AS $$
            SELECT
                p.codigo_parroquia::text,
                p.nombre::text,
                p.canton::text,
                p.provincia::text,
                ir.clasificacion::text,
                ir.riesgo_total
            FROM parroquias p
            LEFT JOIN indice_riesgo ir ON p.codigo_parroquia = ir.codigo_parroquia;
        $$;
    """)
    cur.execute(
        "GRANT EXECUTE ON FUNCTION get_parroquias_lista() TO anon, authenticated"
    )
    cur.execute("SELECT COUNT(*) FROM get_parroquias_lista()")
    print(f"  Test: {cur.fetchone()[0]} parroquias")

    # Verify sizes
    print("\n--- Tamanos ---")
    cur.execute(
        "SELECT pg_size_pretty(SUM(ST_MemSize(geom))::bigint) FROM parroquias WHERE geom IS NOT NULL"
    )
    r = cur.fetchone()
    print(f"  geom total: {r[0] if r else 'N/A'}")
    cur.execute(
        "SELECT pg_size_pretty(SUM(ST_MemSize(geom_simple))::bigint) FROM parroquias WHERE geom_simple IS NOT NULL"
    )
    r = cur.fetchone()
    print(f"  geom_simple total: {r[0] if r else 'N/A'}")
    cur.execute(
        "SELECT pg_size_pretty(SUM(octet_length(geojson))::bigint) FROM provincias_geom"
    )
    r = cur.fetchone()
    print(f"  provincias GeoJSON: {r[0] if r else 'N/A'}")

    cur.close()
    conn.close()

    print(f"\n  Tiempo: {round(time.time() - t0, 1)}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
