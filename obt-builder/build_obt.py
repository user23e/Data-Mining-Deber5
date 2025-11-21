#!/usr/bin/env python3
# Script CLI para construir la One Big Table (OBT) de NYC Taxi

import os
import sys
import argparse
from datetime import datetime
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import io

# CONFIGURACION DESDE VARIABLES DE AMBIENTE

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
PG_SCHEMA_RAW = os.getenv("PG_SCHEMA_RAW")
PG_SCHEMA_ANALYTICS = os.getenv("PG_SCHEMA_ANALYTICS")
RUN_ID = os.getenv("RUN_ID")

# FUNCIONES AUXILIARES

def get_connection():
    """Obtiene conexion a PostgreSQL con optimizaciones"""
    log("Intentando conectar a PostgreSQL...")
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    
    # Optimizaciones de performance
    with conn.cursor() as cur:
        cur.execute("SET work_mem = '512MB';")
        cur.execute("SET maintenance_work_mem = '512MB';")
        cur.execute("SET synchronous_commit = OFF;")
    
    log("Conexion exitosa a PostgreSQL con optimizaciones")
    return conn

def log(message):
    """Imprime mensaje con timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)

def create_obt_table(conn):
    """Crea la tabla analytics.obt_trips si no existe"""
    log("Creando tabla analytics.obt_trips...")
    
    create_table_sql = f"""
    CREATE UNLOGGED TABLE IF NOT EXISTS {PG_SCHEMA_ANALYTICS}.obt_trips (
        trip_id SERIAL PRIMARY KEY,
        service_type VARCHAR(10),
        
        pickup_datetime TIMESTAMP,
        dropoff_datetime TIMESTAMP,
        pickup_hour INTEGER,
        pickup_dow INTEGER,
        month INTEGER,
        year INTEGER,
        
        pu_location_id INTEGER,
        pu_zone VARCHAR(100),
        pu_borough VARCHAR(50),
        
        do_location_id INTEGER,
        do_zone VARCHAR(100),
        do_borough VARCHAR(50),
        
        vendor_id BIGINT,
        rate_code_id BIGINT,
        payment_type BIGINT,
        trip_type DOUBLE PRECISION,
        store_and_fwd_flag VARCHAR(10),
        
        passenger_count BIGINT,
        trip_distance DOUBLE PRECISION,
        fare_amount DOUBLE PRECISION,
        extra DOUBLE PRECISION,
        mta_tax DOUBLE PRECISION,
        tip_amount DOUBLE PRECISION,
        tolls_amount DOUBLE PRECISION,
        improvement_surcharge DOUBLE PRECISION,
        congestion_surcharge INTEGER,
        airport_fee INTEGER,
        total_amount DOUBLE PRECISION,
        
        trip_duration_min DOUBLE PRECISION,
        avg_speed_mph DOUBLE PRECISION,
        tip_pct DOUBLE PRECISION,
        
        run_id VARCHAR(50),
        source_year INTEGER,
        source_month INTEGER,
        ingested_at_utc TIMESTAMP
    );
    """
    
    with conn.cursor() as cur:
        cur.execute(create_table_sql)
    
    log("Tabla analytics.obt_trips creada/verificada")

def check_partition_exists(conn, service, year, month):
    """Verifica si ya existe data para una particion especifica"""
    query = f"""
    SELECT COUNT(*) 
    FROM {PG_SCHEMA_ANALYTICS}.obt_trips 
    WHERE service_type = %s 
    AND source_year = %s 
    AND source_month = %s
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (service, year, month))
        count = cur.fetchone()[0]
    
    return count > 0

def delete_partition(conn, service, year, month):
    """Elimina datos de una particion especifica"""
    log(f"   Eliminando particion existente: {service} {year}-{month:02d}")
    
    delete_sql = f"""
    DELETE FROM {PG_SCHEMA_ANALYTICS}.obt_trips
    WHERE service_type = %s 
    AND source_year = %s 
    AND source_month = %s
    """
    
    with conn.cursor() as cur:
        cur.execute(delete_sql, (service, year, month))
        deleted = cur.rowcount
    
    log(f" -  Eliminadas {deleted:,} filas")

def build_obt_query(service, year, month):
    """Construye la query SQL OPTIMIZADA para extraer datos"""
    
    # Determinar columnas segun servicio
    if service == 'yellow':
        pickup_col = 'tpep_pickup_datetime'
        dropoff_col = 'tpep_dropoff_datetime'
        trip_type_col = 'NULL'
    else:  # green
        pickup_col = 'lpep_pickup_datetime'
        dropoff_col = 'lpep_dropoff_datetime'
        trip_type_col = 'trip_type'
    
    query = f"""
    SELECT
        '{service}' as service_type,
        
        t."{pickup_col}" as pickup_datetime,
        t."{dropoff_col}" as dropoff_datetime,
        EXTRACT(HOUR FROM t."{pickup_col}")::INT as pickup_hour,
        EXTRACT(DOW FROM t."{pickup_col}")::INT as pickup_dow,
        EXTRACT(MONTH FROM t."{pickup_col}")::INT as month,
        EXTRACT(YEAR FROM t."{pickup_col}")::INT as year,
        
        t."PULocationID" as pu_location_id,
        zpu."Zone" as pu_zone,
        zpu."Borough" as pu_borough,
        
        t."DOLocationID" as do_location_id,
        zdo."Zone" as do_zone,
        zdo."Borough" as do_borough,
        
        t."VendorID" as vendor_id,
        t."RatecodeID" as rate_code_id,
        t.payment_type,
        {trip_type_col} as trip_type,
        t.store_and_fwd_flag,
        
        t.passenger_count,
        t.trip_distance,
        t.fare_amount,
        t.extra,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.improvement_surcharge,
        t.congestion_surcharge,
        t.airport_fee,
        t.total_amount,
        
        EXTRACT(EPOCH FROM (t."{dropoff_col}" - t."{pickup_col}")) / 60.0 as trip_duration_min,
        CASE 
            WHEN EXTRACT(EPOCH FROM (t."{dropoff_col}" - t."{pickup_col}")) > 0
            THEN (t.trip_distance / (EXTRACT(EPOCH FROM (t."{dropoff_col}" - t."{pickup_col}")) / 3600.0))
            ELSE NULL 
        END as avg_speed_mph,
        CASE 
            WHEN t.fare_amount > 0 THEN (t.tip_amount / t.fare_amount) * 100.0
            ELSE NULL 
        END as tip_pct,
        
        t.run_id,
        t.source_year,
        t.source_month,
        t.ingested_at_utc
        
    FROM {PG_SCHEMA_RAW}.{service}_taxi_trip t
    LEFT JOIN {PG_SCHEMA_RAW}.taxi_zone_lookup zpu 
        ON t."PULocationID" = zpu."LocationID"
    LEFT JOIN {PG_SCHEMA_RAW}.taxi_zone_lookup zdo 
        ON t."DOLocationID" = zdo."LocationID"
    WHERE t.source_year = {year}
    AND t.source_month = {month}
    """
    
    return query

def bulk_insert_partition(conn, service, year, month):
    """
    INSERCION ULTRA-RAPIDA usando COPY ( más rápido que INSERT)
    """
    log(f"Procesando: {service} {year}-{month:02d}")
    
    # Construir query de origen
    source_query = build_obt_query(service, year, month)
    
    # Query para exportar usando COPY
    copy_export_query = f"COPY ({source_query}) TO STDOUT WITH CSV DELIMITER '|';"
    
    # Query para importar usando COPY
    copy_import_query = f"""
    COPY {PG_SCHEMA_ANALYTICS}.obt_trips (
        service_type,
        pickup_datetime, dropoff_datetime,
        pickup_hour, pickup_dow, month, year,
        pu_location_id, pu_zone, pu_borough,
        do_location_id, do_zone, do_borough,
        vendor_id, rate_code_id, payment_type, trip_type, store_and_fwd_flag,
        passenger_count, trip_distance,
        fare_amount, extra, mta_tax, tip_amount, tolls_amount,
        improvement_surcharge, congestion_surcharge, airport_fee, total_amount,
        trip_duration_min, avg_speed_mph, tip_pct,
        run_id, source_year, source_month, ingested_at_utc
    ) FROM STDIN WITH CSV DELIMITER '|';
    """
    
    try:
        start_time = datetime.now()
        
        with conn.cursor() as cur:
            # Usar buffer en memoria para transferencia ultra-rápida
            log(f" -  Extrayendo datos con COPY...")
            with io.StringIO() as csv_buffer:
                # Exportar de RAW a memoria
                cur.copy_expert(copy_export_query, csv_buffer)
                
                # Regresar al inicio del buffer
                csv_buffer.seek(0)
                
                # Importar de memoria a OBT
                log(f" - Insertando datos con COPY...")
                cur.copy_expert(copy_import_query, csv_buffer)
        
        # Contar filas insertadas
        with conn.cursor() as cur:
            count_query = f"""
            SELECT COUNT(*) FROM {PG_SCHEMA_ANALYTICS}.obt_trips 
            WHERE service_type = %s AND source_year = %s AND source_month = %s;
            """
            cur.execute(count_query, (service, year, month))
            row_count = cur.fetchone()[0]
        
        elapsed = (datetime.now() - start_time).total_seconds()
        rate = row_count / elapsed if elapsed > 0 else 0
        
        log(f" - COMPLETADO: {row_count:,} filas en {elapsed:.1f}s ({rate:.0f} filas/seg)")
        
        return row_count
        
    except Exception as e:
        log(f" - ERROR: {str(e)}")
        return 0

def create_indexes(conn):
    """Crea indices CONCURRENTLY despues de la carga"""
    log("\n" + "="*60)
    log("Creando índices...")
    log("="*60)
    
    # VACUUM ANALYZE primero para optimizar
    log("Ejecutando VACUUM ANALYZE...")
    with conn.cursor() as cur:
        cur.execute(f"VACUUM ANALYZE {PG_SCHEMA_ANALYTICS}.obt_trips;")
    log("- VACUUM ANALYZE completado")
    
    indexes = [
        f"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_obt_service_year_month ON {PG_SCHEMA_ANALYTICS}.obt_trips(service_type, source_year, source_month)",
        f"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_obt_pickup_datetime ON {PG_SCHEMA_ANALYTICS}.obt_trips(pickup_datetime)",
        f"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_obt_pu_borough ON {PG_SCHEMA_ANALYTICS}.obt_trips(pu_borough)",
    ]
    
    for idx_query in indexes:
        try:
            log(f" - Creando índice...")
            with conn.cursor() as cur:
                cur.execute(idx_query)
            log(f" - Índice creado")
        except Exception as e:
            log(f" - Error: {e}")
    
    log("- Índices creados correctamente\n")

def build_obt_full(conn, year_start, year_end, overwrite=False):
    """Construye OBT completo para un rango de años"""
    log("="*60)
    log("INICIANDO CONSTRUCCION OBT (MODO OPTIMIZADO)")
    log("="*60)
    log(f"Rango: {year_start}-{year_end}")
    log(f"Overwrite: {overwrite}")
    log(f"RUN_ID: {RUN_ID}")
    log("="*60 + "\n")
    
    # Crear tabla
    create_obt_table(conn)
    
    total_inserted = 0
    services = ['yellow', 'green']
    
    for service in services:
        log(f"\n{'='*60}")
        log(f"Servicio: {service.upper()}")
        log(f"{'='*60}\n")
        
        for year in range(year_start, year_end + 1):
            log(f"--- Anio {year} ---")
            
            for month in range(1, 13):
                # Verificar si ya existe
                if not overwrite and check_partition_exists(conn, service, year, month):
                    log(f"   [SKIP] {service} {year}-{month:02d}: Ya existe")
                    continue
                
                # Si existe y overwrite=True, eliminar
                if overwrite and check_partition_exists(conn, service, year, month):
                    delete_partition(conn, service, year, month)
                
                # Procesar particion
                inserted = bulk_insert_partition(conn, service, year, month)
                total_inserted += inserted
    
    # Crear indices al final
    if total_inserted > 0:
        create_indexes(conn)
    
    log(f"\n{'='*60}")
    log(f"RESUMEN FINAL")
    log(f"{'='*60}")
    log(f"Total filas insertadas: {total_inserted:,}")
    log(f"Finalizado: {datetime.now()}")
    log(f"{'='*60}\n")

# MAIN

def main():
    parser = argparse.ArgumentParser(description='Construir OBT de NYC Taxi (OPTIMIZADO)')
    parser.add_argument('--mode', choices=['full', 'by-partition'], default='full',
                        help='Modo de construccion')
    parser.add_argument('--year-start', type=int, 
                        default=int(os.getenv('OBT_YEAR_START', '2020')),
                        help='Ano de inicio')
    parser.add_argument('--year-end', type=int, 
                        default=int(os.getenv('OBT_YEAR_END', '2022')),
                        help='Ano de fin')
    parser.add_argument('--overwrite', action='store_true',
                        help='Sobrescribir particiones existentes')
    
    args = parser.parse_args()
    
    log("Iniciando script build_obt.py (OPTIMIZADO CON COPY)")
    log(f"Argumentos: {args}")
    
    conn = get_connection()
    
    try:
        build_obt_full(
            conn,
            year_start=args.year_start,
            year_end=args.year_end,
            overwrite=args.overwrite
        )
        
        log(" - Proceso completado exitosamente")
        
    except Exception as e:
        log(f" - Error fatal: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
