# Data Mining - Deber 5

## Boosting Principal Asignado

**AdaBoost** - Estudio profundo con formulación matemática, algoritmo, hiperparámetros, ventajas/limitaciones y buenas prácticas.

---

## Arquitectura

El proyecto usa Docker Compose con 4 servicios:

- **postgres**: Base de datos con esquemas `raw` y `analytics`.
- **pgadmin**: UI web para administrar Postgres (era opcional).
- **pyspark-notebook**: Jupyter con Spark para ingesta de datos.
- **obt-builder**: Servicio que ejecuta el script CLI para construir la OBT.

---

## Setup

### 1. Clonar el repositorio
```bash
git clone <url-del-repositorio>
cd <nombre-del-repositorio>
```

### 2. Configurar variables de ambiente

Copiar `.env.example` a `.env` y ajustar valores según tu entorno:
```bash
cp .env.example .env
```

### Variables de Ambiente Principales

Editar el archivo `.env` con las siguientes variables:

| Variable | Descripción |
|----------|-------------|
| **PostgreSQL** | |
| `POSTGRES_HOST` | Nombre del servicio en Docker |
| `POSTGRES_PORT` | Puerto de PostgreSQL |
| `POSTGRES_DB` | Nombre de la base de datos |
| `POSTGRES_USER` | Usuario de PostgreSQL |
| `POSTGRES_PASSWORD` | Contraseña de PostgreSQL |
| `PG_SCHEMA_RAW` | Esquema para datos crudos |
| `PG_SCHEMA_ANALYTICS` | Esquema para OBT |
| **pgAdmin** | |
| `PGADMIN_EMAIL` | Email para login en pgAdmin |
| `PGADMIN_PASSWORD` | Contraseña para pgAdmin |
| **Spark** | |
| `SPARK_DRIVER_MEMORY` | Memoria para Spark driver |
| `SPARK_EXECUTOR_MEMORY` | Memoria para Spark executor |
| **Ingesta** | |
| `SOURCE_BASE` | URL base de datos NYC TLC |
| `TAXI_ZONE_URL` | URL del archivo taxi_zone_lookup.csv |
| `START_YEAR` | Año inicial para ingesta |
| `END_YEAR` | Año final para ingesta |
| `SERVICES` | Servicios a ingestar: `yellow , green` |
| `RUN_ID` | Identificador único de la ejecución |
| `BATCH_SIZE` | Meses a procesar por batch |
| **OBT Builder** | |
| `OBT_MODE` | Modo: `full` o `by-partition` |
| `OBT_YEAR_START` | Año inicial para construir OBT |
| `OBT_YEAR_END` | Año final para construir OBT |
| `OBT_OVERWRITE` | `true` para sobrescribir, `false` para solo nuevas |

### 3. Levantar servicios base
```bash
docker compose up -d postgres pgadmin pyspark-notebook
```

Esto levanta:
- Postgres en `localhost:5432`
- pgAdmin en `localhost:8080`
- Jupyter en `localhost:8888`

---

## Ingesta de Datos (RAW)

### Ejecutar notebook de ingesta

1. Acceder a Jupyter en `http://localhost:8888`
2. Abrir y ejecutar `01_ingesta_parquet_raw.ipynb`

Este notebook descarga y carga datos de Yellow y Green taxis (2015-2025) a las tablas:
- `raw.yellow_taxi_trip`
- `raw.green_taxi_trip`
- `raw.taxi_zone_lookup`

---

## Construcción de OBT

La One Big Table (`analytics.obt_trips`) se construye desde las tablas RAW para 3 años (2020-2022).

### Construcción (comando reproducible)
```bash
docker compose build obt-builder
docker compose run --rm obt-builder --overwrite
```

Este comando:
- Construye la OBT desde las tablas RAW
- Procesa datos de 2020, 2021 y 2022
- Usa **COPY** para máxima velocidad
- Crea índices automáticamente

### Características de la OBT

La tabla `analytics.obt_trips` incluye:

- **Tiempo**: pickup_datetime, dropoff_datetime, pickup_hour, pickup_dow, month, year
- **Ubicación**: pu_location_id, pu_zone, pu_borough, do_location_id, do_zone, do_borough
- **Servicio**: service_type, vendor_id, rate_code_id, payment_type, trip_type
- **Montos**: fare_amount, tip_amount, tolls_amount, total_amount
- **Derivadas**: trip_distance, passenger_count, trip_duration_min, avg_speed_mph, tip_pct
- **Metadatos**: run_id, source_year, source_month, ingested_at_utc

---

## Machine Learning - Pipeline de Ensambles

### Objetivo

**Target**: `total_amount` (USD)  
**Meta de Negocio**: Estimar el monto total del viaje al momento del pickup para pricing dinámico y planificación de demanda.

### Ejecutar Notebook Principal

Abrir y ejecutar `pset5_ensemble_regression.ipynb` en Jupyter.

### Split Temporal

- **Train**: 2020
- **Validación**: 2021
- **Test**: 2022

### Features Utilizadas (Sin Leakage)

- **Numéricas**: trip_distance, passenger_count, pickup_hour, pickup_dow, month, year

- **Categóricas**: service_type, pu_borough, do_borough, vendor_id, rate_code_id

- **Derivadas**: is_rush_hour, is_weekend

- **Prohibido**: Cualquier campo post-viaje (fare_amount, tip_amount, tolls_amount, etc.)

### Modelos Implementados

#### 1. Voting Regressor
Combina 3 modelos base con estrategia de averaging.

#### 2. Bagging vs Pasting
Comparación con Decision Tree como base learner.

#### 3. Boosting (5 Algoritmos)

**a) AdaBoost** (Nuestro Boosting Principal)
- Base learner: árboles poco profundos
- Hiperparámetros: n_estimators, learning_rate

**b) Gradient Boosting**
- Hiperparámetros: n_estimators, learning_rate, max_depth, subsample

**c) XGBoost**
- Hiperparámetros: eta, max_depth, min_child_weight, subsample, colsample_bytree, lambda, alpha

**d) LightGBM**
- Hiperparámetros: num_leaves, max_depth, learning_rate, feature_fraction, bagging_fraction

**e) CatBoost**
- Manejo nativo de categóricas
- Hiperparámetros: depth, learning_rate, l2_leaf_reg

### Preprocesamiento

- Imputación: mediana para numéricas, moda para categóricas
- Escalado: StandardScaler cuando es necesario
- Codificación: One-Hot Encoding / manejo nativo según el algoritmo
- Control de cardinalidad: Top-K + "Other" para zonas

### Métricas

**Primarias**: RMSE y MAE  
**Secundaria**: R²  
**Baseline**: Regresión lineal simple

### Validación

- TimeSeriesSplit para validación cruzada temporal
- Grid Search / Random Search para hiperparámetros
- Selección del mejor modelo por RMSE en validación
- Evaluación final en test

### Diagnóstico

- Residuales
- Error por buckets (distancia, hora, borough)
- Feature importances / SHAP

---

## Informe Técnico (PDF)

Se incluye un informe técnico de 4-6 páginas sobre **AdaBoost** que contiene:

1. Resumen ejecutivo
2. Formulación matemática (exponential loss, actualización de pesos)
3. Algoritmo (pseudoflujo)
4. Hiperparámetros clave y efectos
5. Ventajas y limitaciones
6. Buenas prácticas
7. Casos de uso y pitfalls
8. Checklist de tuning

---

## Reproducibilidad

### Semillas Fijas

Todos los experimentos usan `random_state=42` para reproducibilidad.

### Comandos de Ejecución
```bash
# 1. Levantar servicios
docker compose up -d postgres pgadmin pyspark-notebook

# 2. Ejecutar ingesta RAW (desde Jupyter)
# Abrir http://localhost:8888
# Ejecutar: 01_ingesta_parquet_raw.ipynb

# 3. Construir OBT (2020-2022)
docker compose build obt-builder
docker compose run --rm obt-builder --overwrite

# 4. Ejecutar ML pipeline (desde Jupyter)
# Ejecutar: pset5_ensemble_regression.ipynb
```

---

## Validación de Datos

Usar pgAdmin en `http://localhost:8080` para explorar:

**Esquema RAW**:
- `raw.yellow_taxi_trip`
- `raw.green_taxi_trip`
- `raw.taxi_zone_lookup`

**Esquema ANALYTICS**:
- `analytics.obt_trips`

---

## Checklist de Aceptación

- [x] Conexión a Postgres por `.env`, dataset correctamente cargado
- [x] Split temporal documentado (intervalos exactos)
- [x] Voting, Bagging, Pasting implementados y reportados
- [x] AdaBoost, Gradient Boosting, XGBoost, LightGBM, CatBoost con CV + búsqueda
- [x] Tabla comparativa (validación y test) de RMSE/MAE/R² + tiempos
- [x] Informe técnico (PDF) del boosting asignado con matemática y tuning
- [x] Semillas y versiones documentadas; evidencias en `evidencias/`

---
## Autores: 

**Curso**: Data Mining - CMP 5002  
**Institución**: Universidad San Francisco de Quito  

**Integrantes:**:
- Anahí Andrade (00323313)
- Erick Suárez (00325769)
- Jesús Alarcón (00324826)
- Giselle Cevallos (00325549)
