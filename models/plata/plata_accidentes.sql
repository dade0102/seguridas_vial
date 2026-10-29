WITH fuente_bruta AS (
    SELECT *
    FROM {{ source('la_paz_vial', 'ACCIDENTES') }}
),

-- 1. DEDUPLICACION: quedarse con 1 fila por ID_REPORTE_ACCIDENTE
deduplicado AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ID_REPORTE_ACCIDENTE
            ORDER BY FECHA_HORA DESC
        ) AS rn
    FROM fuente_bruta
),

-- 2. PARSEO DE FECHAS (multiformato incluyendo milisegundos de Snowflake)
fechas_parseadas AS (
    SELECT *,
        COALESCE(
            TRY_TO_TIMESTAMP(CAST(FECHA_HORA AS VARCHAR), 'YYYY-MM-DD HH24:MI:SS.FF3'),
            TRY_TO_TIMESTAMP(CAST(FECHA_HORA AS VARCHAR), 'YYYY-MM-DD HH24:MI:SS'),
            TRY_TO_TIMESTAMP(CAST(FECHA_HORA AS VARCHAR), 'YYYY-MM-DD HH24:MI'),
            TRY_TO_TIMESTAMP(CAST(FECHA_HORA AS VARCHAR), 'YYYY-MM-DD'),
            TRY_TO_TIMESTAMP(CAST(FECHA_HORA AS VARCHAR), 'DD/MM/YYYY HH24:MI:SS'),
            TRY_TO_TIMESTAMP(CAST(FECHA_HORA AS VARCHAR), 'DD/MM/YYYY'),
            TRY_CAST(FECHA_HORA AS TIMESTAMP_NTZ)
        ) AS fecha_hora_parsed
    FROM deduplicado
    WHERE rn = 1
),

-- 3. NORMALIZACION DE TEXTOS Y CATEGORIAS
normalizado AS (
    SELECT
        ID_REPORTE_ACCIDENTE AS id_reporte_accidente,
        fecha_hora_parsed AS fecha_hora,

        -- Fecha descompuesta
        YEAR(fecha_hora_parsed) AS anio,
        MONTH(fecha_hora_parsed) AS mes,
        DAY(fecha_hora_parsed) AS dia,
        HOUR(fecha_hora_parsed) AS hora,
        DAYOFWEEK(fecha_hora_parsed) AS dia_semana,

        -- Macrodistrito
        TRIM(UPPER(
            CASE 
                WHEN MACRODISTRITO IS NULL OR TRIM(MACRODISTRITO) = '' THEN 'NO ESPECIFICADO'
                ELSE MACRODISTRITO 
            END
        )) AS macrodistrito,

        -- Barrio
        TRIM(UPPER(
            CASE 
                WHEN ZONA_BARRIO IS NULL OR TRIM(ZONA_BARRIO) = '' THEN 'NO ESPECIFICADO'
                ELSE ZONA_BARRIO 
            END
        )) AS zona_barrio,

        -- Direcciones
        REGEXP_REPLACE(TRIM(UPPER(DIRECCION_EXACTA)), '\\s+', ' ') AS direccion_exacta,
        REGEXP_REPLACE(TRIM(UPPER(CALLE_PRINCIPAL)), '\\s+', ' ') AS calle_principal,
        REGEXP_REPLACE(TRIM(UPPER(CALLE_SECUNDARIA)), '\\s+', ' ') AS calle_secundaria,

        -- Coordenadas
        CASE 
            WHEN LATITUD BETWEEN -90 AND 90 THEN ROUND(LATITUD, 6)
            ELSE NULL 
        END AS latitud,
        CASE 
            WHEN LONGITUD BETWEEN -180 AND 180 THEN ROUND(LONGITUD, 6)
            ELSE NULL 
        END AS longitud,

        -- Tipo de accidente
        TRIM(UPPER(
            CASE 
                WHEN TIPO_ACCIDENTE IS NULL OR TRIM(TIPO_ACCIDENTE) = '' THEN 'NO ESPECIFICADO'
                ELSE TIPO_ACCIDENTE 
            END
        )) AS tipo_accidente,

        -- Causa del accidente 
        TRIM(UPPER(
            CASE 
                WHEN CAUSA_ACCIDENTE IS NULL OR TRIM(CAUSA_ACCIDENTE) = '' THEN 'NO ESPECIFICADO'
                ELSE CAUSA_ACCIDENTE 
            END
        )) AS causa_accidente,

        -- Clima
        TRIM(UPPER(
            CASE 
                WHEN CLIMA IS NULL OR TRIM(CLIMA) = '' THEN 'NO ESPECIFICADO'
                ELSE CLIMA 
            END
        )) AS clima,

        -- Gravedad
        TRIM(UPPER(
            CASE 
                WHEN GRAVEDAD_ACCIDENTE_REPORTADA IS NULL 
                     OR TRIM(GRAVEDAD_ACCIDENTE_REPORTADA) = '' 
                THEN 'LEVE'
                WHEN UPPER(TRIM(GRAVEDAD_ACCIDENTE_REPORTADA)) NOT IN (
                    'LEVE', 'MODERADO', 'GRAVE', 'MORTAL'
                ) THEN 'LEVE'
                ELSE GRAVEDAD_ACCIDENTE_REPORTADA
            END
        )) AS gravedad_accidente,

        -- Unidad policial
        TRIM(UPPER(
            CASE 
                WHEN UNIDAD_POLICIAL IS NULL OR TRIM(UNIDAD_POLICIAL) = '' THEN 'NO ESPECIFICADO'
                ELSE UNIDAD_POLICIAL 
            END
        )) AS unidad_policial,

        TRIM(UPPER(
            CASE 
                WHEN UNIDAD_POLICIAL_MACRODISTRITO IS NULL OR TRIM(UNIDAD_POLICIAL_MACRODISTRITO) = '' THEN 'NO ESPECIFICADO'
                ELSE UNIDAD_POLICIAL_MACRODISTRITO 
            END
        )) AS unidad_policial_macrodistrito,

        -- Cantidades
        CASE 
            WHEN CANTIDAD_VEHICULOS_REPORTADOS < 0 THEN NULL
            WHEN CANTIDAD_VEHICULOS_REPORTADOS > 50 THEN NULL
            ELSE CANTIDAD_VEHICULOS_REPORTADOS
        END AS cantidad_vehiculos,

        CASE 
            WHEN CANTIDAD_PERSONAS_REPORTADOS < 0 THEN NULL
            WHEN CANTIDAD_PERSONAS_REPORTADOS > 100 THEN NULL
            ELSE CANTIDAD_PERSONAS_REPORTADOS
        END AS cantidad_personas,

        -- Narrativa
        REGEXP_REPLACE(TRIM(UPPER(NARRATIVA_HECHOS)), '\\s+', ' ') AS narrativa_hechos,

        -- Flags
        CASE WHEN HOUR(fecha_hora_parsed) BETWEEN 22 AND 23 
                  OR HOUR(fecha_hora_parsed) BETWEEN 0 AND 5 
             THEN TRUE ELSE FALSE 
        END AS es_nocturno,

        CASE WHEN UPPER(TRIM(GRAVEDAD_ACCIDENTE_REPORTADA)) IN ('GRAVE', 'MORTAL') 
             THEN TRUE ELSE FALSE 
        END AS es_grave_o_mortal

    FROM fechas_parseadas
)

SELECT *
FROM normalizado
WHERE fecha_hora IS NOT NULL
  AND macrodistrito IS NOT NULL