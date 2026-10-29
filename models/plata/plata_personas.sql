-- Lee directamente del source: SEGURIDAD_VIAL_DB.BRONCE.PERSONAS (Estructura Airbyte)
-- Granularidad: 1 fila = 1 persona del pool
-- Destino: SEGURIDAD_VIAL_DB.PLATA.PLATA_PERSONAS

WITH fuente_bruta AS (
    SELECT 
        -- Mapeamos explícitamente desde las columnas de Snowflake
        COALESCE(NUMERO_DOC, numero_doc) AS numero_doc,
        COALESCE(TIPO_DOCUMENTO, tipo_documento) AS tipo_documento,
        COALESCE(NOMBRES, nombres) AS nombres,
        COALESCE(APELLIDOS, apellidos) AS apellidos,
        COALESCE(FECHA_NACIMIENTO, fecha_nacimiento) AS fecha_nacimiento,
        COALESCE(EDAD_DECLARA, edad_declara) AS edad_declara,
        COALESCE(GENERO, genero) AS genero,
        COALESCE(NUMERO_LICENCIA, numero_licencia) AS numero_licencia,
        COALESCE(CATEGORIA_LICENCIA, categoria_licencia) AS categoria_licencia,
        COALESCE(TELEFONO, telefono) AS telefono,
        COALESCE(PERFIL_RIESGO, perfil_riesgo) AS perfil_riesgo
    FROM {{ source('la_paz_vial', 'PERSONAS') }}
),

deduplicado AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY numero_doc
            ORDER BY numero_doc
        ) AS rn
    FROM fuente_bruta
),

fechas_parseadas AS (
    SELECT *,
        COALESCE(
            TRY_TO_DATE(CAST(fecha_nacimiento AS VARCHAR), 'YYYY-MM-DD'),
            TRY_TO_DATE(CAST(fecha_nacimiento AS VARCHAR), 'DD/MM/YYYY'),
            TRY_TO_DATE(CAST(fecha_nacimiento AS VARCHAR))
        ) AS fecha_nacimiento_parsed
    FROM deduplicado
    WHERE rn = 1
),

normalizado AS (
    SELECT
        /* =========================================================
           IDENTIFICACIÓN
           ========================================================= */
        TRIM(UPPER(CAST(numero_doc AS VARCHAR))) AS numero_documento,

        TRIM(UPPER(
            CASE
                WHEN tipo_documento IS NULL OR TRIM(CAST(tipo_documento AS VARCHAR)) = '' THEN 'CI'
                ELSE CAST(tipo_documento AS VARCHAR)
            END
        )) AS tipo_documento,

        /* =========================================================
           DATOS PERSONALES (EN MAYÚSCULAS CONSERVANDO LA 'Ñ')
           ========================================================= */
        TRIM(UPPER(
            CASE
                WHEN nombres IS NULL OR TRIM(CAST(nombres AS VARCHAR)) = '' THEN 'NO ESPECIFICADO'
                ELSE CAST(nombres AS VARCHAR)
            END
        )) AS nombres,

        TRIM(UPPER(
            CASE
                WHEN apellidos IS NULL OR TRIM(CAST(apellidos AS VARCHAR)) = '' THEN 'NO ESPECIFICADO'
                ELSE CAST(apellidos AS VARCHAR)
            END
        )) AS apellidos,

        fecha_nacimiento_parsed AS fecha_nacimiento,

        /* =========================================================
           EDAD DECLARADA
           ========================================================= */
        CASE
            WHEN edad_declara IS NULL THEN NULL
            WHEN TRY_TO_NUMBER(CAST(edad_declara AS VARCHAR)) < 0 THEN NULL
            WHEN TRY_TO_NUMBER(CAST(edad_declara AS VARCHAR)) > 120 THEN NULL
            ELSE TRY_TO_NUMBER(CAST(edad_declara AS VARCHAR))
        END AS edad_declarada,

        /* =========================================================
           GÉNERO
           ========================================================= */
        TRIM(UPPER(
            CASE
                WHEN genero IS NULL OR TRIM(CAST(genero AS VARCHAR)) = '' THEN 'NO ESPECIFICADO'
                ELSE CAST(genero AS VARCHAR)
            END
        )) AS genero,

        /* =========================================================
           LICENCIA
           ========================================================= */
        CASE
            WHEN numero_licencia IS NULL OR TRIM(CAST(numero_licencia AS VARCHAR)) = '' THEN NULL
            ELSE TRIM(UPPER(CAST(numero_licencia AS VARCHAR)))
        END AS numero_licencia,

        TRIM(UPPER(
            CASE
                WHEN categoria_licencia IS NULL OR TRIM(CAST(categoria_licencia AS VARCHAR)) = '' THEN 'SIN LICENCIA'
                ELSE CAST(categoria_licencia AS VARCHAR)
            END
        )) AS categoria_licencia,

        /* =========================================================
           CONTACTO
           ========================================================= */
        CASE
            WHEN telefono IS NULL OR TRIM(CAST(telefono AS VARCHAR)) = '' THEN NULL
            ELSE TRIM(CAST(telefono AS VARCHAR))
        END AS telefono,

        /* =========================================================
           PERFIL DE RIESGO
           ========================================================= */
        TRIM(UPPER(
            CASE
                WHEN perfil_riesgo IS NULL OR TRIM(CAST(perfil_riesgo AS VARCHAR)) = '' THEN 'BAJO'
                WHEN UPPER(TRIM(CAST(perfil_riesgo AS VARCHAR))) NOT IN ('ALTO', 'MODERADO', 'BAJO') THEN 'BAJO'
                ELSE CAST(perfil_riesgo AS VARCHAR)
            END
        )) AS perfil_riesgo,

        /* =========================================================
           EDAD CALCULADA
           ========================================================= */
        CASE
            WHEN fecha_nacimiento_parsed IS NULL THEN NULL
            ELSE 
                DATEDIFF(YEAR, fecha_nacimiento_parsed, CURRENT_DATE())
                - 
                CASE
                    WHEN DATE_FROM_PARTS(YEAR(CURRENT_DATE()), MONTH(fecha_nacimiento_parsed), DAY(fecha_nacimiento_parsed)) > CURRENT_DATE()
                    THEN 1
                    ELSE 0
                END
        END AS edad_calculada

    FROM fechas_parseadas
)

/* =============================================================
   RESULTADO FINAL
   ============================================================= */
SELECT
    numero_documento,
    tipo_documento,
    nombres,
    apellidos,
    fecha_nacimiento,
    edad_declarada,
    edad_calculada,
    genero,
    numero_licencia,
    categoria_licencia,
    telefono,
    perfil_riesgo
FROM normalizado
WHERE numero_documento IS NOT NULL
  AND TRIM(numero_documento) != ''
  AND UPPER(TRIM(numero_documento)) != 'SIN_DOCUMENTO'