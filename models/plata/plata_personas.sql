-- Lee directamente del source: SEGURIDAD_VIAL_DB.BRONCE.PERSONAS (Estructura Airbyte)
-- Granularidad: 1 fila = 1 persona del pool
-- Destino: SEGURIDAD_VIAL_DB.PLATA.PLATA_PERSONAS

WITH fuente_bruta AS (
    SELECT *
    FROM {{ source('la_paz_vial', 'PERSONAS') }}
),

deduplicado AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY NUMERO_DOC
            ORDER BY NUMERO_DOC
        ) AS rn
    FROM fuente_bruta
),

fechas_parseadas AS (
    SELECT *,
        COALESCE(
            TRY_TO_DATE(CAST(FECHA_NACIMIENTO AS VARCHAR), 'YYYY-MM-DD'),
            TRY_TO_DATE(CAST(FECHA_NACIMIENTO AS VARCHAR), 'DD/MM/YYYY'),
            TRY_TO_DATE(CAST(FECHA_NACIMIENTO AS VARCHAR))
        ) AS fecha_nacimiento_parsed
    FROM deduplicado
    WHERE rn = 1
),

normalizado AS (
    SELECT
        /* =========================================================
           IDENTIFICACIÓN
           ========================================================= */
        TRIM(UPPER(CAST(NUMERO_DOC AS VARCHAR))) AS numero_documento,

        TRIM(UPPER(
            CASE
                WHEN TIPO_DOCUMENTO IS NULL OR TRIM(CAST(TIPO_DOCUMENTO AS VARCHAR)) = '' THEN 'CI'
                ELSE CAST(TIPO_DOCUMENTO AS VARCHAR)
            END
        )) AS tipo_documento,

        /* =========================================================
           DATOS PERSONALES (EN MAYÚSCULAS CONSERVANDO LA 'Ñ')
           ========================================================= */
        TRIM(UPPER(
            CASE
                WHEN NOMBRES IS NULL OR TRIM(CAST(NOMBRES AS VARCHAR)) = '' THEN 'NO ESPECIFICADO'
                ELSE CAST(NOMBRES AS VARCHAR)
            END
        )) AS nombres,

        TRIM(UPPER(
            CASE
                WHEN APELLIDOS IS NULL OR TRIM(CAST(APELLIDOS AS VARCHAR)) = '' THEN 'NO ESPECIFICADO'
                ELSE CAST(APELLIDOS AS VARCHAR)
            END
        )) AS apellidos,

        fecha_nacimiento_parsed AS fecha_nacimiento,

        /* =========================================================
           EDAD DECLARADA
           ========================================================= */
        CASE
            WHEN EDAD_DECLARA IS NULL THEN NULL
            WHEN TRY_TO_NUMBER(CAST(EDAD_DECLARA AS VARCHAR)) < 0 THEN NULL
            WHEN TRY_TO_NUMBER(CAST(EDAD_DECLARA AS VARCHAR)) > 120 THEN NULL
            ELSE TRY_TO_NUMBER(CAST(EDAD_DECLARA AS VARCHAR))
        END AS edad_declarada,

        /* =========================================================
           GÉNERO
           ========================================================= */
        TRIM(UPPER(
            CASE
                WHEN GENERO IS NULL OR TRIM(CAST(GENERO AS VARCHAR)) = '' THEN 'NO ESPECIFICADO'
                ELSE CAST(GENERO AS VARCHAR)
            END
        )) AS genero,

        /* =========================================================
           LICENCIA
           ========================================================= */
        CASE
            WHEN NUMERO_LICENCIA IS NULL OR TRIM(CAST(NUMERO_LICENCIA AS VARCHAR)) = '' THEN NULL
            ELSE TRIM(UPPER(CAST(NUMERO_LICENCIA AS VARCHAR)))
        END AS numero_licencia,

        TRIM(UPPER(
            CASE
                WHEN CATEGORIA_LICENCIA IS NULL OR TRIM(CAST(CATEGORIA_LICENCIA AS VARCHAR)) = '' THEN 'SIN LICENCIA'
                ELSE CAST(CATEGORIA_LICENCIA AS VARCHAR)
            END
        )) AS categoria_licencia,

        /* =========================================================
           CONTACTO
           ========================================================= */
        CASE
            WHEN TELEFONO IS NULL OR TRIM(CAST(TELEFONO AS VARCHAR)) = '' THEN NULL
            ELSE TRIM(CAST(TELEFONO AS VARCHAR))
        END AS telefono,

        /* =========================================================
           PERFIL DE RIESGO
           ========================================================= */
        TRIM(UPPER(
            CASE
                WHEN PERFIL_RIESGO IS NULL OR TRIM(CAST(PERFIL_RIESGO AS VARCHAR)) = '' THEN 'BAJO'
                WHEN UPPER(TRIM(CAST(PERFIL_RIESGO AS VARCHAR))) NOT IN ('ALTO', 'MODERADO', 'BAJO') THEN 'BAJO'
                ELSE CAST(PERFIL_RIESGO AS VARCHAR)
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