CREATE USER datasource WITH PASSWORD '123456';

CREATE USER datasource_admin WITH PASSWORD '123456';

ALTER USER datasource_admin CREATEDB;

GRANT datasource to datasource_admin;

CREATE DATABASE bda_db WITH OWNER datasource;

CREATE DATABASE test_db WITH OWNER datasource;

CREATE USER keycloak WITH PASSWORD 'password';

CREATE DATABASE keycloak WITH OWNER keycloak;

GRANT ALL PRIVILEGES ON DATABASE keycloak TO keycloak;

\connect bda_db

CREATE TABLE db_info (
    id           SERIAL PRIMARY KEY,
    slug         VARCHAR(64) NOT NULL UNIQUE,
    name         VARCHAR(128) NOT NULL,
    description  VARCHAR(256),
    dbname       VARCHAR(64) NOT NULL UNIQUE,
    connector_id INTEGER
);

ALTER TABLE db_info OWNER TO datasource;

CREATE TABLE execution_engines (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(64) NOT NULL UNIQUE,
    engine_path     TEXT,
    local_engine    BOOLEAN DEFAULT(true),
    args            JSONB
);

ALTER TABLE execution_engines OWNER TO datasource;

CREATE TABLE execution_languages (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(64) NOT NULL UNIQUE
);

ALTER TABLE execution_languages OWNER TO datasource;

CREATE TABLE connectors (
    id                 SERIAL PRIMARY KEY,
    name               VARCHAR(64) NOT NULL UNIQUE,
    address            VARCHAR(256) NOT NULL,
    port               VARCHAR(5) NOT NULL,
    metadata           JSON,
    is_external        BOOLEAN DEFAULT(false)
);

ALTER TABLE connectors OWNER TO datasource;

CREATE TABLE shared_recipes (
    id                  SERIAL PRIMARY KEY,
    name                VARCHAR(64) NOT NULL UNIQUE,
    description         VARCHAR(256),
    language_id         INTEGER NOT NULL,
    executable_path     VARCHAR(512) NOT NULL,
    engine_id           INTEGER NOT NULL,
    args                JSON
    );

ALTER TABLE shared_recipes OWNER TO datasource;

CREATE TABLE sources (
    source_id                  SERIAL PRIMARY KEY,
    source_name                TEXT,
    get_all_dataset_ids_url        TEXT,
    parse_all_dataset_ids_response_path  JSON,
    filters                     JSON,
    download                    JSON
);

ALTER TABLE sources OWNER TO datasource;

CREATE TABLE operator_parameters (
    id                  SERIAL PRIMARY KEY,
    name                TEXT,
    type                TEXT,
    operator_id         INTEGER,
    restrictions        JSON
    );

ALTER TABLE operator_parameters OWNER TO datasource;

CREATE TABLE operators (
    id                  SERIAL PRIMARY KEY,
    name                TEXT,
    description         TEXT,
    shared_recipe_id    INTEGER,
    stage_id            INTEGER
    );

ALTER TABLE operators OWNER TO datasource;

CREATE TABLE workflow_stages (
    id                  SERIAL PRIMARY KEY,
    name                TEXT,
    description         TEXT,
    ordering            INTEGER,
    minimum_selection         INTEGER,
    maximum_selection       INTEGER,
    workflow_type_id            INTEGER,
    allow_duplicate_operators_with_other_parameters BOOLEAN
);

ALTER TABLE workflow_stages OWNER TO datasource;

CREATE TABLE workflow_types (
    id                  SERIAL PRIMARY KEY,
    name                TEXT,
    description         TEXT,
    library             TEXT,
    needs_target        BOOLEAN
);

ALTER TABLE workflow_types OWNER TO datasource;



\connect test_db

CREATE TABLE db_info (
    id           SERIAL PRIMARY KEY,
    slug         VARCHAR(64) NOT NULL UNIQUE,
    name         VARCHAR(128) NOT NULL,
    description  VARCHAR(256),
    dbname       VARCHAR(64) NOT NULL UNIQUE,
    connector_id INTEGER
);

ALTER TABLE db_info OWNER TO datasource;

CREATE TABLE execution_engines (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(64) NOT NULL UNIQUE,
    engine_path     TEXT,
    local_engine    BOOLEAN DEFAULT(true),
    args            JSONB
);

ALTER TABLE execution_engines OWNER TO datasource;

CREATE TABLE execution_languages (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(64) NOT NULL UNIQUE
);

ALTER TABLE execution_languages OWNER TO datasource;

CREATE TABLE connectors (
    id                 SERIAL PRIMARY KEY,
    name               VARCHAR(64) NOT NULL UNIQUE,
    address            VARCHAR(256) NOT NULL,
    port               VARCHAR(5) NOT NULL,
    metadata           JSON,
    is_external        BOOLEAN DEFAULT(false)
);

ALTER TABLE connectors OWNER TO datasource;

CREATE TABLE shared_recipes (
    id                  SERIAL PRIMARY KEY,
    name                VARCHAR(64) NOT NULL UNIQUE,
    description         VARCHAR(256),
    language_id         INTEGER NOT NULL,
    executable_path     VARCHAR(512) NOT NULL,
    engine_id           INTEGER NOT NULL,
    args                JSON
    );

ALTER TABLE shared_recipes OWNER TO datasource;


CREATE TABLE sources (
    source_id                  SERIAL PRIMARY KEY,
    source_name                VARCHAR(1024) NOT NULL UNIQUE,
    get_all_dataset_ids_url        TEXT NOT NULL UNIQUE,
    parse_all_dataset_ids_response_path  JSON,
    filters                     JSON,
    download                    JSON
);

ALTER TABLE sources OWNER TO datasource;

CREATE TABLE operator_parameters (
    id                  SERIAL PRIMARY KEY,
    name                TEXT,
    type                TEXT,
    operator_id         INTEGER,
    restrictions        JSON
    );

ALTER TABLE operator_parameters OWNER TO datasource;

CREATE TABLE operators (
    id                  SERIAL PRIMARY KEY,
    name                TEXT,
    description         TEXT,
    shared_recipe_id    INTEGER,
    stage_id            INTEGER
    );

ALTER TABLE operators OWNER TO datasource;

CREATE TABLE workflow_stages (
    id                  SERIAL PRIMARY KEY,
    name                TEXT,
    description         TEXT,
    ordering            INTEGER,
    minimum_selection         INTEGER,
    maximum_selection       INTEGER,
    workflow_type_id            INTEGER,
    allow_duplicate_operators_with_other_parameters BOOLEAN
);

ALTER TABLE workflow_stages OWNER TO datasource;

CREATE TABLE workflow_types (
    id                  SERIAL PRIMARY KEY,
    name                TEXT,
    description         TEXT,
    library             TEXT,
    needs_target        BOOLEAN
);

ALTER TABLE workflow_types OWNER TO datasource;