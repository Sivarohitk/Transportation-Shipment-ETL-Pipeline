-- Redshift warehouse namespaces.
--
-- The publisher validates each configured identifier before replacing these
-- tokens. Tokens are identifiers, not string literals.

CREATE SCHEMA IF NOT EXISTS {{staging_schema}};
CREATE SCHEMA IF NOT EXISTS {{target_schema}};
CREATE SCHEMA IF NOT EXISTS {{audit_schema}};
