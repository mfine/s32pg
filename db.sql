CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


CREATE TABLE objects (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    key text COLLATE "C" NOT NULL,
    last_modified timestamp with time zone,
    size integer,
    etag text COLLATE "C"
);

CREATE UNIQUE INDEX objects_on_key ON objects USING btree(key);
