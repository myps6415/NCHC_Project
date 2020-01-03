--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: import; Type: TABLE; Schema: public; Owner: thomas; Tablespace: 
--

CREATE TABLE import (
    id bigint NOT NULL,
    package_name text,
    datetime timestamp with time zone,
    comment text,
    status smallint DEFAULT 0 NOT NULL
);


ALTER TABLE import OWNER TO thomas;

--
-- Name: import_id_seq; Type: SEQUENCE; Schema: public; Owner: thomas
--

CREATE SEQUENCE import_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE import_id_seq OWNER TO thomas;

--
-- Name: import_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: thomas
--

ALTER SEQUENCE import_id_seq OWNED BY import.id;


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: thomas
--

ALTER TABLE ONLY import ALTER COLUMN id SET DEFAULT nextval('import_id_seq'::regclass);


--
-- Name: import_pkey; Type: CONSTRAINT; Schema: public; Owner: thomas; Tablespace: 
--

ALTER TABLE ONLY import
    ADD CONSTRAINT import_pkey PRIMARY KEY (id);


--
-- PostgreSQL database dump complete
--

