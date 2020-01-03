--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: ckan_download; Type: TABLE; Schema: public; Owner: ckan_default; Tablespace: 
--

CREATE TABLE ckan_download (
    package_name character(17) NOT NULL,
    file_id character(3) NOT NULL,
    download_time timestamp with time zone,
    status smallint,
    processed boolean
);


ALTER TABLE ckan_download OWNER TO ckan_default;

--
-- Name: import; Type: TABLE; Schema: public; Owner: ckan_default; Tablespace: 
--

CREATE TABLE import (
    id bigint NOT NULL,
    package_name text,
    datetime timestamp with time zone,
    comment text,
    status smallint DEFAULT 0 NOT NULL,
    file_id character(12)
);


ALTER TABLE import OWNER TO ckan_default;

--
-- Name: import_id_seq; Type: SEQUENCE; Schema: public; Owner: ckan_default
--

CREATE SEQUENCE import_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE import_id_seq OWNER TO ckan_default;

--
-- Name: import_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: ckan_default
--

ALTER SEQUENCE import_id_seq OWNED BY import.id;


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: ckan_default
--

ALTER TABLE ONLY import ALTER COLUMN id SET DEFAULT nextval('import_id_seq'::regclass);


--
-- Data for Name: ckan_download; Type: TABLE DATA; Schema: public; Owner: ckan_default
--



--
-- Data for Name: import; Type: TABLE DATA; Schema: public; Owner: ckan_default
--



--
-- Name: import_id_seq; Type: SEQUENCE SET; Schema: public; Owner: ckan_default
--

SELECT pg_catalog.setval('import_id_seq', 1, false);


--
-- Name: ckan_download_pkey; Type: CONSTRAINT; Schema: public; Owner: ckan_default; Tablespace: 
--

ALTER TABLE ONLY ckan_download
    ADD CONSTRAINT ckan_download_pkey PRIMARY KEY (package_name, file_id);


--
-- Name: import_pkey; Type: CONSTRAINT; Schema: public; Owner: ckan_default; Tablespace: 
--

ALTER TABLE ONLY import
    ADD CONSTRAINT import_pkey PRIMARY KEY (id);


--
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

