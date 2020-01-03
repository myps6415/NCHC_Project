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
    package_name text NOT NULL,
    resource_id text NOT NULL,
    download_time timestamp with time zone,
    status smallint,
    processed boolean,
    skip boolean
);


ALTER TABLE ckan_download OWNER TO ckan_default;

--
-- Name: dataset; Type: TABLE; Schema: public; Owner: ckan_default; Tablespace: 
--

CREATE TABLE dataset (
    package_name text NOT NULL,
    processed boolean
);


ALTER TABLE dataset OWNER TO ckan_default;

--
-- Name: import; Type: TABLE; Schema: public; Owner: ckan_default; Tablespace: 
--

CREATE TABLE import (
    id bigint NOT NULL,
    package_name text,
    datetime timestamp with time zone,
    comment text,
    status smallint DEFAULT 0 NOT NULL,
    file_id text
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
-- Name: extractor; Type: TABLE; Schema: public; Owner: ckan_default; Tablespace: 
--

CREATE TABLE extractor (
    id bigint DEFAULT nextval('import_id_seq'::regclass) NOT NULL,
    package_name text,
    status smallint DEFAULT 0 NOT NULL,
    resourceid text,
    ckanuser text,
    skip boolean DEFAULT false
);


ALTER TABLE extractor OWNER TO ckan_default;

--
-- Name: resource_metadata; Type: TABLE; Schema: public; Owner: ckan_default; Tablespace: 
--

CREATE TABLE resource_metadata (
    id integer NOT NULL,
    package_name text NOT NULL,
    resource_id text NOT NULL,
    url text,
    format text,
    processed boolean
);


ALTER TABLE resource_metadata OWNER TO ckan_default;

--
-- Name: resource_metadata_id_seq; Type: SEQUENCE; Schema: public; Owner: ckan_default
--

CREATE SEQUENCE resource_metadata_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE resource_metadata_id_seq OWNER TO ckan_default;

--
-- Name: resource_metadata_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: ckan_default
--

ALTER SEQUENCE resource_metadata_id_seq OWNED BY resource_metadata.id;


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: ckan_default
--

ALTER TABLE ONLY import ALTER COLUMN id SET DEFAULT nextval('import_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: ckan_default
--

ALTER TABLE ONLY resource_metadata ALTER COLUMN id SET DEFAULT nextval('resource_metadata_id_seq'::regclass);


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
--

CREATE TABLE ckan_download (

