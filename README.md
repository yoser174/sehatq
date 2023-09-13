# sehatq

# table create:

"""
-- Table: public.results

-- DROP TABLE IF EXISTS public.results;

CREATE TABLE IF NOT EXISTS public.results
(
id integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
test_name text COLLATE pg_catalog."default",
test_code text COLLATE pg_catalog."default",
unit text COLLATE pg_catalog."default",
ref_range text COLLATE pg_catalog."default",
result text COLLATE pg_catalog."default",
flag text COLLATE pg_catalog."default",
order_id text COLLATE pg_catalog."default",
CONSTRAINT results_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.results
OWNER to postgres;
"""
