ALTER TABLE facttable
ADD COLUMN ticket_type INTEGER;

CREATE TABLE IF NOT EXISTS public.dimtickettype
(
    id integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    ticket_type character varying(20) COLLATE pg_catalog."default",
    CONSTRAINT dimticket_type_pkey PRIMARY KEY (id)
)

ALTER TABLE facttable
ADD CONSTRAINT fk_facttable_ticket_type
FOREIGN KEY (ticket_type)
REFERENCES dimtickettype(id);