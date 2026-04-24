ALTER TABLE facttable
ADD COLUMN segment INTEGER;

ALTER TABLE facttable
ADD CONSTRAINT fk_facttable_segment
FOREIGN KEY (segment)
REFERENCES dimsegment(id);

CREATE TABLE IF NOT EXISTS public.dimsegment
(
    id integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    segment character varying(20) COLLATE pg_catalog."default",
    CONSTRAINT dimsegment_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.dimsegment
    OWNER to kpi;

INSERT INTO dimsegment(segment) VALUES('Segment 1');
INSERT INTO dimsegment(segment) VALUES('Segment 2');
INSERT INTO dimsegment(segment) VALUES('Segment 3');
INSERT INTO dimsegment(segment) VALUES('Segment 4');
INSERT INTO dimsegment(segment) VALUES('.NET');
INSERT INTO dimsegment(segment) VALUES('No Assignee');
INSERT INTO dimsegment(segment) VALUES('Architecture');
INSERT INTO dimsegment(segment) VALUES('PLs');
INSERT INTO dimsegment(segment) VALUES('Other');
INSERT INTO dimsegment(segment) VALUES('OpenTPB');

UPDATE facttable f
SET segment = m.segment
FROM (
    VALUES
        (1112, 2),
        (1113, 1),
        (1114, 2),
        (1121, 2),
        (1122, 10),
        (1123, 10),
        (1124, 2),
        (1126, 2),
        (1127, 8),
        (1128, 7),
        (1132, 2),
        (1133, 2),
        (1136, 1),
        (1139, 1),
        (1146, 2),
        (1147, 5),
        (1163, 9),
        (1175, 3),
        (1182, 4),
        (1190, 1),
        (1200, 4),
        (1205, 1),
        (1235, 3),
        (1293, 8),
        (1887, 1),
        (1890, 3),
        (1904, 10),
        (1947, 3),
        (2064, 1),
        (2101, 3),
        (2157, 3),
        (2388, 10)
) AS m(assignee, segment)
WHERE f.assignee = m.assignee;

UPDATE facttable SET segment = 6 WHERE segment IS NULL