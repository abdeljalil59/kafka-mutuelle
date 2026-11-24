CREATE TABLE IF NOT EXISTS outbox (
    id_evenement VARCHAR2(36 CHAR)  NOT NULL,
    topic        VARCHAR2(100 CHAR) NOT NULL,
    cle          VARCHAR2(100 CHAR) NOT NULL,
    payload      CLOB               NOT NULL,
    cree_le      TIMESTAMP WITH TIME ZONE NOT NULL,
    envoye_le    TIMESTAMP WITH TIME ZONE,
    CONSTRAINT pk_outbox PRIMARY KEY (id_evenement)
);
