CREATE TABLE IF NOT EXISTS decision_prelevement (
    id_evenement VARCHAR2(36 CHAR)  NOT NULL,
    id_adherent  VARCHAR2(64 CHAR)  NOT NULL,
    montant      NUMBER(19,2)       NOT NULL,
    decision     VARCHAR2(10 CHAR)  NOT NULL,
    decide_le    TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT pk_decision_prelevement PRIMARY KEY (id_evenement)
);
