CREATE TABLE IF NOT EXISTS decision_prelevement (
    id_evenement VARCHAR(36)   NOT NULL,
    id_adherent  VARCHAR(64)   NOT NULL,
    montant      DECIMAL(19,2) NOT NULL,
    decision     VARCHAR(10)   NOT NULL,
    decide_le    TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT pk_decision_prelevement PRIMARY KEY (id_evenement)
);
