CREATE TABLE IF NOT EXISTS resultats_prelevements (
    "idEvenement" VARCHAR(36) PRIMARY KEY,
    "type" VARCHAR(10),
    "idAdherent" VARCHAR(255),
    "montant" NUMERIC(19,2),
    "decideLe" BIGINT
);
