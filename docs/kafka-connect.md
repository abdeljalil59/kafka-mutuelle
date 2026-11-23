# Kafka Connect : JDBC Sink sur `prelevements.resultats`

Objectif : écrire les résultats de prélèvement dans PostgreSQL **sans une ligne de Java**, uniquement par configuration (`connect/jdbc-sink-resultats.json`, une quinzaine de lignes de JSON, plus un script SQL de 7 lignes pour la table). `impaye-service` fait un travail voisin (consommer, dédoublonner, persister) en environ 200 lignes.

> **Statut.** Le connecteur a été démarré et diagnostiqué **manuellement** (sans journal de commandes conservé) : il fonctionne, avec 4 lignes dans `resultats_prelevements` (ADH-CONNECT-01 : 2× ACCEPTE à 42,50 € et 1× REJETE à 150,00 € ; ADH-CONNECT-02 : 1× REJETE à 200,00 €). Les points marqués « constaté » viennent de ce diagnostic ; ceux marqués « non vérifié » n'ont pas été relevés. Le test de rejeu (§9) n'a pas été fait.

## 1. Qu'est-ce que Kafka Connect

- Un framework fourni avec Kafka pour **déplacer des données entre Kafka et un système externe** sans coder de producteur ni de consommateur.
- **Source** : système externe vers Kafka (base de données, fichiers). **Sink** : Kafka vers système externe (base, Elasticsearch, S3). Ici : un Sink.
- **Worker** : processus JVM qui exécute les connecteurs. Plusieurs workers de même `group.id` (`kafka-mutuelle-connect`) forment un cluster « distribué » et se répartissent le travail. Ici : un seul worker.
- **Connecteur** : la configuration d'un travail. **Task** : l'unité d'exécution, parallélisable. Un sink a au plus une task par partition (3 ici) ; la configuration retenue utilise `tasks.max=1`.
- **Converters** : transforment les octets Kafka en données Connect (clé : `StringConverter`, valeur : `AvroConverter` avec le Schema Registry). Le `StringConverter` de la clé est défini au niveau du worker (compose), pas dans le JSON du connecteur.
- **API REST** (port 8083 dans le conteneur, **8085 sur l'hôte**) : créer, lister, suspendre, supprimer des connecteurs. Les configurations, offsets et statuts sont stockés dans trois topics Kafka internes (`_connect-configs`, `_connect-offsets`, `_connect-status`), à réplication 1 car il n'y a qu'un broker.

## 2. Connect ou consumer Spring Kafka codé à la main

| Critère | Connect | Spring Kafka |
|---|---|---|
| Mouvement de données simple (topic vers table) | idéal : configuration seule | possible, mais du code à écrire et à tester |
| Logique métier (décider, relancer après N rejets) | inadapté | adapté : c'est le cas d'`impaye-service` |
| Idempotence | par `upsert` et clé primaire, sans code | à écrire (table des événements traités) |
| Transformations | légères (SMT) | libres |
| Exactly-once de bout en bout | non garanti : au moins une fois, l'upsert absorbe les doublons | transactions possibles côté Kafka |
| Exploitation | REST, tasks, rebalancing fournis | à construire |

Règle pratique : **Connect pour copier**, **Spring pour décider**. Dans ce projet, le connecteur a en plus demandé du diagnostic et une table écrite à la main (§7) : « zéro code » ne veut pas dire « zéro travail ».

## 3. Démarrage

Docker Desktop doit être démarré. Ne jamais utiliser `down` ni `-v` (le volume `kafka-mutuelle-data` serait supprimé).

```powershell
docker compose -f infra/docker-compose.yml up -d postgres kafka-connect
```

Au démarrage, le conteneur installe le plugin (`confluent-hub install confluentinc/kafka-connect-jdbc:10.8.8`) : **accès Internet requis**, et le worker met un moment avant de répondre. Suivre l'avancement :

```powershell
docker logs kafka-connect --tail 30
```

Notes de configuration :
- Le worker joint le broker sur `kafka:19092` (listener interne). `9092` n'est annoncé que comme `localhost:9092`, injoignable depuis un conteneur.
- L'URL du Schema Registry est `http://schema-registry:8081` : `8082` n'est que le port publié sur l'hôte.

## 4. Création de la table (manuelle)

La table n'est **pas** créée par le connecteur (`auto.create=false`, voir §7). Le script `connect/init-postgres.sql` n'est monté dans aucun conteneur : on l'exécute à la main, **avant** de créer le connecteur. Il est idempotent (`CREATE TABLE IF NOT EXISTS`).

```powershell
Get-Content connect/init-postgres.sql | docker exec -i postgres psql -U kafka -d kafka_connect_sink
```

(PowerShell n'a pas l'opérateur `<` : le contenu du fichier passe par le tube.)

## 5. Création du connecteur

Le fichier JSON contient `name` et `config`, ce qui est le format attendu par `POST /connectors`.

```powershell
curl.exe -X POST -H "Content-Type: application/json" --data "@connect/jdbc-sink-resultats.json" http://localhost:8085/connectors
```

(`curl.exe` et non `curl` : dans Windows PowerShell, `curl` est un alias de `Invoke-WebRequest`.)

## 6. Vérification et consultation des données

```powershell
curl.exe http://localhost:8085/connectors
curl.exe http://localhost:8085/connectors/jdbc-sink-resultats/status
```

Attendu : le connecteur et sa task à l'état `RUNNING`. Le groupe de consommation du sink s'appelle `connect-jdbc-sink-resultats` (convention `connect-<nom>`) et se voit dans Kafka UI.

Lecture brute (`decideLe` apparaît en microsecondes) :

```powershell
docker exec postgres psql -U kafka -d kafka_connect_sink -c "SELECT * FROM resultats_prelevements;"
```

Lecture avec horodatage lisible : la colonne `decideLe` stocke des microsecondes depuis l'epoch, `to_timestamp` attend des secondes, d'où la division par 1 000 000.

```powershell
docker exec postgres psql -U kafka -d kafka_connect_sink -c "SELECT \"idEvenement\", \"type\", \"idAdherent\", \"montant\", to_timestamp(\"decideLe\" / 1000000.0) AS decide_le FROM resultats_prelevements ORDER BY \"decideLe\";"
```

Structure de la table :

```powershell
docker exec postgres psql -U kafka -d kafka_connect_sink -c "\d resultats_prelevements"
```

## 7. Pièges rencontrés

Trois problèmes constatés au premier essai, diagnostiqués à la main. Les corrections sont celles de `jdbc-sink-resultats.json` et `init-postgres.sql`.

### 7.1 Un point dans le nom du topic fait échouer `auto.create`

- **Constaté** : avec `auto.create=true`, le nom de table par défaut est celui du topic, `prelevements.resultats`. PostgreSQL lit le point comme `schéma.table` ; le schéma `prelevements` n'existe pas, d'où une `SQLException`.
- **Correction** : `table.name.format=resultats_prelevements` et `auto.create=false`, la table étant créée par `connect/init-postgres.sql`.
- **Pourquoi pas seulement `table.name.format` avec `auto.create=true`** : le choix de la table manuelle vient du piège suivant, puisque la création automatique aurait produit un type de colonne inadapté pour `decideLe`. Ce raisonnement est une déduction : la création automatique avec `table.name.format` seul n'a pas été essayée.

### 7.2 `timestamp-micros` arrive comme un `bigint` brut

- **Constaté** : le champ Avro `decideLe` (`long`, `timestamp-micros`) est envoyé à PostgreSQL comme un entier brut (microsecondes depuis l'epoch), et PostgreSQL refuse le cast implicite `bigint` vers `TIMESTAMP`.
- **Correction** : la colonne `decideLe` est déclarée `BIGINT`. L'affichage lisible se fait à la lecture, avec `to_timestamp("decideLe" / 1000000.0)` (§6).
- **Conséquence** : la base ne porte pas un vrai `TIMESTAMP` ; tout consommateur SQL doit convertir. La précision microseconde est conservée telle quelle dans le `BIGINT`.
- **Cause exacte non vérifiée** : pourquoi le connecteur n'a pas converti ce logicalType en `TIMESTAMP`, ni si une version plus récente du plugin le ferait.

### 7.3 Les erreurs SQL sont masquées côté Connect

- **Constaté** : le `LogRedactor` de Confluent remplace le détail des erreurs SQL par `<redacted>` dans les journaux du worker. La cause réelle n'y est pas lisible.
- **Contournement** : activer la journalisation des requêtes **côté PostgreSQL** et lire les requêtes et erreurs dans `docker logs postgres` :

```powershell
docker exec postgres psql -U kafka -d kafka_connect_sink -c "ALTER SYSTEM SET log_statement = 'all';"
docker exec postgres psql -U kafka -d kafka_connect_sink -c "SELECT pg_reload_conf();"
```

À remettre à la valeur par défaut une fois le diagnostic fini (`ALTER SYSTEM RESET log_statement;` puis un nouveau `pg_reload_conf()`), car `log_statement = 'all'` journalise tout, y compris les mots de passe éventuels dans des requêtes.

## 8. Types : ce qui est constaté, ce qui reste à vérifier

| Champ Avro | Colonne retenue | Statut |
|---|---|---|
| `idEvenement` (`string`, `uuid`) | `VARCHAR(36)`, clé primaire | choisi à la main ; le `uuid` n'a pas d'équivalent Connect, donc du texte : déduction |
| `type` (enum) | `VARCHAR(10)` | choisi à la main ; valeurs ACCEPTE et REJETE relevées en base |
| `idAdherent` | `VARCHAR(255)` | choisi à la main |
| `montant` (`bytes`, `decimal(19,2)`) | `NUMERIC(19,2)` | écriture constatée (42,50, 150,00, 200,00 relus en base) |
| `decideLe` (`long`, `timestamp-micros`) | `BIGINT` | constaté : voir §7.2 |

- `pk.mode=record_value` avec `insert.mode=upsert` donne l'idempotence par la clé primaire `idEvenement` : un message rejoué met à jour la ligne au lieu de la dupliquer. **Non prouvé** : le rejeu n'a pas été testé (§9). Le sink est « au moins une fois ».
- **Plugin** : `kafka-connect-jdbc:10.8.8`, installé au démarrage. Le numéro a été vu dans un dépôt Maven Confluent, non confirmé sur Confluent Hub (page consultée : 10.7.6 ; changelog : 10.9.x). Il a fonctionné, mais sa disponibilité sur le Hub n'est pas vérifiée.
- **Pilote PostgreSQL** : la connexion fonctionne sans pilote ajouté, donc il est fourni avec le plugin (constaté indirectement).

## 9. Compatibilité cp-kafka-connect 7.9.11 et broker 4.4.0

`cp-kafka-connect:7.9.11` est aligné sur le Schema Registry (7.9.11). Le connecteur a consommé et écrit les messages : **la compatibilité fonctionne dans le cas testé** (lecture du topic, groupe de consommation, topics internes). Les transactions, la reprise après panne du worker et le rebalancing n'ont pas été éprouvés.

## 10. Preuves restant à produire

1. Rejouer un message (reset d'offset du groupe `connect-jdbc-sink-resultats`) et vérifier que le nombre de lignes ne change pas (idempotence par upsert).
2. Consigner dans ce fichier les sorties de `status` et de `\d resultats_prelevements`, avec la date.
