# Kafka Connect : JDBC Sink sur `prelevements.resultats`

Objectif : écrire les résultats de prélèvement dans PostgreSQL **sans une ligne de Java**, uniquement par configuration (`connect/jdbc-sink-resultats.json`, 15 lignes de JSON). `impaye-service` fait un travail voisin (consommer, dédoublonner, persister) en environ 200 lignes.

> **Statut : configuration écrite et YAML validé (`docker compose config`), mais rien n'a été démarré.** Aucune des affirmations d'exécution ci-dessous (démarrage de Connect, création de la table, types obtenus) n'est constatée. Elles sont marquées « à vérifier ».

## 1. Qu'est-ce que Kafka Connect

- Un framework fourni avec Kafka pour **déplacer des données entre Kafka et un système externe** sans coder de producteur ni de consommateur.
- **Source** : système externe vers Kafka (base de données, fichiers). **Sink** : Kafka vers système externe (base, Elasticsearch, S3). Ici : un Sink.
- **Worker** : processus JVM qui exécute les connecteurs. Plusieurs workers de même `group.id` (`kafka-mutuelle-connect`) forment un cluster « distribué » et se répartissent le travail. Ici : un seul worker.
- **Connecteur** : la configuration d'un travail. **Task** : l'unité d'exécution, parallélisable. Un sink a au plus une task par partition ; `tasks.max=3` correspond aux 3 partitions du topic.
- **Converters** : transforment les octets Kafka en données Connect (clé : `StringConverter`, valeur : `AvroConverter` avec le Schema Registry).
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

Règle pratique : **Connect pour copier**, **Spring pour décider**.

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

## 4. Création du connecteur

Le fichier JSON contient `name` et `config`, ce qui est le format attendu par `POST /connectors`.

```powershell
curl.exe -X POST -H "Content-Type: application/json" --data "@connect/jdbc-sink-resultats.json" http://localhost:8085/connectors
```

(`curl.exe` et non `curl` : dans Windows PowerShell, `curl` est un alias de `Invoke-WebRequest`.)

## 5. Vérification

```powershell
curl.exe http://localhost:8085/connectors
curl.exe http://localhost:8085/connectors/jdbc-sink-resultats/status
```

Attendu (à vérifier) : le connecteur et ses 3 tasks à l'état `RUNNING`. Le groupe de consommation du sink s'appelle `connect-jdbc-sink-resultats` (convention `connect-<nom>`) et se voit dans Kafka UI.

## 6. Consultation des données

```powershell
docker exec postgres psql -U kafka -d kafka_connect_sink -c "SELECT * FROM \"prelevements.resultats\" LIMIT 10;"
```

Par défaut, la table prend le nom du topic (`table.name.format=${topic}`) : avec un point dans le nom, elle doit être citée. Si elle porte un autre nom, `\dt` la liste :

```powershell
docker exec postgres psql -U kafka -d kafka_connect_sink -c "\dt"
```

## 7. Types logiques Avro vers types SQL

Source consultée : documentation du JDBC Sink Connector (docs.confluent.io, table de correspondance des types). **Seul le principe général est confirmé** ; ce qui suit pour PostgreSQL est une **attente**, à comparer à `\d "prelevements.resultats"` après le premier message.

| Champ Avro | Type Connect | Colonne PostgreSQL attendue | À vérifier |
|---|---|---|---|
| `idEvenement` (`string`, `uuid`) | `STRING` : le logicalType `uuid` n'a pas d'équivalent Connect | `TEXT`, pas `UUID` | confirmé dans la doc : non, aucune règle UUID trouvée |
| `type` (enum) | `STRING` (l'enum est converti en chaîne) | `TEXT` | oui |
| `idAdherent` | `STRING` | `TEXT` | oui |
| `montant` (`bytes`, `decimal(19,2)`) | `Decimal` (`scale=2`) | `DECIMAL` | doc : `DECIMAL` sur PostgreSQL ; précision 19 conservée ou non : non confirmé |
| `decideLe` (`long`, `timestamp-micros`) | `Timestamp` (**millisecondes**) | `TIMESTAMP` | la précision microseconde est probablement tronquée à la milliseconde : non confirmé |

Conséquences à surveiller :
- La clé primaire `idEvenement` sera du texte, pas un `UUID` natif.
- La conversion en `Timestamp` Connect peut perdre les microsecondes.
- `pk.mode=record_value` avec `insert.mode=upsert` donne l'idempotence : un message rejoué met à jour la ligne existante sans la dupliquer. Le sink est « au moins une fois », l'upsert suffit à absorber les doublons.
- Le plugin est installé en version fixe `10.8.8` : numéro vu dans un dépôt Maven Confluent, **non confirmé sur Confluent Hub** (la page consultée affichait 10.7.6 ; le changelog mentionne 10.9.x). Si `confluent-hub install` échoue, ajuster le tag.
- Le pilote PostgreSQL est supposé embarqué dans le plugin (il l'est dans la documentation des pilotes JDBC Confluent, à vérifier au démarrage).

## 8. Compatibilité cp-kafka-connect 7.9.11 et broker 4.4.0

`cp-kafka-connect:7.9.11` est aligné sur le Schema Registry déjà utilisé (7.9.11). Connect est un client Kafka (clients 3.9 attendus dans cette version) face à un broker 4.4.0. La compatibilité protocolaire est attendue mais **non vérifiée** : rien n'a été lancé. Si les logs de Connect montrent une erreur de version d'API au démarrage, c'est le premier point à examiner.

## 9. Preuve à produire (à faire)

1. Démarrer, créer le connecteur, publier des cotisations par `cotisation-service` avec `prelevement-service` actif.
2. Relever `status`, `\d "prelevements.resultats"` et un `SELECT` : consigner ici le constaté.
3. Rejouer un message (reset d'offset du groupe `connect-jdbc-sink-resultats`) et vérifier que le nombre de lignes ne change pas.
