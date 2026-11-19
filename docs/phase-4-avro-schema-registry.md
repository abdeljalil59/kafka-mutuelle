# Phase 4 : Avro et Schema Registry (`events-contract`, `cotisation-service`, `prelevement-service`)

Environnement (19/11/2026, heure locale +01:00) : Spring Boot 4.1.2, spring-kafka 4.1.2, kafka-clients 4.2.2, broker `apache/kafka:4.4.0`, Schema Registry `confluentinc/cp-schema-registry:7.9.11`, Avro 1.11.5, `io.confluent:kafka-avro-serializer` 7.9.11 (`versions.md` n'est pas encore à jour pour ces deux dernières). Outbox : [phase-3-outbox.md](phase-3-outbox.md). Message illisible et DLT : [phase-3-message-invalide.md](phase-3-message-invalide.md).

Pour chaque point : ce qui est **constaté** (test, sortie réelle, source ou doc lue), **déduit** et **non vérifié**. Les résultats de la fumée (section 6) sont des **relevés de l'utilisateur** : je n'ai pas rejoué les services. La doc Confluent lue est celle de la plateforme **8.4**, pas de la 7.9.

## 1. Objectif
- Remplacer le JSON en chaîne sur `cotisations.emises` par un contrat typé : schéma Avro enregistré dans un Schema Registry.
- **Numérotation** : « phase 4 » est le nom de travail de cette étape. Au tableau du README (issu du brief), Avro et Schema Registry relèvent de la phase 6, et la phase 4 est Prometheus et Grafana.
- **Commits** : `38a056a` (infrastructure et schéma), `6d0c28e` (migration), puis ce journal. **Constaté** : `BUILD SUCCESS`, 123 tests (16 pour `events-contract`, 54 pour `cotisation-service`, 53 pour `prelevement-service`).
- Inchangés : la table `outbox` (JSON interne), `RetryDemoListener` et sa fabrique.

## 2. Infrastructure
- **Registre** : `cp-schema-registry:7.9.11`, dernière 7.x (Docker Hub, 10/11/2026, amd64 et arm64) ; les artefacts du dépôt Maven Confluent vont jusqu'à 8.4.1, donc la 7.x n'est plus la ligne la plus récente (**constaté**).
- **Port 8082** de l'hôte vers le 8081 du conteneur : `cotisation-service` écoute sur 8081 (`server.port`) ; Kafka UI (8080) n'est pas en conflit. Connexion au broker par le listener interne `kafka:19092`, comme Kafka UI. Pas de volume : les schémas sont dans le topic `_schemas` (**constaté** : 1 partition, réplication 1, compacté).
- **Variables** : `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS`, `_HOST_NAME`, `_LISTENERS`, `_KAFKASTORE_TOPIC_REPLICATION_FACTOR=1`. Doc : préfixe `SCHEMA_REGISTRY_`, points remplacés par `_` ; réplication par défaut 3, « the smaller of this value and the number of live Kafka brokers » (fixée à 1 explicitement).
- **Compatibilité** : `BACKWARD` par défaut (**constaté** : `GET /config`). Kafka UI voit le registre (**constaté** : `/api/clusters/local/schemas` répond 200).
- **Versions alignées** : le pom parent Confluent 7.9.11 fixe Avro 1.11.5, `kafka-clients` 7.9.11-ccs (Kafka 3.9) et Jackson 2.18.11 (**constaté**). Le BOM Spring Boot ne gère ni `avro` ni `io.confluent` (**constaté**) : versions dans le pom parent, dépôt Confluent déclaré (artefacts absents de Maven Central).
- **Client en 4.2.2** : dans les deux services, `kafka-schema-registry-client` 7.9.11 est résolu contre `kafka-clients` **4.2.2** (**constaté**, `dependency:tree`) ; le registre, lui, utilise des clients 3.9 contre le broker 4.4.0 et démarre sans erreur (**constaté**).
- **Plugin** : `avro-maven-plugin` génère les décimaux en `ByteBuffer` par défaut (`enableDecimalLogicalType=false`, **constaté** dans son `plugin.xml`) : activé pour obtenir un `BigDecimal`.

## 3. Schéma `CotisationEmise`
Fichier `events-contract/src/main/avro/CotisationEmise.avsc`, namespace `io.github.abdeljalil59.mutuelle.events.avro` (renommé avant tout enregistrement, au moment du déplacement vers le module partagé ; le nom complet fait partie de l'identité du schéma : **déduit**).
| Champ | Avro | Java généré (**constaté**) | Défaut |
|---|---|---|---|
| `idEvenement` | `string` + `uuid` | `UUID` | `00000000-0000-0000-0000-000000000000` |
| `idAdherent` | `string` | `String` | `""` |
| `montant` | `bytes` + `decimal(19,2)` | `BigDecimal` | `"\u0000"` (0.00) |
| `emiseLe` | `long` + `timestamp-micros` | `Instant` | `0` (epoch) |
- **Sentinelles, constaté** : un ancien message qui n'écrit que `idAdherent` est lu avec l'UUID nul, un montant 0 et l'epoch (test d'évolution de `CotisationEmiseAvroTest`).
- Les défauts ne sont pas nécessaires à la version 1 pour `BACKWARD` : doc, seul l'**ajout** d'un champ en exige (« Had the default value been omitted in the new field, the new schema would not be backward compatible »). Ils servent à pouvoir supprimer un champ plus tard.
- **Déduit** : `DemandePrelevement` ne contrôle que `idAdherent` et `montant` ; l'UUID nul et l'epoch seraient donc acceptés sans erreur.

## 4. Architecture de la migration
```
Producteur : POST -> EmettreCotisation -> outbox (JSON, H2) -> RelayerOutbox -> port PublicationCotisation (JSON)
             -> PublicationCotisationKafka -> ConvertisseurAvro -> KafkaAvroSerializer -> cotisations.emises
Consommateur : cotisations.emises -> ErrorHandlingDeserializer -> KafkaAvroDeserializer (specific.avro.reader=true)
             -> CotisationEmise (Avro) -> CotisationEmiseListener -> DemandePrelevement
Rejets : DefaultErrorHandler -> DeadLetterPublishingRecoverer -> DelegatingByTypeSerializer
         (byte[] -> ByteArraySerializer ; CotisationEmise Avro -> KafkaAvroSerializer) -> cotisations.emises-dlt
```
- **Hexagonal** : le port `PublicationCotisation` et `RelayerOutbox` sont inchangés ; `domain/` et `application/` ne connaissent ni Avro ni Jackson (**constaté**, `git`) ; la conversion est dans l'adaptateur Kafka.
- **Deux types `CotisationEmise`** : le record `events.CotisationEmise` (format JSON de l'outbox, invariants dans son constructeur) et la classe Avro `events.avro.CotisationEmise` (format sur le fil).
- **Erreurs de conversion** : `IllegalArgumentException`, pas `PublicationImpossibleException` (**constaté** par test unitaire : rien n'est publié). Le relais les journalise en ERROR avec la pile, au lieu du WARN des pannes transitoires (**lu dans le code**, non observé).
- **`ErrorHandlingDeserializer` conservé** autour du désérialiseur Avro. **Constaté** : un message illisible part au DLT avec ses octets d'origine (`MessageInvalideIntegrationTest`).
- **DLT mixte** : octets d'origine (illisible) ou objet Avro (exception du listener) — **constaté** par tests. Second sujet `cotisations.emises-dlt-value` à la première republication : **déduit** (stratégie de nom par défaut), non relevé.
- **Tests** : registre factice `mock://` (`MockSchemaRegistry` présent dans le jar client, **constaté**), même scope côté service et côté test.

## 5. Constats vérifiés
- **Avro normalise l'échelle**, **constaté** (`CotisationEmiseAvroTest`) : `50` donne `50.00` sans erreur, `50.123` lève une `AvroTypeException` (« scale »). Mon analyse préalable disait le contraire. Le `setScale(2, UNNECESSARY)` de `ConvertisseurAvro` sert à **classer l'erreur** : donnée inconvertible (échec avant `send()`) plutôt que `SerializationException` enveloppée comme panne transitoire.
- **`PrelevementInvalideException` est atteignable via Kafka**, **constaté** (`MessageInvalideIntegrationTest` : un montant négatif en Avro atteint le domaine, part au DLT en Avro, cause `PrelevementInvalideException`). Avec JSON, le constructeur du record rejetait la valeur à la désérialisation ; l'Avro n'a pas d'invariants. Cela contredit une phrase de [phase-3-message-invalide.md](phase-3-message-invalide.md) (note de mise à jour ajoutée).
- **`addNotRetryableExceptions(PrelevementInvalideException.class)`**, **constaté** (`ErreursKafkaConfigTest`, `handleOne`) : récupérée dès la première livraison, alors qu'une autre exception est réessayée.
- **Règle de domaine (option B)** : montant à 2 décimales au plus, **constaté** (`CotisationTest`) : `50.001`, `10.123` et `50.000` sont refusés, `50`, `50.5`, `50.00`, `0.01` et `5E+1` acceptés. Elle évite qu'une ligne inconvertible bloque l'outbox (le relais s'arrête à la première erreur du lot : **déduit** du code).
- **`timestamp-micros` tronque l'`Instant`**, **constaté** : `…20.803705900Z` devient `…20.803705Z`.
- **Confluent 7.9.11 (kafka-clients 3.9) avec kafka-clients 4.2.2**, **constaté** (arbre de dépendances, 123 tests, fumée). **Non vérifié** : les autres chemins de l'API (transactions…).
- **`auto.register.schemas`** vaut `true` par défaut (**constaté** : constante du jar) ; le schéma est enregistré au premier `send()` (**relevé** en fumée 1).
- **Journal « These configurations '[schema.registry.url]' were supplied but are not used yet »** (**relevé**, fumée 1) : sans danger, `kafka-clients` ne connaît pas cette clé, que `KafkaAvroSerializer` lit lui-même. Explication **déduite**.
- **Propagation de la configuration**, **constaté** : `DelegatingByTypeSerializer.configure` transmet `schema.registry.url` à `KafkaAvroSerializer` (`ErreursKafkaConfigTest`). Le `Supplier` de la fabrique renvoie un sérialiseur non configuré (« You must configure() before serialize() »), mais la fabrique le configure à la création du producteur (les tests d'intégration du DLT publient en Avro).

## 6. Fumée avec le vrai registre (relevé de l'utilisateur, 19/11/2026)
Broker réel 4.4.0, Schema Registry 7.9.11.
- **Fumée 1, message valide** : `POST /cotisations {"idAdherent":"ADH-AVRO-001","montant":150.00}` répond 201. `GET /subjects` renvoie `["cotisations.emises-value"]` (schéma auto-enregistré). Journal du producteur : `outbox : 1 événement(s) publié(s)`. Journal du consommateur : `partition=2 offset=10 cle=ADH-AVRO-001 idEvenement=3b34c030-9e8b-4793-912e-f4944cbe5597 montant=150.00 decision=REJETE` (plafond 100).
- **Fumée 2, trois décimales** : `POST /cotisations {"idAdherent":"ADH-AVRO-002","montant":10.123}` répond 400, corps `{"detail":"montant doit avoir au plus 2 décimales","instance":"/cotisations","status":400,"title":"Cotisation invalide"}`. Rien dans l'outbox ni dans Kafka.
- **Relevé en passant** : l'API REST répond correctement (201, et 400 en `ProblemDetail`) avec Jackson 2 sur le classpath.

## 7. Risques résiduels
- **Topic mixte JSON et Avro** : `cotisations.emises` contient d'anciens messages JSON. Un nouveau groupe ou un `--reset-offsets` les relirait : ils iraient au DLT, illisibles (**déduit**, non essayé). `auto-offset-reset` reste `earliest`.
- **Jackson 2 sur le classpath principal** : `avro` apporte `jackson-databind` 2.21.7 (version gérée par Boot) aux deux services (**constaté**). Aucun effet observé (tests, fumée).
- **Évolution du schéma** : aucune version 2 n'a été enregistrée ; le rejet d'un changement incompatible par le registre est **non vérifié**.
- **Sentinelles** : l'UUID nul et l'epoch passent sans erreur (section 3).
- **DLT** : lisible seulement avec le registre (Kafka UI l'a, la console Kafka non).
- **`auto.register.schemas=true`** : l'application enregistre elle-même son schéma ; en production, un enregistrement contrôlé serait préférable (**déduit**, non étudié).
- **Statut de support de la ligne 7.x** de Confluent : **non vérifié**.

## 8. Journaux antérieurs devenus partiellement obsolètes
- [phase-3-message-invalide.md](phase-3-message-invalide.md) : note de mise à jour ajoutée (désérialiseur JSON, sérialiseur Jackson du DLT, montant négatif).
- [phase-3-idempotence.md](phase-3-idempotence.md) (sections 6 et 7) : le DLT contenait « le JSON du `CotisationEmise` » ; c'est de l'Avro depuis `6d0c28e`. Non modifié.
- [phase-3-outbox.md](phase-3-outbox.md) (section 4) : le producteur n'utilise plus `StringSerializer` ni `KafkaTemplate<String, String>` ; la valeur lue sur le topic est de l'Avro converti. Non modifié.
