# Phase 2 : clés, rebalancing à deux consommateurs et arrêt brutal

Environnement (16/11/2026, heure locale +01:00) : Spring Boot 4.1.2, spring-kafka 4.1.2, kafka-clients 4.2.2 (versions : voir [versions.md](versions.md)), broker `apache/kafka:4.4.0` du compose. Consommateur : [phase-1-consommateur.md](phase-1-consommateur.md).

Pour chaque point : ce qui est **constaté** (sortie réelle ou source lue), **déduit** et **non vérifié**.

## 1. Protocole expérimental
- **Quatre terminaux IntelliJ** : T1 (consommateur A), T2 (consommateur B), T3 (`cotisation-service`, port 8081), T4 (commandes : `Invoke-RestMethod`, `docker exec … kafka-consumer-groups --describe`).
- **Jars** construits à 17:21:39 : `mvn -B package -DskipTests -pl prelevement-service,cotisation-service -am`, `BUILD SUCCESS` en 18.991 s. Lancement : `java -jar prelevement-service\target\prelevement-service-0.1.0-SNAPSHOT.jar` dans T1 puis T2 (même `group.id` `prelevement-service`), `cotisation-service` dans T3.
- **Topic** `cotisations.emises`, 3 partitions. **Constaté (logs)** : `protocol='range'` (assignor `range`, protocole de groupe `classic`).
- **Clés de test** : `adh1` à `adh6` sont des chaînes choisies dans le champ `idAdherent` et utilisées comme clé de message ; elles n'existent nulle part ailleurs.
- **Scénarios** : A (état de départ), B (arrivée du second membre), C (messages sur les trois partitions), D (départ propre de T2 par `Ctrl+C`), E (arrêt brutal de T2 par `Stop-Process -Force`).
- Aucun paramètre de session n'a été modifié : `session.timeout.ms` et `heartbeat.interval.ms` sont les valeurs de la démonstration précédente (45000 et 3000 ms, constatées dans [phase-1-consommateur.md](phase-1-consommateur.md), section 2) et **ne sont pas relues dans les logs de cette exécution**.

## 2. Vérifications dans le jar (`kafka-clients-4.2.2.jar`, `javap -c -p`, lecture seule)
### (a) Choix de la partition d'un message avec clé
- **Constaté (bytecode)** : `KafkaProducer.partition(...)` essaie, dans cet ordre : la partition explicite du `ProducerRecord` ; un `Partitioner` personnalisé s'il est configuré ; sinon, si les octets de la clé ne sont pas null et que `partitionerIgnoreKeys` est faux, `BuiltInPartitioner.partitionForKey(octetsClé, Cluster.partitionsForTopic(topic).size())` ; sinon `-1` (choix sans clé).
- **Constaté (bytecode)** : `partitionForKey(byte[], int)` = `Utils.murmur2(octets)`, puis `Utils.toPositive(...)` (`x & 0x7fffffff`), puis le reste de la division par le nombre de partitions (`irem`).
- **Déduit** : le résultat dépend du nombre de partitions : passer de 3 à N partitions déplacerait des clés.
- **Calcul hors projet** (conseiller, Python, algorithme murmur2 de Kafka, modulo 3) : `adh1` 0x339eabcf donne 2, `adh2` 0xe6685a51 donne 2, `adh3` 0x0b6c3bbf donne 0, `adh4` 0x306b6c1c donne 0, `adh5` 0x8104e0a5 donne 1, `adh6` 0xbc5da44c donne 0. **Constaté** : ces six résultats concordent avec les partitions observées (section 3, C). **Non vérifié** : le corps de `murmur2` (non lu) ; je n'ai pas recalculé ces valeurs, donc la concordance n'est pas une preuve de l'algorithme.
- **Non vérifié** : les valeurs par défaut de `partitioner.class` et `partitioner.ignore.keys` dans notre application ; le sérialiseur de clé de `cotisation-service` (non relu ici).

### (b) `RangeAssignor` et tri des membres
- **Constaté (bytecode)** : `AbstractPartitionAssignor$MemberInfo` est `Comparable` ; `compareTo` compare d'abord `groupInstanceId` quand il est présent, puis `memberId` par `String.compareTo`. Le constructeur de `RangeAssignor$TopicAssignmentState` trie les membres (`Collections.sort`) et calcule `numPartitionsPerConsumer = nbPartitions / nbMembres` et `remainingConsumersWithExtraPartition = nbPartitions % nbMembres`. `maxAssignable` = `numPartitionsPerConsumer + (remaining > 0 ? 1 : 0) - déjà assigné`. Les partitions sont triées par `TopicPartitionComparator`, `assignRanges` parcourt les membres dans l'ordre du `LinkedHashMap` et prend `limit(maxAssignable)` partitions non attribuées.
- **Constaté** : `group.instance.id` n'est pas défini dans `application.yml` : le tri se fait donc sur `memberId`.
- **Déduit** : avec 3 partitions et 2 membres, le premier membre dans l'ordre trié reçoit 2 partitions. **Concordance avec les observations** : en B, `c8d611a3…` (avant `ef1b164d…`) reçoit P0 et P1 ; en E, `8323136f…` (avant `c8d611a3…`) reçoit P0 et P1.
- **Non lu** : `onAssigned` (le décrément de `remaining`), `assignRanges` en entier, le type de collection des partitions non attribuées.

## 3. Observations par scénario
### A. État de départ
- `docker ps` : `kafka` (`apache/kafka:4.4.0`) `Up 6 hours`, `kafka-ui` (`ghcr.io/kafbat/kafka-ui:v1.5.0`) `Up 3 days`. Aucun `java.exe` en cours, port 8081 libre.
- `describe` avant lancement : `has no active members`, une seule ligne : partition 2, `CURRENT-OFFSET 8`, `LOG-END-OFFSET 8`, `LAG 0`. **Déduit** : les partitions 0 et 1, jamais commitées, ne sont pas listées sans membre actif.
- T1 lancé (PID 33460) : un seul membre `consumer-prelevement-service-1-c8d611a3-8f9f-40f9-b54d-4e60c07e5fa7` (`HOST /172.20.0.1`, `CLIENT-ID consumer-prelevement-service-1`) pour les 3 partitions : P0 `- 0 -`, P1 `- 0 -`, P2 `8 8 0`. Logs de démarrage de T1 non relevés.

### B. Arrivée du second membre (T2, PID 36636)
```
T2 17:23:57.120 Request joining group due to: need to re-join with the given member-id: consumer-prelevement-service-1-ef1b164d-2b70-42d2-9bff-754339a1749d
T2 17:23:57.121 (Re-)joining group
T1 17:24:00.037 (Re-)joining group        (le relevé de T1 commence à cette ligne)
T1 17:24:00.098 Successfully joined group with generation Generation{generationId=8, memberId='consumer-prelevement-service-1-c8d611a3-…', protocol='range'}
T1 17:24:00.100 Finished assignment for group at generation 8: {…ef1b164d-…=Assignment(partitions=[cotisations.emises-2]), …c8d611a3-…=Assignment(partitions=[cotisations.emises-0, cotisations.emises-1])}
T2 17:24:00.101 Successfully joined group with generation Generation{generationId=8, memberId='consumer-prelevement-service-1-ef1b164d-…', protocol='range'}
T1/T2 17:24:00.108 Successfully synced group in generation 8
T2 17:24:00.115 Adding newly assigned partitions: [cotisations.emises-2]
T2 17:24:00.127 Setting offset for partition cotisations.emises-2 to the committed offset FetchPosition{offset=8, …}
T1 17:24:00.109 Adding newly assigned partitions: [cotisations.emises-0, cotisations.emises-1]
T1 17:24:00.114 Found no committed offset for partition cotisations.emises-1 puis -0
T1 17:24:00.327/.328 Resetting offset for partition cotisations.emises-1 / -0 to position FetchPosition{offset=0, …}
```
- **Constaté** : la ligne `Finished assignment` n'apparaît que chez T1 (le leader calcule le partage) ; T2 n'a que `Successfully synced group`. T2 reprend la partition 2 à l'offset commité 8 (aucun `Resetting offset`). Les lignes de T1 avant 17:24:00.037, dont un éventuel `Revoke`, ne sont pas relevées.
- **Constaté** : 2,98 s entre le re-join de T2 (17:23:57.121) et la génération 8 (17:24:00.101).
- **Déduit** : le groupe attend que T1 rejoigne et T1 l'apprend par son heartbeat (`heartbeat.interval.ms` 3000, voir section 1). **Non vérifié** : le mécanisme exact (non lu).
- `describe` : `c8d611a3…` : P0 `- 0 -`, P1 `- 0 -` ; `ef1b164d…` : P2 `8 8 0`. Les deux membres ont le même `CLIENT-ID` `consumer-prelevement-service-1` ; seul le `CONSUMER-ID` les distingue.

### C. Messages sur les trois partitions
`POST /cotisations` pour `adh1` à `adh6`, montant 50.00 (`emiseLe` UTC : `adh1` 2026-11-16T16:26:11.662664400Z, `adh2` …11.912273Z, `adh3` …11.942136700Z, `adh4` …11.966105800Z, `adh5` …12.000986800Z, `adh6` …12.033271700Z).
```
T2 (ef1b164d) 17:26:11.917 partition=2 offset=8 cle=adh1 … decision=ACCEPTE
T2 (ef1b164d) 17:26:11.959 partition=2 offset=9 cle=adh2 … decision=ACCEPTE
T1 (c8d611a3) 17:26:12.015 partition=0 offset=0 cle=adh3 … decision=ACCEPTE
T1 (c8d611a3) 17:26:12.043 partition=1 offset=0 cle=adh5 … decision=ACCEPTE
T1 (c8d611a3) 17:26:12.044 partition=0 offset=1 cle=adh4 … decision=ACCEPTE
T1 (c8d611a3) 17:26:12.062 partition=0 offset=2 cle=adh6 … decision=ACCEPTE
```
`describe` après : `c8d611a3…` : P0 `3 3 0`, P1 `1 1 0` ; `ef1b164d…` : P2 `10 10 0`.
- **Constaté** : dans toutes nos observations (cette exécution et celles de la phase 1), chaque clé retombe dans la même partition (`adh1` et `adh2` en P2 ; `adh3`, `adh4`, `adh6` en P0 ; `adh5` en P1), et chaque partition est lue par son membre. Dans une même partition, l'ordre d'envoi est conservé (`adh3`, `adh4`, `adh6` aux offsets 0, 1, 2).
- **Constaté** : entre partitions, aucun ordre : `adh5` (émis après `adh4`) est traité à .043, avant `adh4` à .044 (1 ms d'écart). **Déduit** : l'ordre n'est donc pas garanti d'une partition à l'autre.

### D. Départ propre de T2 (`Ctrl+C`)
```
T2 17:31:08.936 Revoke previously assigned partitions [cotisations.emises-2] ; partitions revoked
T2 17:31:08.938 Member consumer-prelevement-service-1-ef1b164d-… sending LeaveGroup request to coordinator localhost:9092 (id: 2147483646 …) due to the consumer unsubscribed from all topics
T2 17:31:08.938 Resetting generation and member id due to: consumer pro-actively leaving the group
T2 17:31:08.939 Request joining group due to: consumer pro-actively leaving the group ; Unsubscribed all topics or patterns and assigned partitions
T2 17:31:08.943 Resetting generation and member id … puis Request joining group … (réapparaissent une seconde fois)
T2 17:31:08.962/.963 Metrics scheduler closed, reporters fermés ; 17:31:08.975 App info unregistered ; 17:31:08.976 prelevement-service: Consumer stopped
T1 17:31:10.439 Request joining group due to: group is already rebalancing
T1 17:31:10.439 Revoke previously assigned partitions [cotisations.emises-0, cotisations.emises-1] ; 17:31:10.440 partitions revoked ; (Re-)joining group
T1 17:31:10.443 Successfully joined group with generation Generation{generationId=9, memberId='consumer-prelevement-service-1-c8d611a3-…', protocol='range'}
T1 17:31:10.444 Finished assignment for group at generation 9: {…c8d611a3-…=Assignment(partitions=[cotisations.emises-0, cotisations.emises-1, cotisations.emises-2])}
T1 17:31:10.447 Successfully synced group in generation 9 ; .449 Adding newly assigned partitions: [0, 1, 2]
T1 17:31:10.452 Setting offset for partition cotisations.emises-2 to the committed offset FetchPosition{offset=10, …} ; .453 partition 1 offset=1 ; .453 partition 0 offset=3
T1 17:31:10.453 prelevement-service: partitions assigned: [cotisations.emises-0, cotisations.emises-1, cotisations.emises-2]
```
`describe` après : `c8d611a3…` seul : P0 `3 3 0`, P1 `1 1 0`, P2 `10 10 0`.
- **Constaté** : arrêt de T2 en 40 ms (17:31:08.936 à .976) ; T1 réagit 1,5 s après le `LeaveGroup` (08.938 à 10.439) ; T1 **révoque ses propres partitions 0 et 1 alors qu'il les garde**, puis reçoit les trois ; génération 9 ; T1 garde son `memberId` d'une génération à l'autre ; reprise aux offsets commités 10, 1, 3, sans `Resetting offset`.
- **Déduit** : T1 l'apprend par son heartbeat (1,5 s, inférieur à l'intervalle de 3 s). **Non vérifié** : la raison du doublon `Resetting generation and member id` / `Request joining group` à la fermeture de T2.

### E. Arrêt brutal de T2 (16/11/2026, lancement vers 20:06)
- **État avant** : nouveau T2 : `consumer-prelevement-service-1-8323136f-b0b8-4cdf-b406-9937534d0dfd` : P0 `3 3 0`, P1 `1 1 0` ; T1 (`c8d611a3…`, même membre qu'en D) : P2 `10 10 0`. **Constaté** : le nouveau membre reçoit 2 partitions et le plus ancien 1 (l'inverse de B). **Déduit** : voir (b), section 2.
- **Kill** à 20:07:52.671 (`Stop-Process -Force` sur le processus `prelevement-service` le plus récent par `CreationDate`, PID non relevé). Juste après : `adh3` (P0) et `adh5` (P1), montant 50.00 (sorties masquées par `Out-Null` : instant d'envoi et `emiseLe` non relevés). Puis boucle `describe` toutes les 5 s (heure de `Get-Date` affichée **avant** le `docker exec`).

| Relevé (local) | Délai après le kill | Constaté |
|---|---|---|
| 20:08:19 | ≈ 27 s | P0 `3 4 1`, P1 `1 2 1` avec `8323136f…` ; P2 `10 10 0` avec `c8d611a3…` |
| 20:08:26 | ≈ 34 s | identique |
| 20:08:33 | ≈ 41 s | `Warning: Consumer group 'prelevement-service' is rebalancing.` ; P2 `10 10 0` `c8d611a3…` ; P0 `3 4 1` et P1 `1 2 1` avec `CONSUMER-ID`, `HOST`, `CLIENT-ID` à `-` |
| 20:08:41 | ≈ 49 s | `c8d611a3…` pour les 3 partitions : P0 `4 4 0`, P1 `2 2 0`, P2 `10 10 0` |
| 20:08:47 à 20:09:52 (10 relevés) | | inchangé |

- **Constaté** : le broker liste encore le membre mort environ 34 s après le kill, avec ses deux partitions et un lag de 1. **Déduit** : aucun `LeaveGroup` n'a été reçu (un kill de processus ne l'exécute pas). Le contenu de la fenêtre de T2 au moment du kill n'a pas été relevé.
- **Constaté** : un rebalancing est vu entre 20:08:26 et 20:08:33 ; pendant ce rebalancing, P0 et P1 n'ont aucun propriétaire (`CONSUMER-ID` à `-`) ; à 20:08:41, T1 tient les trois partitions, lag 0, offsets 4 et 2 : rien n'est perdu ni relu (au niveau des offsets).
- **Déduit** : le délai est compatible avec `session.timeout.ms = 45000` compté depuis le dernier heartbeat de T2 (toutes les 3 s), soit environ 42 à 45 s après le kill. Le relevé du rebalancing (≈ 41 s, heure affichée avant l'exécution de `docker exec`, donc l'instant réel du `describe` est un peu plus tardif) est à une seconde de cette borne basse : imprécision de mesure.
- **Constaté** : les deux messages (`adh3`, `adh5`) sont non traités au moins jusqu'au relevé d'environ 34 s après le kill, et traités au plus tard au relevé d'environ 49 s. Les messages ont été envoyés avant le premier relevé (instant exact non relevé) : leur durée d'attente totale n'est pas connue.

## 4. Départ propre et arrêt brutal
| | Départ propre (D) | Arrêt brutal (E) |
|---|---|---|
| `LeaveGroup` | **Oui** (constaté, log de T2) | **Non** (déduit ; broker encore à l'écoute du membre 34 s après) |
| Réaction de T1 | ≈ 1,5 s (constaté) | ≈ 34 à 41 s (constaté, à une seconde près) |
| Messages en attente | **Non testé** : aucun message envoyé pendant ce scénario (lag 0 partout après) | **Oui** : lag 1 sur P0 et P1 pendant au moins 34 s (constaté) |
| Offsets après reprise | 10, 1, 3 repris tels quels (constaté) | 4 et 2, lag 0 (constaté) |

Les deux scénarios n'ont pas les mêmes conditions (D sans message en vol, E avec deux messages) : la ligne « messages en attente » n'est pas une comparaison à armes égales.

## 5. Ce qu'on retient
- **La clé fixe la partition** (constaté en C, formule constatée en (a)) : la même clé retombe dans la même partition tant que le nombre de partitions ne change pas (déduit).
- **L'ordre n'est garanti que dans une partition** (constaté en C : `adh3`, `adh4`, `adh6` ; aucun ordre entre partitions).
- **Le rebalancing de cette configuration révoque toutes les partitions** : en D, T1 révoque `[0, 1]` alors qu'il les garde, puis reçoit `[0, 1, 2]` (constaté). La qualification « eager, stop-the-world » est une connaissance générale que je n'ai pas vérifiée dans une source lue ici ; pendant le rebalancing de E, P0 et P1 n'avaient aucun propriétaire (constaté).
- **Répartition par `memberId` trié** : le membre au plus petit `memberId` reçoit la partition en plus (constaté dans le bytecode, concordant avec B et E ; déduit pour le cas 3 partitions / 2 membres).
- **Compromis de `session.timeout.ms`** (connaissance générale, **non testé ici**, non sourcé dans cette session) : court, une panne est détectée vite mais des rebalancings inutiles sont possibles ; long, l'attente est plus longue en cas de vraie panne.

## 6. Non exercé
- `CooperativeStickyAssignor` (présent dans la liste mais non choisi : `protocol='range'`), `group.protocol=consumer` (KIP-848), `session.timeout.ms` modifié, plus de deux membres, plus de membres que de partitions.
- Les transactions.

## 7. Non vérifié
- Le corps de `Utils.murmur2` ; les valeurs Python (non recalculées) ; les valeurs par défaut de `partitioner.class` et `partitioner.ignore.keys` ; le sérialiseur de clé de `cotisation-service`.
- `onAssigned`, `assignRanges` en entier et le type de collection des partitions non attribuées (le « premier membre reçoit la partition en plus » est déduit).
- Le mécanisme du délai de 3 s en B (le heartbeat est une déduction).
- La raison du doublon `Resetting generation and member id` / `Request joining group` à la fermeture de T2 (D).
- **Non relevé** : les logs de démarrage de T1 (A) ; les lignes de T1 avant 17:24:00.037 en B ; le PID du processus tué et le contenu de la fenêtre T2 au moment du kill ; l'instant d'envoi et l'`emiseLe` de `adh3` et `adh5` en E ; les logs de T1 pendant l'arrêt brutal (génération, offsets repris, instants de traitement) ; les logs de démarrage du nouveau T2.
- Les valeurs `session.timeout.ms` et `heartbeat.interval.ms` de cette exécution (reprises de la démonstration précédente).
