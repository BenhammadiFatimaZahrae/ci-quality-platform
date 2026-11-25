# Schémas des messages Kafka (contrat entre les 4 parties)

Règles communes : JSON en UTF-8, une seule ligne JSON par message,
`null` JSON pour les valeurs absentes, horodatages ISO 8601 en UTC.
Clé du message = `project` (même projet, même partition, ordre conservé).

## Topic `builds` (3 partitions)

| Champ | Type | Exemple | Remarque |
|---|---|---|---|
| build_id | texte | B-000001 | identifiant unique d'un build |
| project | texte | repo-C | repo-A à repo-E |
| branch | texte | main | main, feature/login, feature/api, fix/bug |
| commit_sha | texte | 3fa9c1d | 7 caractères hexadécimaux |
| trigger | texte | push | push ou rerun (relance du même commit) |
| status | texte | success | running, success ou failure |
| started_at | texte | 2026-11-25T10:00:00.120+00:00 | ISO 8601, UTC |
| finished_at | texte ou null | 2026-11-25T10:04:12.120+00:00 | null tant que status = running |
| duration_s | entier ou null | 252 | null tant que status = running |
| files_changed | entier | 6 | utile pour MLlib |
| lines_changed | entier | 118 | utile pour MLlib |
| event_ts | texte | 2026-11-25T10:00:04.870+00:00 | instant d'émission du message |

Un build émet d'abord un message `running`, puis un message final
(`success` ou `failure`) avec le même build_id.

## Topic `tests` (3 partitions)

| Champ | Type | Exemple | Remarque |
|---|---|---|---|
| test_id | texte | B-000001:test_07 | build_id + nom du test |
| build_id | texte | B-000001 | lien avec builds |
| project | texte | repo-C | |
| commit_sha | texte | 3fa9c1d | |
| test_name | texte | test_07 | test_01 à test_20 |
| status | texte | pass | pass, fail ou skipped |
| duration_ms | entier | 840 | |
| event_ts | texte | 2026-11-25T10:04:13.000+00:00 | instant d'émission |

## Hypothèses du simulateur

| Hypothèse | Valeur | Pourquoi |
|---|---|---|
| Projets | repo-A à repo-E, taux d'échec de base 5, 30, 15, 10, 20 % | indicateurs lisibles |
| Taille du commit | +1 point d'échec par fichier modifié | signal pour MLlib |
| Tests | 20 par build ; test_07, test_13, test_18 instables (25 %) | détecter les tests instables |
| Relances | 25 % des builds relancent un commit récent | sans relance, pas de test instable détectable |
| Doublons | 2 % des messages envoyés deux fois | justifie la déduplication de Silver |
| Deux temps | running puis résultat | justifie les upserts de Silver |
| Graine | SIM_SEED = 42 | résultats reproductibles |
