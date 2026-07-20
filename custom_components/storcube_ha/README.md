# Storcube Battery Monitor

[![hacs_badge](https://img.shields.io/badge/HACS-Custom-orange.svg)](https://github.com/custom-components/hacs)

Intégration Home Assistant pour les batteries de balcon **STORCUBE S1000 / S1000 Pro**, via l'API cloud Baterway.

## Fonctionnement

L'intégration ne communique pas directement avec la batterie : elle passe par le service cloud de Baterway, comme l'application mobile STORCUBE.

- Un **WebSocket** maintient une connexion permanente et reçoit l'état temps réel de chaque batterie (charge, puissance onduleur, production solaire, température).
- Une **boucle REST** interroge périodiquement l'état de sortie et le firmware.
- Les deux flux alimentent un coordinateur unique, qui les fusionne par batterie — le WebSocket étant prioritaire car plus frais.

Chaque batterie d'un stack (maître et esclaves) devient un appareil distinct dans Home Assistant, avec son propre jeu d'entités.

Si l'intégration MQTT de Home Assistant est présente, l'état est également republié sur le broker sous `storcube/<equipId>/…`, pour les automatisations externes. Elle est facultative.

> **Note :** `iot_class` est `cloud_push`. Tout transite par `baterway.com` ; aucune communication locale avec la batterie n'est possible en l'état.

## Entités

### Capteurs

| Entité | Unité | Source |
|---|---|---|
| Niveau batterie | % | WebSocket |
| Puissance batterie | W | WebSocket |
| Température batterie | °C | WebSocket |
| Capacité batterie | Wh | WebSocket |
| Puissance solaire 1 / 2 | W | WebSocket |
| Énergie solaire 1 / 2 / totale | kWh | calculée |
| Puissance de sortie | W | REST |
| Énergie de sortie | kWh | calculée |
| État batterie, État de fonctionnement, État de connexion | — | mixte |
| Type de sortie, Mode de fonctionnement, Code d'erreur | — | mixte |
| Modèle, Numéro de série, Version firmware | — | WebSocket |
| Firmware (état de mise à jour) | — | REST |
| Seuil batterie, Niveau de réserve | % | REST |

Les compteurs d'énergie sont obtenus par intégration de la puissance et **restaurés au redémarrage** de Home Assistant. Ils sont compatibles avec le tableau de bord Énergie (`total_increasing`).

### Contrôles

| Entité | Plage |
|---|---|
| Puissance de sortie | 0 – 800 W |
| Seuil de décharge | 0 – 100 % |

### Capteur binaire

`Connexion` — état de liaison de la batterie. La plateforme existe mais n'est pas activée par défaut ; ajoutez `Platform.BINARY_SENSOR` à `PLATFORMS` dans `__init__.py` pour l'utiliser.

## Services

| Service | Description |
|---|---|
| `storcube_ha.set_power` | Définit la consigne de puissance de sortie (0–800 W) |
| `storcube_ha.set_threshold` | Définit le seuil de décharge (0–100 %) |
| `storcube_ha.check_firmware` | Interroge l'API et **retourne** les versions et notes de version |

Le champ `device_id` est facultatif : il n'est requis que si plusieurs entrées sont configurées.

```yaml
action: storcube_ha.check_firmware
response_variable: firmware
```

## Installation

### Via HACS

1. HACS → Intégrations → menu ⋮ → Dépôts personnalisés
2. Ajoutez `https://github.com/xez7082/storcube_Ha`, catégorie « Intégration »
3. Recherchez « Storcube Battery Monitor », téléchargez
4. Redémarrez Home Assistant

> N'ajoutez pas deux dépôts HACS pointant sur le même domaine `storcube_ha` : la mise à jour de l'un écraserait les fichiers de l'autre.

### Manuelle

Copiez `custom_components/storcube_ha/` dans votre dossier `config/custom_components/`, puis redémarrez Home Assistant.

## Configuration

Paramètres → Appareils et services → Ajouter une intégration → « Storcube Battery Monitor ».

| Champ | Valeur |
|---|---|
| ID de l'appareil | Numéro de série de la batterie maître, visible dans l'application |
| Nom d'utilisateur | Identifiant du compte STORCUBE — **l'adresse e-mail complète** si vous vous êtes inscrit ainsi |
| Mot de passe | Mot de passe du même compte |
| Code de l'application | `Storcube` (valeur par défaut) |

Ce sont les identifiants de l'**application mobile STORCUBE**, pas ceux de Home Assistant ni d'un broker MQTT. Le domaine `baterway.com` correspond au backend historique de la marque.

Une batterie ne peut être ajoutée qu'une fois : l'identifiant de l'appareil sert de clé d'unicité.

### Options

L'intervalle d'interrogation REST est réglable de 15 à 600 secondes (30 s par défaut). Pour modifier vos identifiants, utilisez **Reconfigurer** sur l'entrée, pas les options.

## Dépannage

Activez les journaux détaillés :

```yaml
logger:
  default: warning
  logs:
    custom_components.storcube_ha: debug
```

| Message | Signification |
|---|---|
| `Token StorCube renouvelé` | Authentification réussie |
| `WebSocket StorCube connecté` | Liaison temps réel établie |
| `Nouvelle batterie StorCube détectée : …` | Appareil et entités créés |
| `Trame WebSocket sans batterie : clés=…` | Format de trame inattendu — ouvrez un ticket avec cette ligne |
| `Boucle REST en échec …` | API injoignable ; réessai avec délai croissant |
| `Champ de seuil retenu pour l'API : …` | Variante de paramètre acceptée par l'API |

**Entités bloquées sur `unknown`** — si seules les valeurs issues du WebSocket manquent (niveau, puissance, température, solaire), la connexion temps réel n'aboutit pas. Vérifiez que le compte n'est pas déjà connecté ailleurs : l'API tolère mal plusieurs sessions simultanées sur le même compte.

**« Authentification invalide »** — l'API répond en HTTP 200 même avec de mauvais identifiants ; c'est le champ `code` de la réponse qui fait foi. Vérifiez d'abord que le compte fonctionne dans l'application mobile.

## Historique

### v1.3.2

Refonte complète de l'architecture interne.

- **Coordinateur unique.** Auparavant, `sensor.py` ouvrait son propre WebSocket et sa propre boucle REST en parallèle de ceux du coordinateur, avec des authentifications séparées. Une seule connexion subsiste.
- **Correction du plantage `'StorCubeDataUpdateCoordinator' object has no attribute 'get'`**, causé par un désaccord entre `__init__.py` et `sensor.py` sur le contenu de `hass.data`.
- **Le WebSocket démarre réellement.** Le listener n'était jamais appelé, donc aucune entité n'était créée.
- **Entrées en double supprimées.** Le flux de configuration pose désormais un `unique_id`, et la réauthentification met à jour l'entrée existante au lieu d'en créer une nouvelle — cause historique de la multiplication des connexions.
- **Plus d'écriture dans le tableau de bord.** Deux appels à `lovelace.save_config` remplaçaient la configuration Lovelace de l'utilisateur à chaque démarrage.
- **Compteurs d'énergie fiables.** Ils repartaient de zéro à chaque redémarrage, ce que le tableau de bord Énergie interprétait comme une nouvelle production. Ils sont maintenant restaurés.
- **Topics MQTT distincts.** Six constantes pointaient sur trois topics, le firmware écrasant la production solaire.
- **Client MQTT parallèle supprimé** au profit du broker de Home Assistant.
- **Appels réseau non bloquants.** `requests` en pleine coroutine a été remplacé par la session aiohttp partagée.
- Backoff exponentiel sur la boucle REST, token persistant avec renouvellement automatique, `iot_class` corrigé en `cloud_push`, support multi-batteries, traductions françaises.

### v1.2.31 et antérieures

Voir l'historique du dépôt d'origine [jon7119/storcube_Ha](https://github.com/jon7119/storcube_Ha).

## Limites connues

- L'API Baterway n'est pas documentée. Le paramètre du seuil de décharge est accepté sous plusieurs noms de champ selon les firmwares ; l'intégration essaie les variantes connues et retient celle qui fonctionne.
- Les champs `lastBigVersion` et `currentBigVersion` renvoyés par l'API portent des noms contre-intuitifs. Le mapping actuel considère le premier comme la version installée. À vérifier si l'affichage semble inversé.
- Aucune communication locale n'est possible : une coupure Internet rend les entités indisponibles.

## Contribution

Les tickets et pull requests sont les bienvenus. Pour un problème de connexion, joignez les journaux en `debug` — en particulier les lignes `Trame WebSocket sans batterie`, qui documentent les formats d'API non encore gérés.

## Licence

Voir le fichier `LICENSE` du dépôt.
