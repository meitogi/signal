# @meitogi/signal — contrat v1 (brouillon)

Ce document décrit ce que le paquet `@meitogi/signal` fera, assez précisément pour
écrire le code d'une application **avant** sa publication. Le paquet sera codé
contre ce contrat ; si un point change, ce document change avec, et la ligne
« Changements » en bas le dit.

> **Statut** : brouillon, rien n'est publié. Les noms, les options et les codes
> d'erreur ci-dessous sont ceux visés pour la version 0.1.0.

## En une phrase

Des appareils qui connaissent le même **secret** se retrouvent dans une **room**
et s'échangent des messages en **direct** (WebRTC), sans que le relay ni
personne d'autre ne puisse lire, entrer ou se faire passer pour l'un d'eux.

```
 appareil A ─┐                       ┌─ appareil B
             │   relay (WebSocket)   │
             └──► se trouvent ◄──────┘     le relay ne voit qu'une empreinte
                                           de la room, jamais le secret
 appareil A ◄════ WebRTC direct, chiffré, preuve du secret ════► appareil B
```

## Installer

```
npm install @meitogi/signal
```

| Import | Où | Pour |
|---|---|---|
| `@meitogi/signal` | navigateur | `joinRoom()`, `newSecret()`, liens |
| `@meitogi/signal/node` | Node ≥ 22 | la même API côté Node (WebRTC en pur JS, sans binaire natif) |
| `npx @meitogi/signal` | terminal | lance un relay |

Licence MIT. Dépôt : `https://github.com/meitogi/signal`.

## Utiliser

```js
import { joinRoom, newSecret } from '@meitogi/signal';

const secret = newSecret(); // à partager avec les autres, jamais avec un serveur

const room = joinRoom({
  relay: 'wss://signal.du.ranchchoco.boo',
  room: 'partie-du-soir',
  secret,
  max: 4, // pris en compte seulement si on crée la room
});

room.on('peer', (peer) => {
  peer.on('message', (data) => console.log(peer.id, data));
  peer.send('salut');
});
room.on('leave', (peer, reason) => console.log('parti', peer.id, reason));
room.on('close', (error) => console.log('room terminée', error?.code));

await room.ready; // on est dans la room
await room.broadcast('à tous');
room.leave();
```

`joinRoom()` retourne la room **tout de suite**, sans promesse : on branche les
écouteurs avant que le réseau ne bouge, aucun événement n'est manqué. `room.ready`
dit quand on est entré.

Côté Node, c'est le même code avec `import { joinRoom } from '@meitogi/signal/node'`.

### Partager la room par un lien

```js
import { formatLink, parseLink } from '@meitogi/signal';

formatLink('https://mon-app.example/', { room: 'partie-du-soir', secret });
// → https://mon-app.example/#r=partie-du-soir&s=<secret>

parseLink(location.href); // → { room, secret, relay? } ou null
```

Tout est après `#` : le navigateur n'envoie jamais cette partie au serveur web.

## Options de `joinRoom()`

| Option | Défaut | |
|---|---|---|
| `relay` | **obligatoire** | `wss://…`, ou `ws://` seulement sur `localhost` / `127.0.0.1` |
| `room` | **obligatoire** | nom choisi par l'app, 1 à 64 caractères |
| `secret` | **obligatoire** | sortie de `newSecret()` : 32 octets en base64url (43 caractères). Sans secret valide, la room se ferme avec le code `secret` |
| `max` | `8` | peers max dans la room, soi compris (2 au minimum). Fixé par celui qui **crée** la room ; ignoré pour les autres ; plafonné par le relay |
| `relayKey` | — | clé d'accès, si le relay est privé |
| `iceServers` | `[{ urls: 'stun:stun.l.google.com:19302' }]` | `[]` pour n'utiliser aucun serveur STUN |
| `fallback` | `true` | si le direct échoue, passer les messages par le relay (plus lent, toujours chiffré) |
| `connectMs` | `20000` | délai pour établir le direct avec un peer, avant repli ou abandon |
| `handshakeMs` | `10000` | délai pour que la preuve du secret aboutisse |
| `maxMessageBytes` | `1048576` | taille max d'un message envoyé ou reçu (1 Mio) |

## Room

| Membre | |
|---|---|
| `room.ready` | `Promise<void>` : résolue à l'entrée dans la room, rejetée avec une `SignalError` |
| `room.id` | mon id dans la room |
| `room.isCreator` | `true` si j'ai créé la room (et donc fixé `max`) |
| `room.max` | le maximum en vigueur |
| `room.peers` | les peers prêts (preuve faite), en tableau |
| `room.on(event, fn)` | retourne une fonction qui désabonne |
| `room.broadcast(data)` | `Promise<void>` : envoie à tous les peers prêts |
| `room.leave()` | quitte la room, ferme tout ; `close` est émis avec `null` |

| Événement | Argument | Quand |
|---|---|---|
| `peer` | `Peer` | un peer a prouvé le secret, on peut lui écrire |
| `leave` | `Peer`, `reason` | un peer prêt est parti ou a été coupé |
| `error` | `SignalError` | incident **non fatal** (un peer a raté sa preuve, a été coupé pour débit…) ; la room continue |
| `close` | `SignalError` ou `null` | la room est terminée ; rien ne repart tout seul |

Le relay peut tomber sans casser les connexions directes déjà ouvertes : la
room se reconnecte seule au relay (0,5 s, 1 s, 2 s… jusqu'à 30 s) pour accueillir
les nouveaux venus. `close` n'est émis que pour une fin définitive (`leave()`,
clé refusée, ban, room pleine à l'entrée).

## Peer

| Membre | |
|---|---|
| `peer.id` | son id dans la room |
| `peer.via` | `'direct'` ou `'relay'` (repli) |
| `peer.send(data)` | `Promise<void>`, voir ci-dessous |
| `peer.on('message', fn)` | `fn(data)` : `string` si on a envoyé une chaîne, sinon `Uint8Array` |
| `peer.on('close', fn)` | `fn(reason)` |
| `peer.close()` | coupe ce peer seulement |

`send(data)` accepte `string`, `Uint8Array` ou `ArrayBuffer`. Un message plus
gros que la limite technique de WebRTC est découpé et recollé : l'app ne voit
qu'un message. La promesse :

- se résout quand le message est accepté dans la file d'envoi. Si la file est
  pleine (peer lent), elle **attend** qu'elle se vide : `await peer.send()` dans
  une boucle ne fait jamais gonfler la mémoire ;
- est rejetée avec `too-big` au-delà de `maxMessageBytes`, ou `closed` si le
  peer est parti.

Pas de JSON imposé : `peer.send(JSON.stringify(obj))` et `JSON.parse(data)` côté
app.

## Erreurs

Toutes les erreurs sont des `SignalError` avec un `code` :

| `code` | Fatal | Sens |
|---|---|---|
| `options` | oui | option manquante ou invalide (URL du relay, nom de room, `max`) |
| `secret` | oui | secret absent ou mal formé |
| `relay` | non | relay injoignable ou coupé ; la room réessaie seule |
| `key` | oui | `relayKey` absente ou refusée |
| `room-full` | oui | room pleine à l'entrée |
| `id-taken` | non | collision d'id (quasi impossible) ; un nouvel id est tiré |
| `rate` | oui | coupé par le relay pour débit |
| `banned` | oui | IP bannie temporairement par le relay |
| `proof` | non | un peer n'a pas prouvé le secret : il est coupé |
| `timeout` | non | un peer n'a pas abouti à temps : il est coupé |
| `ice` | non | pas de chemin direct et `fallback: false` |
| `peer-rate` | non | un peer a dépassé le débit autorisé : il est coupé |
| `too-big` | — | message trop gros, sur `send()` |
| `closed` | — | envoi vers un peer ou une room fermés |

« Fatal » : `room.ready` est rejetée (ou `close` émis si on était déjà entré).

## Sécurité : ce qui est garanti

1. **Le secret ne quitte jamais l'appareil.** Le relay reçoit l'id de room
   `HKDF-SHA256(secret, room)` : sans le secret, on ne trouve pas la room. Deux
   groupes avec le même nom et des secrets différents ne se croisent jamais.
2. **Tout ce qui passe par le relay est chiffré** (AES-GCM, clé tirée du secret) :
   offres, réponses, candidats ICE, messages de repli. Le relay ne voit ni les
   adresses IP des peers ni le contenu. Une boîte qui ne se déchiffre pas est
   jetée, et son expéditeur bloqué pour soi.
3. **Chaque peer prouve le secret avant tout échange**, par un HMAC sur les
   empreintes DTLS des deux côtés et deux nonces frais. Même le relay ne peut pas
   se placer au milieu. Un peer n'émet `peer` qu'après cette preuve.
4. **Rien n'est lu avant la preuve.** Avant la preuve, un message de plus de
   1 Ko, ou qui n'est pas la preuve, coupe le peer **sans être parsé ni
   déchiffré**. Un peer légitime n'envoie de données qu'après le « ok » de l'autre :
   le lag rallonge l'attente, il ne coupe jamais un peer honnête.
5. **On ne paie rien pour un inconnu.** Aucune connexion WebRTC n'est créée sans
   une offre chiffrée avec le secret ; 4 poignées de main en cours au plus ; une
   offre en double est jetée.

Ce qui reste à l'app : savoir **qui** est le peer au-delà de « il connaît le
secret ». Toute personne qui a le lien est dans la room ; un nom ou un rôle
s'échangent par messages.

## Lancer un relay

```
npx @meitogi/signal
```

ou `docker run -p 8787:8787 ghcr.io/meitogi/signal` (image prévue, nom à
confirmer). Il écoute en `ws://` sur le port 8787 ; un navigateur exige
`wss://`, donc derrière un reverse proxy (exemples Traefik, Caddy, HAProxy dans le
dépôt) ou avec `TLS_CERT` / `TLS_KEY`.

| Variable | Défaut | |
|---|---|---|
| `PORT` / `HOST` | `8787` / `0.0.0.0` | |
| `RELAY_KEY` | vide | clé d'accès ; vide = relay ouvert à tous |
| `RELAY_ROOM_MAX` | `8` | plafond du `max` d'une room |
| `RELAY_MAX_BYTES` | `16384` | taille max d'un message ; au-delà, coupé (1009) |
| `RELAY_HELLO_MS` | `5000` | délai pour recevoir le `hello` |
| `RELAY_IDLE_MS` | `30000` | silence max après le `hello` |
| `RELAY_RATE` / `RELAY_BURST` | `120` / `240` | messages par seconde par connexion |
| `RELAY_OFFERS_PER_MIN` | `6` | offres max d'un peer vers un même peer |
| `RELAY_IP_CONN` | `10` | connexions simultanées par IP |
| `RELAY_PENDING_MAX` | `200` | connexions sans `hello`, au total |
| `RELAY_CONN_MAX` | `2000` | connexions au total |
| `RELAY_BAN_AFTER` / `RELAY_BAN_MS` | `5` / `600000` | 5 fautes en 10 min → ban de 10 min, doublé à chaque récidive |
| `TRUST_PROXY` | vide | IP ou CIDR du reverse proxy, séparés par des virgules ; seulement de là, `X-Forwarded-For` donne l'IP réelle |
| `RELAY_LEGACY` | `0` | `1` accepte aussi les anciens clients sans room (ancien protocole) |
| `TLS_CERT` / `TLS_KEY` | — | servir `wss://` sans proxy |
| `TLS_WATCH_MS` | `60000` | fréquence de vérification d'un certificat renouvelé ; `0` = seulement sur `SIGHUP` |
| `RELAY_LOG_DATA` | — | `1` journalise le début de chaque `data` relayé (chiffré), pour déboguer |

`GET /healthz` répond `200 ok`.

## Protocole du relay (pour déboguer)

L'app n'a pas à l'implémenter : `joinRoom()` le parle. JSON texte, version 1.

| Message | Envoyé par | Le relay |
|---|---|---|
| `{type:'hello', v:1, id, room, max?, key?}` | chaque peer, en premier | entre dans `room` ; répond `{type:'welcome', peers:[{id}], max, creator}` (`peers` soi compris, `creator` booléen : `true` pour celui qui a créé la room) ; envoie `{type:'peers', list:[{id}]}` à la room |
| `{type:'signal', to, kind, box}` | un peer | `kind` en clair : `'offer'`, `'answer'` ou `'ice'` ; transmet `{type:'signal', from, kind, box}` à `to` dans la même room. Le relay compte les `offer` (`RELAY_OFFERS_PER_MIN`) ; le client rejette une boîte dont le contenu ne correspond pas à `kind` |
| `{type:'data', to?, box}` | un peer (repli) | transmet `{type:'data', from, box}` à `to`, ou à toute la room |
| `{type:'block', id}` | un peer | ne lui transmet plus rien de `id` |
| `{type:'ping', ts}` | chaque peer, toutes les 5 s | répond `{type:'pong', ts, srv}` |

`box` est opaque pour le relay (chiffré). Codes de fermeture : 4400 `hello` mal
formé, 4401 clé ou `hello` absent / en retard, 4403 room pleine, 4408 silence
au-delà de `RELAY_IDLE_MS`, 4409 id pris, 4429 débit ou trop d'offres, 1009
message trop gros. Une IP bannie ou au-delà des plafonds reçoit
un HTTP 429 avant même le WebSocket.

## Coder avant la publication

- Écrire le code de l'app contre ce contrat : `joinRoom`, les événements, les
  codes d'erreur. **Ne pas réimplémenter le protocole** : le chiffrement et la
  preuve sont la partie qu'on ne veut écrire qu'une fois.
- Le relay en ligne `wss://signal.du.ranchchoco.boo` parle encore l'ancien
  protocole ; il passera à la v1 après la sortie du paquet. Pour tester avant,
  lancer un relay local (`npx @meitogi/signal`, puis `relay: 'ws://localhost:8787'`).
- Avant la publication npm, le paquet sera fourni en archive :
  `npm install ./meitogi-signal-0.1.0.tgz`. Rien d'autre à changer le jour où il
  arrive sur npm.

## Changements

- 2026-10-23 — premier brouillon.
- 2026-10-23 — protocole du relay : `signal` porte `kind` en clair (`offer` /
  `answer` / `ice`) pour que le relay puisse plafonner les offres sans lire la
  boîte ; nouveau code de fermeture 4408 (silence après le `hello`) ; `creator`
  du `welcome` est un booléen et `peers` inclut soi ; `TRUST_PROXY` accepte une
  liste ; `TLS_WATCH_MS` et `RELAY_LOG_DATA` ajoutées au tableau. Rien ne change pour l'app : `joinRoom()` parle ce protocole.
