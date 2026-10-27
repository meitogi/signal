# @meitogi/signal — contrat v1

Ce document décrit ce que fait le paquet `@meitogi/signal`, assez précisément pour
écrire le code d'une application contre lui. Si un point change, ce document change
avec, et la ligne « Changements » en bas le dit.

> **Statut** : utilisable, version 0.1.2, installée depuis GitHub (pas encore sur npm, voir
> « Installer »). Le relay en ligne `wss://signal.du.ranchchoco.boo` parle ce protocole et
> demande une clé par application.

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
npm install github:meitogi/signal#v0.1.2
```

En attendant sa publication sur npm, le paquet s'installe depuis son dépôt, épinglé sur un tag :
le code est livré tel quel (pas d'étape de build), `werift` et `ws` s'installent avec. Les imports
sont les mêmes ; le jour où il arrive sur npm (`npm install @meitogi/signal`), seule la ligne du
`package.json` change.

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
//   (+ &relay=<url> avec `relay`, + &key=<clé> avec `relayKey`)

parseLink(location.href); // → { room, secret, relay?, relayKey? } ou null

joinRoom({ relay: 'wss://…', ...parseLink(location.href) }); // relayKey compris
```

Tout est après `#` : le navigateur n'envoie jamais cette partie au serveur web. Un lien
est un passe : il porte le secret de la room, et la clé du relay quand on l'y met.

## Options de `joinRoom()`

| Option | Défaut | |
|---|---|---|
| `relay` | **obligatoire** | `wss://…`, ou `ws://` seulement sur `localhost` / `127.0.0.1` |
| `room` | **obligatoire** | nom choisi par l'app, 1 à 64 caractères |
| `secret` | **obligatoire** | sortie de `newSecret()` : 32 octets en base64url (43 caractères). Sans secret valide, la room se ferme avec le code `secret` |
| `max` | `8` | peers max dans la room, soi compris (2 au minimum). Fixé par celui qui **crée** la room ; ignoré pour les autres ; plafonné par le relay |
| `relayKey` | — | clé d'accès, si le relay est privé (une clé par application, voir « Lancer un relay ») |
| `iceServers` | `[{ urls: 'stun:stun.l.google.com:19302' }]` | `[]` pour n'utiliser aucun serveur STUN (navigateur et Node) |
| `connect` | `'auto'` | `'auto'` : en entrant, on se relie à tous les membres déjà là (tout le monde finit relié à tout le monde). `'manual'` : on ne se relie à personne, l'app appelle `room.connect(id)` pour chaque lien voulu (voir « Liens à la demande ») |
| `fallback` | `true` | si le direct échoue, passer les messages par le relay (plus lent, toujours chiffré) |
| `connectMs` | `20000` | délai pour établir le direct avec un peer, avant repli ou abandon |
| `handshakeMs` | `10000` | délai pour que la preuve du secret aboutisse |
| `maxMessageBytes` | `1048576` | taille max d'un message envoyé ou reçu (1 Mio) |
| `sendRate` / `sendBurst` | `100` / `200` | messages par seconde (soutenu / d'un coup) vers le relay, signalisation et repli compris. Un seau par `joinRoom()`, c'est-à-dire par connexion au relay : deux rooms ouvertes sur un appareil = deux seaux. Plafonné à 5/6 de ce que le relay accorde (voir plus bas) |

Le relay coupe une connexion qui dépasse son débit (messages jetés en silence, puis 4429 et une
faute vers un ban d'IP). Le client reste donc **sous** ce que le relay lui accorde : le `welcome`
du relay annonce le débit accordé à la connexion, et le client se tient à 5/6 de ce débit, quelle
que soit l'option demandée. Un relay plus ancien qui ne l'annonce pas : l'option s'applique telle
quelle, et c'est à l'app de rester sous le `RELAY_RATE` / `RELAY_BURST` du relay (120 / 240 par
défaut).

## Room

| Membre | |
|---|---|
| `room.ready` | `Promise<void>` : résolue à l'entrée dans la room, rejetée avec une `SignalError` |
| `room.id` | mon id dans la room |
| `room.isCreator` | `true` si j'ai créé la room (et donc fixé `max`) |
| `room.max` | le maximum en vigueur |
| `room.peers` | les peers prêts (preuve faite), en tableau |
| `room.members` | les ids des autres membres de la room, reliés ou non, en tableau (vide avant l'entrée) |
| `room.connect(id)` | `Promise<Peer>` : se relie à ce membre, ou donne le peer s'il l'est déjà (voir « Liens à la demande ») |
| `room.on(event, fn)` | retourne une fonction qui désabonne |
| `room.broadcast(data)` | `Promise<void>` : envoie à tous les peers prêts |
| `room.leave()` | quitte la room, ferme tout ; `close` est émis avec `null` |

| Événement | Argument | Quand |
|---|---|---|
| `peer` | `Peer` | un peer a prouvé le secret, on peut lui écrire |
| `members` | `string[]` | la liste des autres membres a changé (une arrivée, un départ) |
| `leave` | `Peer`, `reason` | un peer prêt est parti ou a été coupé |
| `error` | `SignalError` | incident **non fatal** (un peer a raté sa preuve, a été coupé pour débit…) ; la room continue |
| `close` | `SignalError` ou `null` | la room est terminée ; rien ne repart tout seul |

Le relay peut tomber sans casser les connexions directes déjà ouvertes : la
room se reconnecte seule au relay (0,5 s, 1 s, 2 s… jusqu'à 30 s) pour accueillir
les nouveaux venus. `close` n'est émis que pour une fin définitive (`leave()`,
clé refusée, ban, room pleine à l'entrée).

### Liens à la demande : `connect: 'manual'`

Par défaut, chaque nouveau venu se relie à tous les membres déjà là : avec N membres, N(N-1)/2
liens. Quand seuls quelques liens servent (une page qui pilote des bots, les bots n'ayant rien à se
dire), `connect: 'manual'` n'en ouvre aucun tout seul :

```js
const room = joinRoom({ relay, room: 'atelier', secret, connect: 'manual' });

room.on('members', (ids) => {
  for (const id of ids) room.connect(id).catch((e) => console.warn(id, e.code)); // la page se relie à chaque bot
});
room.on('peer', (peer) => { /* un lien est prêt, qu'on l'ait demandé ou non */ });
```

- **`room.members`** liste les ids des autres membres, reliés ou non ; l'événement `members` donne
  la nouvelle liste à chaque arrivée ou départ. Un id ne dit rien de plus que « il connaît le
  secret » : le rôle (page, bot…) s'échange par messages une fois relié.
- **`room.connect(id)`** se résout avec le `Peer` une fois la preuve faite, en direct ou par le
  repli. Il attend `room.ready`. Appelé deux fois, ou sur un peer déjà relié, il donne le même
  peer : l'app peut l'appeler sans tenir de compte. `peer` est aussi émis, des deux côtés.
- **L'autre côté accepte seul** : un membre en `'manual'` répond à un `connect()` qui le vise, sans
  que son app ait rien à faire (la preuve du secret s'applique comme toujours). `'manual'` veut dire
  « je n'appelle personne de moi-même », pas « je refuse les appels ».
- **Deux `connect()` croisés** (A vers B et B vers A en même temps) donnent un seul lien : l'offre
  de l'id le plus petit l'emporte, l'autre côté abandonne la sienne. Les deux promesses se
  résolvent avec ce lien.
- **Rejetée** (`SignalError` non fatale) avec `closed` si l'id n'est pas dans la room, s'il part
  avant d'être relié, ou si la room se ferme ; avec le code de l'incident (`ice` sans repli,
  `timeout`, `proof`, `room-full`) si le lien échoue ; avec `timeout` au-delà de
  `connectMs + 2 × handshakeMs` en tout cas. L'incident est aussi émis en `error`, comme en
  `'auto'`.
- **Un lien coupé ne revient pas tout seul** : après un `leave` (le peer est parti, ou un peer de
  repli perdu avec le relay), rappeler `room.connect(id)` s'il le faut encore.
- **Les deux modes se mélangent** dans une room : un nouveau venu en `'auto'` se relie à tous les
  membres, `'manual'` compris ; un membre en `'manual'` ne se relie qu'à qui il appelle.
- `connect()` marche aussi en `'auto'` : il donne le peer déjà relié, ou attend qu'il le soit.
- Le relay n'a rien de nouveau à faire : il liste déjà les membres et transmet les offres.

## Peer

| Membre | |
|---|---|
| `peer.id` | son id dans la room |
| `peer.via` | `'direct'` ou `'relay'` (repli) |
| `peer.send(data, options?)` | `Promise`, voir ci-dessous |
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

### Envoi non fiable : `peer.send(data, { reliable: false })`

Pour ce qui se périme vite (positions d'un jeu, curseurs) : un message perdu n'est pas renvoyé et
ne retarde pas les suivants.

- En direct, il part sur un second canal WebRTC, **sans ordre et sans renvoi**. Il peut arriver
  dans le désordre, ou pas du tout. Il arrive par le même `peer.on('message')`.
- **Un message = une trame** : au plus 16 384 octets (et pas plus que `maxMessageBytes`), sinon
  `too-big`. Viser ~1 Ko pour qu'il tienne dans un paquet réseau.
- **N'attend jamais** : la promesse se résout `true` si le message est parti, `false` s'il a été
  jeté parce que plus de 64 Ko attendent déjà sur ce canal. Rejetée avec `closed` / `too-big`
  comme l'autre.
- **Par le relay** (`peer.via === 'relay'`), il prend le chemin fiable (ordonné, renvoyé) et se
  résout `true` une fois accepté ; même règle de taille, pour que l'app ne change pas de
  comportement selon le chemin.
- Le plafond de réception par peer compte les deux canaux ensemble.
- Côté Node (werift 0.25), un message **envoyé** depuis Node sur ce canal est sans ordre mais
  **renvoyé** s'il se perd (il arrive en retard au lieu de jamais) : werift ignore
  `maxRetransmits: 0`. Un navigateur qui envoie le jette bien.

Sans option (ou `reliable: true`), `send()` reste fiable et ordonné, et sa promesse se résout sans
valeur.

## Plusieurs rooms à la fois

- **Une room = une connexion au relay**, ouverte de `joinRoom()` à `room.leave()` (ou à la fin de
  la page / du process). Elle reste ouverte pour accueillir un nouveau venu, se reconnecter, et
  porter le repli. Au repos : un ping toutes les 5 s.
- **Le débit est par connexion** : chaque `joinRoom()` a son propre seau (`sendRate` /
  `sendBurst`) et le relay lui accorde le débit de sa clé. Dix rooms ne se partagent pas un débit.
- **Une room relie tout le monde à tout le monde** par défaut (au plus `max`, plafonné par
  `RELAY_ROOM_MAX`, 8 par défaut). Pour ne relier que certains membres, `connect: 'manual'` et
  `room.connect(id)` (voir « Liens à la demande »). Pour des liens que les autres ne doivent même
  pas voir, une room par lien, chacune avec son secret :

  ```
  A1 ⇄ B1, A2 ⇄ B2          deux rooms : chaque paire la sienne
  A1 ⇄ B1, A2 ⇄ B1          B1 fait deux joinRoom(), un par A : A1 et A2 ne se voient pas
  A1, A2, B1 ensemble       une room max: 3 : A1 et A2 se connectent aussi entre eux
  ```

- **Plafond par IP** : le relay accepte au plus `RELAY_IP_CONN` connexions ouvertes par IP publique
  (30 sur le relay en ligne). Une maison derrière sa box, ou des téléphones en 4G derrière le NAT de
  l'opérateur, partagent ce plafond. Au-delà, la connexion est refusée (`429`) ; ce refus **n'est
  pas une faute** et ne mène jamais à un ban. Un navigateur ne voit pas le `429` : la room réessaie
  seule. Côté Node, à l'entrée, il donne `banned` (le relay ne dit pas si l'IP est bannie ou
  pleine) : réessayer plus tard est alors à l'app.

## Repli par le relay

Quand le direct n'aboutit pas en `connectMs` (20 s), et avec `fallback: true`, les messages vers ce
peer passent par le relay : `peer.via === 'relay'`. Le relay ne voit que des boîtes chiffrées.

- **Fiable et ordonné**, même pour `{ reliable: false }`.
- **Coût** : un message de l'app = une trame par tranche de 11 Ko, et `room.broadcast()` = une trame
  **par peer en repli** (chaque boîte est chiffrée pour son destinataire). Ces trames passent par le
  seau de la connexion (100 / s par défaut), signalisation comprise.
- **Au-delà du seau, un message attend**, il n'est jamais jeté : la latence monte. Pour un jeu,
  envoyer **un message par tick** (tout ce qui a changé), pas un par événement :

  | messages / s par peer | 2 peers | 4 peers | 8 peers |
  |---|---|---|---|
  | 10 | 10 / s | 30 / s | 70 / s |
  | 20 | 20 / s | 60 / s | 140 / s : attend |
  | 30 | 30 / s | 90 / s | 210 / s : attend |

  (messages / s × peers en repli, contre 100 / s.) Au-delà, demander à l'exploitant du relay un débit
  plus haut pour sa clé, puis monter `sendRate` / `sendBurst` : le client se tient de lui-même à 5/6
  de ce que le relay accorde.
- **Côté réception**, chaque côté coupe un peer au-delà de 1000 trames / s (`peer-rate`).

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

Deux précisions issues de l'implémentation :

- `relay` est **fatal** dans deux cas qui ne peuvent pas se produire avec `joinRoom()`
  (codes de fermeture 4400 et 1009 : incompatibilité de protocole ; réessayer ne servirait
  à rien et compterait comme une faute pour le relay).
- `banned` n'est observable que côté Node, et seulement à l'entrée : un navigateur ne voit
  pas le statut HTTP 429 (il reçoit `relay`, non fatal, et la room réessaie) ; une fois dans
  la room, un 429 à la reconnexion est aussi `relay`. Le tableau complet des codes de
  fermeture est dans le README du paquet.

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
   déchiffré**. Ce qui arrive sur le canal non fiable avant la preuve est jeté sans
   être lu ; tout autre canal ouvert par un peer est fermé. Un peer légitime n'envoie
   de données qu'après le « ok » de l'autre : le lag rallonge l'attente, il ne coupe
   jamais un peer honnête.
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
| `RELAY_KEYS` | vide | les clés d'accès, une par application : `nom:clé[:débit[:rafale]]`, séparées par des virgules (voir plus bas) |
| `RELAY_KEY` | vide | une seule clé (liste d'une clé nommée `default`) ; vide avec `RELAY_KEYS` vide = relay ouvert à tous |
| `RELAY_ROOM_MAX` | `8` | plafond du `max` d'une room |
| `RELAY_MAX_BYTES` | `16384` | taille max d'un message ; au-delà, coupé (1009) |
| `RELAY_HELLO_MS` | `5000` | délai pour recevoir le `hello` |
| `RELAY_IDLE_MS` | `30000` | silence max après le `hello` |
| `RELAY_RATE` / `RELAY_BURST` | `120` / `240` | messages par seconde par connexion : avant le `hello`, sur un relay ouvert, pour l'ancien protocole, et pour une clé sans débit à elle |
| `RELAY_OFFERS_PER_MIN` | `6` | offres max d'un peer vers un même peer |
| `RELAY_IP_CONN` | `10` | connexions simultanées par IP ; au-delà, refus `429`, qui n'est pas une faute (voir « Plusieurs rooms à la fois ») |
| `RELAY_PENDING_MAX` | `200` | connexions sans `hello`, au total |
| `RELAY_CONN_MAX` | `2000` | connexions au total |
| `RELAY_BAN_AFTER` / `RELAY_BAN_MS` | `5` / `600000` | 5 fautes en 10 min → ban de 10 min, doublé à chaque récidive |
| `TRUST_PROXY` | vide | IP ou CIDR du reverse proxy, séparés par des virgules ; seulement de là, `X-Forwarded-For` donne l'IP réelle |
| `RELAY_LEGACY` | `0` | `1` accepte aussi les anciens clients sans room (ancien protocole) |
| `TLS_CERT` / `TLS_KEY` | — | servir `wss://` sans proxy |
| `TLS_WATCH_MS` | `60000` | fréquence de vérification d'un certificat renouvelé ; `0` = seulement sur `SIGHUP` |
| `RELAY_LOG_DATA` | — | `1` journalise le début de chaque `data` relayé (chiffré), pour déboguer |

`GET /healthz` répond `200 ok`.

### Clés d'accès

```
RELAY_KEYS=debrid-remote:<clé>:120:240,signal-test-page:<clé>:30:60,mmo:<clé>:400:800
```

- **Une clé par application**, chacune avec son débit **par connexion** : chaque connexion qui
  présente la clé a son propre seau de `débit` / `rafale` (par défaut `RELAY_RATE` / `RELAY_BURST`).
  Avant son `hello`, une connexion a le seau par défaut ; la clé reconnue, elle passe au sien, plein.
- La clé est vérifiée au `hello` seulement : une connexion ouverte garde sa clé jusqu'à sa
  reconnexion. Changer ou retirer une clé = modifier la variable et redémarrer le relay ; les rooms
  se reconnectent seules, les liens directs ne sont pas touchés.
- Le journal nomme la clé (`key=debrid-remote`), jamais sa valeur. Une liste mal formée, deux
  noms ou deux clés identiques : le relay refuse de démarrer, sans afficher de clé.
- `RELAY_KEY` seule = une liste d'une clé, `default`, au débit par défaut. `RELAY_KEY` **et**
  `RELAY_KEYS` : le relay refuse de démarrer. Ni l'une ni l'autre : relay ouvert.
- Un `hello` v1 sans clé, ou avec une clé inconnue, sur un relay à clés : 4401.
- Avec `RELAY_LEGACY=1`, un `hello` de l'ancien protocole (sans `v`) entre **sans clé**, au débit
  par défaut, jusqu'à ce que ses clients migrent.
- Une clé ne protège que la bande passante du relay : les données restent chiffrées de bout en
  bout par le secret de la room. Une clé dans une page publique est publique.

## Protocole du relay (pour déboguer)

L'app n'a pas à l'implémenter : `joinRoom()` le parle. JSON texte, version 1.

| Message | Envoyé par | Le relay |
|---|---|---|
| `{type:'hello', v:1, id, room, max?, key?}` | chaque peer, en premier | entre dans `room` ; répond `{type:'welcome', peers:[{id}], max, creator, rate, burst}` (`peers` soi compris, `creator` booléen : `true` pour celui qui a créé la room, `rate` / `burst` : le débit accordé à cette connexion) ; envoie `{type:'peers', list:[{id}]}` à la room |
| `{type:'signal', to, kind, box}` | un peer | `kind` en clair : `'offer'`, `'answer'` ou `'ice'` ; transmet `{type:'signal', from, kind, box}` à `to` dans la même room. Le relay compte les `offer` (`RELAY_OFFERS_PER_MIN`) ; le client rejette une boîte dont le contenu ne correspond pas à `kind` |
| `{type:'data', to?, box}` | un peer (repli) | transmet `{type:'data', from, box}` à `to`, ou à toute la room |
| `{type:'block', id}` | un peer | ne lui transmet plus rien de `id` |
| `{type:'ping', ts}` | chaque peer, toutes les 5 s | répond `{type:'pong', ts, srv}` |

`box` est opaque pour le relay (chiffré). Codes de fermeture : 4400 `hello` mal
formé, 4401 clé ou `hello` absent / en retard, 4403 room pleine, 4408 silence
au-delà de `RELAY_IDLE_MS`, 4409 id pris, 4429 débit ou trop d'offres, 1009
message trop gros. Une IP bannie ou au-delà des plafonds reçoit
un HTTP 429 avant même le WebSocket.

## Coder avec

- Écrire le code de l'app contre ce contrat : `joinRoom`, les événements, les
  codes d'erreur. **Ne pas réimplémenter le protocole** : le chiffrement et la
  preuve sont la partie qu'on ne veut écrire qu'une fois.
- Le relay en ligne `wss://signal.du.ranchchoco.boo` parle ce protocole (v1, plus l'ancien pour
  ses anciens clients) et demande une `relayKey` : une par application, donnée par son exploitant.
  Une clé refusée est une faute pour le relay (`key`, fatale, la room ne réessaie pas) : cinq en
  10 min bannissent l'IP. Ne pas boucler sur `joinRoom()` après un `key`.
- Pour tester sans clé, lancer un relay local (`npx github:meitogi/signal#v0.1.2`, puis
  `relay: 'ws://localhost:8787'`).

## Changements

- 2026-10-23 — premier brouillon.
- 2026-10-23 — protocole du relay : `signal` porte `kind` en clair (`offer` /
  `answer` / `ice`) pour que le relay puisse plafonner les offres sans lire la
  boîte ; nouveau code de fermeture 4408 (silence après le `hello`) ; `creator`
  du `welcome` est un booléen et `peers` inclut soi ; `TRUST_PROXY` accepte une
  liste ; `TLS_WATCH_MS` et `RELAY_LOG_DATA` ajoutées au tableau. Rien ne change pour l'app : `joinRoom()` parle ce protocole.
- 2026-10-23 — client : `relay` est fatal sur les fermetures 4400 et 1009 (incompatibilité
  de protocole) ; `banned` n'est observable que côté Node, à l'entrée. Rien ne change pour
  l'app.
- 2026-10-27 — relay : `RELAY_KEYS`, une clé par application avec son débit par connexion
  (`RELAY_KEY` seule reste une liste d'une clé) ; l'ancien protocole entre sans clé ; le
  `welcome` v1 porte `rate` / `burst`. Client : options `sendRate` / `sendBurst` (défauts
  inchangés, plafonnés à 5/6 de ce qu'accorde le relay) ; `peer.send(data, { reliable: false })`
  sur un canal sans ordre ni renvoi (une trame, jamais d'attente, chemin fiable par le relay) ;
  `formatLink` / `parseLink` portent `relayKey` (`&key=`) ; `iceServers: []` n'interroge plus
  aucun serveur STUN côté Node. Rien ne change pour une app qui n'utilise pas ces nouveautés.
- 2026-10-27 — utilisable : installation depuis GitHub (`#v0.1.0`) en attendant npm ; le relay en
  ligne parle la v1 avec des clés. Relay : un refus au plafond par IP (`RELAY_IP_CONN`) n'est plus
  une faute vers un ban. Nouvelles sections « Plusieurs rooms à la fois » et « Repli par le
  relay ». Rien ne change pour l'app.
- 2026-10-27 — 0.1.1, client : le créateur d'une room ne compte plus la négociation directe
  qu'un peer abandonne quand ce peer se replie par le relay ; une room à `max: 2` acceptait
  mal ce repli (`room-full` d'un côté, `timeout` de l'autre). Rien ne change pour l'app.
- 2026-10-27 — 0.1.2, client : option `connect: 'manual'` (on ne se relie à personne en entrant),
  `room.connect(id)` pour un lien à la demande, `room.members` et l'événement `members` pour savoir
  qui est là. Deux offres qui se croisent donnent un seul lien (l'id le plus petit l'emporte). Le
  défaut reste `'auto'` : rien ne change pour une app existante. Relay inchangé.
