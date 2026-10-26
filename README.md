# @meitogi/signal

Des appareils qui connaissent le même **secret** se retrouvent dans une **room**
et s'échangent des messages en direct (WebRTC). Le relay ne peut ni lire, ni
entrer, ni se faire passer pour l'un d'eux.

Le contrat complet (API, options, erreurs, garanties, protocole) est dans
[CONTRACT.md](CONTRACT.md).

## Installer

```
npm install @meitogi/signal
```

| Import | Où | Pour |
|---|---|---|
| `@meitogi/signal` | navigateur | `joinRoom()`, `newSecret()`, `formatLink()` / `parseLink()`, `SignalError` — aucune dépendance |
| `@meitogi/signal/node` | Node ≥ 22 | la même API ; WebRTC par [werift](https://github.com/shinyoshiaki/werift-webrtc) (pur JS, sans binaire natif), WebSocket par `ws` |
| `npx @meitogi/signal` | terminal | lance un relay (plus bas) |

## Rejoindre une room

```js
import { joinRoom, newSecret } from '@meitogi/signal'; // ou '@meitogi/signal/node'

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
room.on('error', (e) => console.warn('incident', e.code, e.message)); // la room continue
room.on('close', (error) => console.log('room terminée', error?.code)); // null après leave()

await room.ready; // on est dans la room
await room.broadcast('à tous');
room.leave();
```

`joinRoom()` retourne la room tout de suite : on branche les écouteurs avant que le
réseau ne bouge. `room.ready` dit quand on est entré (ou rejette avec une `SignalError`
fatale). Le relay peut tomber sans casser les connexions directes : la room s'y
reconnecte seule (0,5 s, 1 s, 2 s… jusqu'à 30 s) et émet `error` à chaque échec.

### Options

| Option | Défaut | |
|---|---|---|
| `relay` | **obligatoire** | `wss://…`, ou `ws://` seulement sur `localhost` / `127.0.0.1` |
| `room` | **obligatoire** | nom choisi par l'app, 1 à 64 caractères ; le relay ne le voit jamais |
| `secret` | **obligatoire** | sortie de `newSecret()` : 32 octets en base64url (43 caractères) |
| `max` | `8` | peers max, soi compris (≥ 2). Fixé par celui qui **crée** la room, plafonné par le relay |
| `relayKey` | — | clé d'accès si le relay est privé (`RELAY_KEY`) |
| `iceServers` | STUN Google | `[]` pour n'utiliser aucun serveur STUN |
| `fallback` | `true` | si le direct échoue, passer les messages par le relay (chiffrés, plus lents) |
| `connectMs` | `20000` | délai pour établir le direct avec un peer, avant repli ou abandon |
| `handshakeMs` | `10000` | délai pour que la preuve du secret aboutisse |
| `maxMessageBytes` | `1048576` | taille max d'un message envoyé **ou reçu** (1 Mio) |

### Room et Peer

| Membre | |
|---|---|
| `room.ready`, `room.id`, `room.isCreator`, `room.max`, `room.peers` | voir [CONTRACT.md](CONTRACT.md#room) |
| `room.on(event, fn)` | retourne la fonction qui désabonne ; événements `peer`, `leave`, `error`, `close` |
| `room.broadcast(data)` | envoie à tous les peers prêts |
| `room.leave()` | quitte, ferme tout ; `close` émis avec `null` |
| `peer.id`, `peer.via` | `'direct'` (WebRTC) ou `'relay'` (repli) |
| `peer.send(data)` | `string`, `Uint8Array` ou `ArrayBuffer` ; découpé et recollé au-delà de 16 Ko ; **attend** si la file d'envoi est pleine ; rejette `too-big` / `closed` |
| `peer.on('message', fn)` | `string` si l'autre a envoyé une chaîne, sinon `Uint8Array` |
| `peer.on('close', fn)` | `fn(reason)` : `closed` (fermé par l'un ou l'autre), `relay` (peer de repli perdu avec le relay), `peer-rate`, `too-big` |
| `peer.close()` | coupe ce peer seulement |

Pas de JSON imposé : `peer.send(JSON.stringify(obj))` et `JSON.parse(data)` côté app.

### Partager la room par un lien

```js
import { formatLink, parseLink } from '@meitogi/signal';

formatLink('https://mon-app.example/', { room: 'partie-du-soir', secret });
// → https://mon-app.example/#r=partie-du-soir&s=<secret>      (+ &relay=… si donné)

parseLink(location.href); // → { room, secret, relay? } ou null
```

Tout est après `#` : le navigateur n'envoie jamais cette partie au serveur web.

### Erreurs

Toutes les erreurs sont des `SignalError` avec `code` et `fatal`. Fatal : `room.ready`
rejette (ou `close` est émis si on était entré). Non fatal : `error` est émis, la room
continue. Les codes sont ceux de [CONTRACT.md](CONTRACT.md#erreurs) ; voici comment les
fermetures du relay s'y traduisent (`src/errors.js` porte la même table) :

| Fermeture du relay | `code` | Fatal | |
|---|---|---|---|
| 4401 | `key` | oui | `relayKey` absente ou refusée — **ou** `hello` arrivé en retard : indiscernables |
| 4403 | `room-full` | oui | à l'entrée, et aussi à une reconnexion (la place a été prise pendant l'absence) |
| 4408 | `relay` | non | silence de 30 s ; les pings toutes les 5 s le rendent rare (machine endormie) ; reconnexion |
| 4409 | `id-taken` | non | un nouvel id est tiré, reconnexion immédiate |
| 4429 | `rate` | oui | débit ou trop d'offres — hors d'atteinte par construction (voir Sécurité) |
| 4400, 1009 | `relay` | **oui** | incompatibilité de protocole ; impossible avec `joinRoom()`, et réessayer compterait comme une faute |
| HTTP 429 à l'upgrade | `banned` (Node, à l'entrée) · `relay` (Node une fois entré, et toujours dans un navigateur) | selon | ban **ou** plafond de connexions atteint : indiscernables ; un navigateur ne voit pas le statut HTTP |
| 1006, 1001, 1012, relay injoignable | `relay` | non | reconnexion avec backoff ; `ready` reste en attente tant qu'on n'est jamais entré |

Incidents sur un peer (non fatals, le peer est coupé) : `proof` (preuve ratée, boîte
illisible ou mensonge sur `kind`, message avant la preuve), `timeout` (preuve ou repli pas
aboutis à temps ; aussi une offre refusée faute de place : 4 poignées de main en cours, ou
3 offres déjà envoyées à ce peer dans la minute), `ice` (pas de chemin direct et
`fallback: false`), `peer-rate`, `too-big` (un peer a dépassé **notre** `maxMessageBytes`).

### Sécurité : ce qui est garanti

1. **Le secret ne quitte jamais l'appareil.** Le relay reçoit `HKDF-SHA256(secret, room)`
   comme id de room (43 caractères) ; deux groupes de même nom et de secrets différents
   ne se croisent jamais.
2. **Tout ce qui passe par le relay est chiffré** (AES-GCM-256, clé dérivée du secret) :
   offres, réponses, candidats ICE, messages de repli. Les données additionnelles lient
   room, expéditeur et destinataire ; un compteur par expéditeur refuse les rejeux. Une
   boîte qui ne s'ouvre pas, est rejouée, ou dont le `kind` scellé ne correspond pas au
   `kind` en clair est jetée et son expéditeur bloqué (`block`).
3. **Chaque peer prouve le secret avant tout échange**, par HMAC-SHA256 sur les empreintes
   DTLS des deux SDP, l'id de room et deux nonces frais : la preuve du répondeur voyage
   dans sa réponse scellée, celle de l'offrant est le premier message du canal, puis `ok`.
   `peer` n'est émis qu'à ce `ok`.
4. **Rien n'est lu avant la preuve.** Sur le canal, avant la preuve, un message binaire,
   de plus de 1 Ko, ou qui n'est pas la preuve attendue coupe le peer sans être parsé.
5. **On ne paie rien pour un inconnu.** Aucune `RTCPeerConnection` sans une offre qui se
   déchiffre ; 4 poignées de main entrantes au plus ; une offre en double est jetée ; le
   créateur refuse au-delà de `max` ; après la preuve, un peer qui dépasse le débit
   autorisé est coupé (`peer-rate`) ; notre SDP annonce `a=max-message-size:65536`.
6. **Le repli par le relay est prouvé par l'enveloppe elle-même.** Sans DTLS, c'est la
   boîte scellée qui prouve le secret : elle ne s'ouvre qu'avec la clé de la room, ses
   données additionnelles la lient à l'expéditeur et au destinataire, son compteur la rend
   non rejouable. Le repli s'ouvre par un `join` scellé (de celui qui offrait) et un `ok`
   scellé en retour ; les deux côtés obtiennent un peer `via: 'relay'`.
7. **On reste sous les limites du relay par construction.** Au plus 3 offres par minute
   vers un même peer (le relay en tolère 6), un seul seau de jetons pour tout ce qui sort
   (100 messages/s, le relay en tolère 120), des trames de repli de 11 Ko (le relay coupe
   à 16 Ko), un `ping` toutes les 5 s (le relay coupe après 30 s de silence).

Ce qui reste à l'app : savoir **qui** est le peer au-delà de « il connaît le secret ».

### Réglages avancés

`limits: { peerRate, peerBurst }` règle le plafond de réception par peer (défaut 1000
trames/s, rafale 2000). Hors contrat : sert aux tests, à ne toucher qu'en connaissance
de cause.

### Coder avant la publication

Le paquet n'est pas encore sur npm : il est fourni en archive,
`npm install ./meitogi-signal-0.1.0.tgz`, et rien ne change le jour où il y arrive. Pour
tester en local : `npx @meitogi/signal` (ou `node bin/signal.js` depuis ce dépôt), puis
`relay: 'ws://localhost:8787'`. Le relay en ligne `wss://signal.du.ranchchoco.boo` parle
encore l'ancien protocole ; il passera à la v1 après la sortie du paquet.

## Lancer un relay

```
npx @meitogi/signal
```

ou avec Docker, depuis ce dépôt :

```
docker build -t signal-relay .
docker run -d --name signal-relay -p 8787:8787 signal-relay
```

Le relay écoute en `ws://` sur le port 8787. Un navigateur exige `wss://` : le
mettre derrière un reverse proxy ([deploy/](deploy/), plus bas), ou lui donner un
certificat avec `TLS_CERT` / `TLS_KEY`, et il sert `wss://` lui-même. Node 22 ou
plus ; le relay lui-même n'a besoin que de `ws` (`werift` ne sert qu'à l'entrée Node du
client).

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

`GET /healthz` répond `200 ok` (healthcheck Docker, sondes des proxies).

### Ce que le relay refuse

- **Avant le WebSocket**, sur la requête HTTP `upgrade` : une IP bannie, une IP
  qui a déjà `RELAY_IP_CONN` connexions, plus de `RELAY_PENDING_MAX` connexions
  sans `hello`, plus de `RELAY_CONN_MAX` connexions. Réponse `429`, socket fermé :
  une connexion muette ne coûte rien de plus.
- **Pendant** : un message plus gros que `RELAY_MAX_BYTES` (1009, coupé à la
  réception) ; pas de `hello` valide en `RELAY_HELLO_MS` ou un premier message qui
  n'en est pas un (4401) ; un `hello` mal formé (4400) ; une room pleine (4403) ;
  un id déjà pris dans la room (4409) ; un débit au-delà de `RELAY_RATE` /
  `RELAY_BURST`, ou plus de `RELAY_OFFERS_PER_MIN` offres vers un même peer
  (4429) ; un silence de `RELAY_IDLE_MS` après le `hello` (4408). Un socket mort
  est détecté par un ping WebSocket toutes les 30 s.
- **Ban** : les fermetures 4400, 4401, 4429, 1009 et les refus au-delà de
  `RELAY_IP_CONN` comptent comme fautes. `RELAY_BAN_AFTER` fautes en 10 min
  bannissent l'IP pour `RELAY_BAN_MS`, puis le double à chaque récidive (la
  récidive est oubliée 24 h après la fin du ban). Room pleine, id pris, silence et
  plafonds globaux ne sont pas des fautes : ce n'est pas la faute de l'IP.

Le but : le relay ne tombe jamais ; au pire, il refuse les nouveaux venus.

### L'IP réelle derrière un proxy

Derrière un reverse proxy, toutes les connexions viennent de l'IP du proxy :
sans `TRUST_PROXY`, les plafonds par IP et les bans visent le proxy. Mettre dans
`TRUST_PROXY` l'adresse ou le sous-réseau du proxy (par exemple celui du réseau
Docker partagé) : le relay lit alors l'IP du client dans `X-Forwarded-For`, en
partant de la droite et en sautant les sauts de confiance. D'une autre source,
l'en-tête est ignoré, si bien qu'un client ne peut pas se donner une autre IP.

### Derrière votre reverse proxy

[deploy/](deploy/) contient un kit pour **Traefik**, **HAProxy** et **Caddy**. Les
trois lisent un seul `.env` :

```
cp deploy/.env.example deploy/.env     # RELAY_DOMAIN, RELAY_KEY, PROXY_NETWORK, TRUST_PROXY
cd deploy/traefik                      # ou haproxy, caddy
docker compose --env-file ../.env -p signal-relay up -d --build
```

- **Traefik** (provider docker) : les labels de `traefik/compose.yml` suffisent.
  Mettre vos noms dans `TRAEFIK_ENTRYPOINT` / `TRAEFIK_CERTRESOLVER`. Deux
  middlewares par IP client arrêtent un flood avant le relay : `inFlightReq`
  (`TRAEFIK_INFLIGHT` WebSockets ouverts, défaut 10) et `rateLimit`
  (`TRAEFIK_RATE` nouvelles connexions par seconde, rafale `TRAEFIK_RATE_BURST`).
- **HAProxy** : charger `haproxy/signal-relay.cfg` à côté de votre config
  (`haproxy -f haproxy.cfg -f signal-relay.cfg`, `RELAY_DOMAIN` dans
  l'environnement de HAProxy), puis ajouter la ligne `use_backend` de son en-tête
  à votre frontend HTTPS. `option forwardfor` transmet l'IP client,
  `timeout tunnel 1h` garde les WebSockets inactifs ouverts.
- **Caddy** : `import relay.caddy` dans votre Caddyfile, `RELAY_DOMAIN` dans
  l'environnement de Caddy. Caddy obtient le certificat, relaie les WebSockets et
  pose `X-Forwarded-For` tout seul.

Le conteneur rejoint le réseau du proxy (`PROXY_NETWORK`) sous le nom
`signal-relay`. Le DNS de `RELAY_DOMAIN` doit pointer directement sur le proxy :
un CDN devant (le nuage orange de Cloudflare) coupe les WebSockets inactifs.

`deploy/test/run.sh [traefik|haproxy|caddy|all]` lance chaque kit pour de vrai
avec son proxy dans Docker et vérifie, à travers lui, la connexion `wss://`, la
clé, le transfert, le délai du `hello`, le plafond par IP, et que le relay voit
bien l'IP du client.

Une fois en ligne, `node deploy/test/live-room.mjs wss://relay.example.org` fait
entrer deux peers Node dans une room à travers lui et échanger un message dans
chaque sens.

`node deploy/test/bench.mjs` est le banc de connectivité complet : il affiche un
lien vers la [page de test](https://github.com/meitogi/signal-test-page) (à ouvrir
sur un téléphone, en 4G ou sur le même Wi-Fi), puis vérifie des deux côtés le
chemin direct (candidats, paire choisie), le repli par le relay forcé (le second
arrivant entre avec `connectMs: 1`) et son débit, les échos texte et octets, un
gros message fragmenté sur chaque chemin, et mesure une cadence de jeu (paquets
à 30 / 60 / 120 Hz, avec ou sans regroupement par tick de 20 ms : RTT, gigue,
pertes). À lancer en natif (pas dans un conteneur Docker Desktop, qui n'a pas de
chemin direct) ; les options sont en tête du fichier. Chaque passage est enregistré dans
`deploy/test/results/` (ignoré par git : il contient des IP publiques) au nom de l'appareil et du
réseau (`ios-18.7-safari-18.7_other-net`, `macos-brave_same-net`…), avec une ligne de plus dans
`results.md` pour comparer les passages.

### La clé d'accès

Avec `RELAY_KEY`, le premier message d'une connexion doit être son `hello`, avec
`key`. Une mauvaise clé, un autre premier message, ou pas de `hello` à temps
ferment le socket en 4401. La comparaison est en temps constant. La clé n'est ni
un en-tête ni une query string : un navigateur ne peut pas poser d'en-tête sur un
WebSocket, et les query strings finissent dans les logs des proxies. Pour en
changer : modifier `RELAY_KEY`, redémarrer.

### Certificat renouvelé

Avec `TLS_CERT` / `TLS_KEY`, un certificat renouvelé est pris sans redémarrage :
quand les fichiers changent, ou sur `SIGHUP` (`docker kill -s HUP signal-relay`),
le relay les relit pour les nouvelles connexions. Les rooms et les sockets
ouverts ne bougent pas. Une paire qui ne se charge pas (clé qui ne correspond pas
au certificat, fichier à moitié écrit) est journalisée, et le certificat
précédent reste.

### Anciens clients (`RELAY_LEGACY=1`)

L'ancien protocole ouvert (`{type:'hello', id, name?, kind?}`, sans `v` ni `room`)
est accepté dans une room à part, `""`, avec `name` / `kind` dans les listes et
`signal` transmis tel quel. Les clients v1 et les anciens ne se voient jamais.
Cette room n'a pas de `max` et pas de délai de silence (un ancien serveur
n'envoie rien après son `hello`). Les autres limites s'appliquent. À couper
quand les anciens clients ont migré.

### Embarquer le relay

```js
import { createRelay } from '@meitogi/signal/relay';

const relay = await createRelay({ port: 8787, key: process.env.RELAY_KEY });
// relay.port, relay.stats(), relay.close()
```

Les options reprennent les variables ci-dessus (`roomMax`, `maxBytes`, `helloMs`,
`idleMs`, `rate`, `burst`, `offersPerMin`, `ipConn`, `pendingMax`, `connMax`,
`banAfter`, `banMs`, `trustProxy`, `legacy`, `tls`) ; détail dans
[src/relay.js](src/relay.js).

### Tests

```
npm ci && npm test        # protocole, ancien protocole, floods, bans, TLS
deploy/test/run.sh        # les trois kits de proxy, dans Docker
PEER=node RELAY=local node deploy/test/bench.mjs   # le banc, deux peers Node en local
```

## Licence

MIT — voir [LICENSE](LICENSE).
