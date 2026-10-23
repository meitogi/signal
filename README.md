# @meitogi/signal

Des appareils qui connaissent le même **secret** se retrouvent dans une **room**
et s'échangent des messages en direct (WebRTC). Le relay ne peut ni lire, ni
entrer, ni se faire passer pour l'un d'eux.

Le contrat complet (API, options, erreurs, garanties, protocole) est dans
[CONTRACT.md](CONTRACT.md).

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
plus, une seule dépendance (`ws`).

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
```

## Licence

MIT — voir [LICENSE](LICENSE).
