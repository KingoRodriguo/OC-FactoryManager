# GTNH OpenComputers Machine Monitor

Monitoring centre machines pour GTNH/OpenComputers sur serveur public.

Le design évite tout accès aux fichiers du serveur Minecraft. Les slaves OC mesurent les machines et journalisent localement. Le central OC reçoit les slaves via modem câblé, ACK immédiatement, bufferise, puis envoie des batches HTTP périodiques vers un exécutable lancé sur le PC du joueur. Les métriques sont calculées dans l'interface web, à partir des événements bruts.

## Structure

- `oc/slave.lua`: agent OpenComputers à placer sur chaque machine surveillée.
- `oc/central.lua`: collecteur OpenComputers central.
- `oc/lib/bn/*.lua`: bibliothèques OC partagées.
- `pc/monitor.py`: exécutable PC, endpoint `/ingest`, stockage SQLite, dashboard web.
- `tests/`: tests Python du stockage et de la déduplication.

## Installation PC

```bash
python3 pc/monitor.py --host 127.0.0.1 --port 8765 --db data/monitor.sqlite3 --data-dir data/raw
```

Expose ensuite `http://127.0.0.1:8765/ingest` via ton tunnel préféré, par exemple Cloudflare Tunnel, Tailscale Funnel ou ngrok. Mets l'URL publique dans `/etc/bn-central.cfg` côté OpenComputers:

```lua
{
  port = 4242,
  upload_url = "https://ton-tunnel.example/ingest",
  upload_interval_s = 60,
  max_batch_events = 500,
  max_batch_bytes = 12000,
  buffer_capacity_events = 2500,
  force_upload_buffer_pct = 80,
  retry_backoff_s = 30,
  max_retry_backoff_s = 600,
  update_manifest_url = "",
}
```

Le dashboard est disponible sur `http://127.0.0.1:8765`.

## Installation OpenComputers

Sur chaque ordinateur OC, copie `oc/lib/bn` vers `/usr/lib/bn`.

Central:

```text
/usr/bin/bn-central.lua  <- oc/central.lua
/usr/lib/bn/*.lua       <- oc/lib/bn/*.lua
```

Slave:

```text
/usr/bin/bn-slave.lua   <- oc/slave.lua
/usr/lib/bn/*.lua       <- oc/lib/bn/*.lua
```

Au premier lancement, les scripts créent leurs configs:

- Central: `/etc/bn-central.cfg`
- Slave: `/etc/bn-slave.cfg`

Configure au minimum chaque slave:

```lua
{
  label = "EBF 1",
  group = "ebf",
  machine_type = "auto",
  machine_address = "",
  port = 4242,
}
```

Si `machine_address` est vide, le slave choisit le premier composant non système visible via l'Adapter. Pour les machines qui n'exposent pas de méthode exploitable, configure un signal redstone avec `redstone_side`.

## Protocole de données

Les slaves écrivent un journal append-only dans `/var/bn-slave/events.log`. Chaque événement a un `event_seq` monotone par slave. Le central déduplique implicitement par dernier ACK de chaque slave et le PC déduplique définitivement avec `(slave_id, event_seq)`.

Le central écrit les événements non uploadés dans `/var/bn-central/pending.log`. Si l'endpoint PC ou le tunnel tombe, il continue d'ACK les slaves et réessaie l'upload plus tard.

## Updates

Le central peut télécharger un manifest public en lecture seule via `update_manifest_url`. Le format attendu est une table Lua sérialisée OpenComputers:

```lua
{
  version = "0.1.1",
  entrypoint = "/usr/bin/bn-slave.lua",
  files = {
    ["/usr/bin/bn-slave.lua"] = {
      url = "https://raw.githubusercontent.com/user/repo/main/oc/slave.lua",
      fnv1a32 = "0123abcd",
    },
    ["/usr/lib/bn/protocol.lua"] = {
      url = "https://raw.githubusercontent.com/user/repo/main/oc/lib/bn/protocol.lua",
      fnv1a32 = "89abcdef",
    },
  },
}
```

Les fichiers sont envoyés aux slaves par chunks via modem. Le slave vérifie `fnv1a32` si fourni, garde une copie de l'ancien fichier dans `/var/bn-slave/previous`, puis redémarre après update.

## Tests

```bash
python3 -m unittest discover -s tests
```

## Limites connues

- Les métriques sont centrées machines, pas items AE2.
- La précision dépend des méthodes exposées par la machine via Adapter. Si aucun statut/progress n'est disponible, utilise un fallback redstone et la confiance sera plus basse.
- Le checksum FNV-1a sert surtout à détecter les corruptions de transfert. Ce n'est pas une signature de sécurité.
