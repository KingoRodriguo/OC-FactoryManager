local component = require("component")
local computer = require("computer")
local event = require("event")
local filesystem = require("filesystem")
local serialization = require("serialization")

local fsutil = require("bn.fsutil")
local hash = require("bn.hash")
local json = require("bn.json")
local protocol = require("bn.protocol")

local VERSION = "0.1.0"
local ROOT = "/var/bn-central"
local CONFIG_PATH = "/etc/bn-central.cfg"
local PENDING_PATH = ROOT .. "/pending.log"
local SLAVES_PATH = ROOT .. "/slaves.dat"
local STATE_PATH = ROOT .. "/state.dat"
local UPDATE_CACHE = ROOT .. "/updates"

local default_config = {
  port = 4242,
  upload_url = "http://127.0.0.1:8765/ingest",
  upload_interval_s = 60,
  max_batch_events = 500,
  max_batch_bytes = 12000,
  buffer_capacity_events = 2500,
  force_upload_buffer_pct = 80,
  force_upload_buffer_events = nil,
  retry_backoff_s = 30,
  max_retry_backoff_s = 600,
  update_check_interval_s = 3600,
  update_manifest_url = "",
  slave_target_version = "",
  update_chunk_size = 6000,
}

local function load_table(path, fallback)
  local raw = fsutil.read_all(path)
  if not raw or raw == "" then return fallback end
  local ok, value = pcall(serialization.unserialize, raw)
  if ok and type(value) == "table" then return value end
  return fallback
end

local function save_table(path, value)
  fsutil.write_all(path, serialization.serialize(value))
end

local function ensure_config()
  if not filesystem.exists("/etc") then filesystem.makeDirectory("/etc") end
  if not filesystem.exists(CONFIG_PATH) then
    fsutil.write_all(CONFIG_PATH, serialization.serialize(default_config))
  end
  local config = load_table(CONFIG_PATH, default_config)
  for key, value in pairs(default_config) do
    if config[key] == nil then config[key] = value end
  end
  return config
end

local function find_modem()
  for address, _ in component.list("modem") do
    return component.proxy(address)
  end
  error("no modem component found")
end

local function find_internet()
  for address, _ in component.list("internet") do
    return component.proxy(address)
  end
  return nil
end

local function now()
  return math.floor(computer.uptime())
end

local function load_pending()
  local events = {}
  for _, line in ipairs(fsutil.read_lines(PENDING_PATH)) do
    local ok, value = pcall(serialization.unserialize, line)
    if ok and type(value) == "table" then
      events[#events + 1] = value
    end
  end
  return events
end

local function rewrite_pending(events)
  local handle = io.open(PENDING_PATH, "w")
  if not handle then return false end
  for _, item in ipairs(events) do
    handle:write(serialization.serialize(item))
    handle:write("\n")
  end
  handle:close()
  return true
end

local function append_pending(item)
  fsutil.append_line(PENDING_PATH, serialization.serialize(item))
end

local function estimate_json_size(events)
  local size = 0
  for _, item in ipairs(events) do
    size = size + #(json.encode(item)) + 2
  end
  return size
end

local function read_http_all(handle)
  local chunks = {}
  while true do
    local chunk = handle.read()
    if not chunk then break end
    chunks[#chunks + 1] = chunk
  end
  return table.concat(chunks)
end

local function http_post(internet, url, payload)
  if not internet then return false, "no internet card" end
  local headers = {
    ["Content-Type"] = "application/json",
    ["User-Agent"] = "gtnh-bottleneck-central/" .. VERSION,
  }
  local ok, handle_or_err = pcall(internet.request, url, payload, headers)
  if not ok or not handle_or_err then
    return false, tostring(handle_or_err)
  end
  local ok_read, response = pcall(read_http_all, handle_or_err)
  if not ok_read then return false, tostring(response) end
  if response and response:find('"accepted"%s*:%s*true') then
    return true, response
  end
  return false, response or "empty response"
end

local function http_get(internet, url)
  if not internet then return nil, "no internet card" end
  local ok, handle_or_err = pcall(internet.request, url)
  if not ok or not handle_or_err then return nil, tostring(handle_or_err) end
  local ok_read, response = pcall(read_http_all, handle_or_err)
  if not ok_read then return nil, tostring(response) end
  return response
end

local function build_batch(config, state, pending)
  local selected = {}
  local size = 0
  local limit_events = math.min(config.max_batch_events, #pending)
  for i = 1, limit_events do
    local item = pending[i]
    local item_size = #(json.encode(item)) + 2
    if #selected > 0 and size + item_size > config.max_batch_bytes then break end
    selected[#selected + 1] = item
    size = size + item_size
  end
  if #selected == 0 then return nil end

  state.batch_counter = (state.batch_counter or 0) + 1
  local first = selected[1].event
  local last = selected[#selected].event
  local batch = {
    source_server_id = state.server_id,
    batch_id = state.server_id .. "-" .. tostring(os.time()) .. "-" .. tostring(state.batch_counter),
    first_event_seq = first and first.event_seq or 0,
    last_event_seq = last and last.event_seq or 0,
    created_at = os.time(),
    events = {},
  }
  for _, item in ipairs(selected) do
    batch.events[#batch.events + 1] = item.event
  end
  batch.checksum = hash.fnv1a32(json.encode(batch.events))
  return batch, #selected
end

local function update_slave(slaves, slave_id, patch)
  local slave = slaves[slave_id] or {}
  for key, value in pairs(patch) do slave[key] = value end
  slave.last_seen_uptime = now()
  slaves[slave_id] = slave
end

local function handle_register(config, modem, remote, slaves, message)
  update_slave(slaves, message.slave_id, {
    label = message.label or "unnamed",
    group = message.group or "default",
    machine_type = message.machine_type or "unknown",
    script_version = message.script_version or "unknown",
    protocol_version = message.protocol_version or 0,
    capabilities = message.capabilities or {},
    modem_address = remote,
    status = "online",
  })
  protocol.send(modem, remote, config.port, {
    type = "register_ack",
    slave_id = message.slave_id,
    central_version = VERSION,
  })
  if config.slave_target_version ~= "" and message.script_version ~= config.slave_target_version then
    protocol.send(modem, remote, config.port, {
      type = "update_available",
      target_version = config.slave_target_version,
      required = false,
    })
  end
end

local function handle_heartbeat(config, modem, remote, slaves, message)
  update_slave(slaves, message.slave_id, {
    state = message.state or "unknown",
    script_version = message.script_version or "unknown",
    disk_free = message.disk_free,
    backlog = message.backlog,
    modem_address = remote,
    status = "online",
  })
  protocol.send(modem, remote, config.port, {
    type = "heartbeat_ack",
    slave_id = message.slave_id,
    server_time = os.time(),
  })
end

local function handle_sync_batch(config, modem, remote, slaves, pending, message)
  if type(message.events) ~= "table" or not message.slave_id then return end
  local slave = slaves[message.slave_id] or {}
  local acked = slave.acked_to_central or 0
  local accepted = 0
  for _, ev in ipairs(message.events) do
    if type(ev) == "table" and type(ev.event_seq) == "number" and ev.event_seq > acked then
      pending[#pending + 1] = {received_at = os.time(), event = ev}
      append_pending(pending[#pending])
      acked = ev.event_seq
      accepted = accepted + 1
    end
  end
  update_slave(slaves, message.slave_id, {
    acked_to_central = acked,
    modem_address = remote,
    status = "online",
  })
  protocol.send(modem, remote, config.port, {
    type = "sync_ack",
    slave_id = message.slave_id,
    acked_to_seq = acked,
    accepted = accepted,
    server_time = os.time(),
  })
end

local function fetch_update_manifest(config, internet)
  if not config.update_manifest_url or config.update_manifest_url == "" then
    return nil, "update_manifest_url empty"
  end
  local raw, err = http_get(internet, config.update_manifest_url)
  if not raw then return nil, err end
  local ok, manifest = pcall(serialization.unserialize, raw)
  if not ok or type(manifest) ~= "table" then
    return nil, "manifest is not a serialized Lua table"
  end
  return manifest
end

local function cache_update_file(config, internet, manifest, path)
  local file = manifest.files and manifest.files[path]
  if not file or not file.url then return nil, "file not in manifest" end
  local cache_name = UPDATE_CACHE .. "/" .. path:gsub("[/%\\:]", "_")
  if filesystem.exists(cache_name) then return fsutil.read_all(cache_name) end
  local data, err = http_get(internet, file.url)
  if not data then return nil, err end
  if file.fnv1a32 and hash.fnv1a32(data) ~= file.fnv1a32 then
    return nil, "checksum mismatch for " .. path
  end
  fsutil.write_all(cache_name, data)
  return data
end

local function handle_update_request(config, modem, remote, internet, manifest, message)
  if not manifest then
    protocol.send(modem, remote, config.port, {type = "update_error", error = "no manifest loaded"})
    return
  end
  if message.type == "update_manifest_request" then
    protocol.send(modem, remote, config.port, {
      type = "update_manifest",
      version = manifest.version,
      files = manifest.files,
      entrypoint = manifest.entrypoint,
    })
  elseif message.type == "update_chunk_request" then
    local data, err = cache_update_file(config, internet, manifest, message.file_path)
    if not data then
      protocol.send(modem, remote, config.port, {type = "update_error", error = err})
      return
    end
    local chunk_size = config.update_chunk_size
    local index = message.chunk_index or 1
    local start_pos = ((index - 1) * chunk_size) + 1
    local chunk = data:sub(start_pos, start_pos + chunk_size - 1)
    protocol.send(modem, remote, config.port, {
      type = "update_chunk",
      file_path = message.file_path,
      chunk_index = index,
      chunk = chunk,
      done = start_pos + chunk_size > #data,
    })
  end
end

fsutil.ensure_dir(ROOT)
fsutil.ensure_dir(UPDATE_CACHE)

local config = ensure_config()
local modem = find_modem()
local internet = find_internet()
local slaves = load_table(SLAVES_PATH, {})
local state = load_table(STATE_PATH, {})
local pending = load_pending()
local manifest = nil

if not state.server_id then
  state.server_id = "central-" .. computer.address():sub(1, 8)
  save_table(STATE_PATH, state)
end

modem.open(config.port)
print("GTNH bottleneck central " .. VERSION)
print("server_id=" .. state.server_id .. " port=" .. tostring(config.port))
print("pending_events=" .. tostring(#pending) .. " upload_interval_s=" .. tostring(config.upload_interval_s))

local next_upload = now() + config.upload_interval_s
local next_save = now() + 15
local next_update_check = now() + 5
local retry_after = 0
local retry_backoff = config.retry_backoff_s

while true do
  local timeout = 1
  local ev = {event.pull(timeout)}
  if ev[1] == "modem_message" then
    local remote = ev[3]
    local port = ev[4]
    local payload = ev[6]
    if port == config.port then
      local message = protocol.decode(payload)
      if message and message.type == "register" then
        handle_register(config, modem, remote, slaves, message)
      elseif message and message.type == "heartbeat" then
        handle_heartbeat(config, modem, remote, slaves, message)
      elseif message and message.type == "sync_batch" then
        handle_sync_batch(config, modem, remote, slaves, pending, message)
      elseif message and message.type == "update_result" then
        update_slave(slaves, message.slave_id, {
          update_status = message.success and "ok" or "failed",
          installed_version = message.installed_version,
          update_error = message.error,
          modem_address = remote,
        })
      elseif message and (message.type == "update_manifest_request" or message.type == "update_chunk_request") then
        handle_update_request(config, modem, remote, internet, manifest, message)
      end
    end
  end

  local current = now()
  local force_threshold = config.force_upload_buffer_events
  if not force_threshold then
    force_threshold = math.floor((config.buffer_capacity_events or 2500) * (config.force_upload_buffer_pct or 80) / 100)
  end
  local should_force_upload = #pending >= force_threshold
  if #pending > 0 and current >= retry_after and (current >= next_upload or should_force_upload) then
    local batch, selected_count = build_batch(config, state, pending)
    if batch then
      save_table(STATE_PATH, state)
      local ok, result = http_post(internet, config.upload_url, json.encode(batch))
      if ok then
        print("uploaded batch " .. batch.batch_id .. " events=" .. tostring(selected_count))
        for _ = 1, selected_count do table.remove(pending, 1) end
        rewrite_pending(pending)
        retry_backoff = config.retry_backoff_s
        next_upload = current + config.upload_interval_s
      else
        print("upload failed: " .. tostring(result))
        retry_after = current + retry_backoff
        retry_backoff = math.min(retry_backoff * 2, config.max_retry_backoff_s)
        next_upload = current + config.upload_interval_s
      end
    end
  end

  if current >= next_update_check then
    local loaded, err = fetch_update_manifest(config, internet)
    if loaded then
      manifest = loaded
      print("loaded update manifest version=" .. tostring(manifest.version))
      if manifest.version then config.slave_target_version = manifest.version end
    elseif config.update_manifest_url ~= "" then
      print("update check failed: " .. tostring(err))
    end
    next_update_check = current + config.update_check_interval_s
  end

  if current >= next_save then
    save_table(SLAVES_PATH, slaves)
    save_table(STATE_PATH, state)
    next_save = current + 15
  end
end
