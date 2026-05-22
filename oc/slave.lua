local component = require("component")
local computer = require("computer")
local event = require("event")
local filesystem = require("filesystem")
local serialization = require("serialization")

local fsutil = require("bn.fsutil")
local hash = require("bn.hash")
local protocol = require("bn.protocol")

local VERSION = "0.1.0"
local ROOT = "/var/bn-slave"
local CONFIG_PATH = "/etc/bn-slave.cfg"
local ID_PATH = ROOT .. "/slave.id"
local JOURNAL_PATH = ROOT .. "/events.log"
local STATE_PATH = ROOT .. "/state.dat"
local STAGING_ROOT = ROOT .. "/staging"
local PREVIOUS_ROOT = ROOT .. "/previous"

local default_config = {
  port = 4242,
  label = "unnamed-machine",
  group = "default",
  machine_type = "auto",
  machine_address = "",
  poll_interval_s = 2,
  sample_interval_s = 30,
  heartbeat_interval_s = 10,
  sync_interval_s = 5,
  max_sync_events = 100,
  compact_confirmed_keep_s = 86400,
  blocked_after_s = 45,
  redstone_side = -1,
}

local ignored_component_types = {
  computer = true,
  data = true,
  eeprom = true,
  filesystem = true,
  gpu = true,
  internet = true,
  keyboard = true,
  modem = true,
  redstone = false,
  screen = true,
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

local function load_or_create_id()
  local existing = fsutil.read_all(ID_PATH)
  if existing and existing:gsub("%s+", "") ~= "" then
    return existing:gsub("%s+", "")
  end
  local id = "slave-" .. computer.address():sub(1, 8) .. "-" .. tostring(math.floor(computer.uptime() * 1000))
  fsutil.write_all(ID_PATH, id)
  return id
end

local function safe_call(proxy, method, ...)
  if not proxy or not method or not proxy[method] then return nil end
  local ok, result = pcall(proxy[method], ...)
  if ok then return result end
  return nil
end

local function list_methods(address)
  local methods = {}
  local ok, result = pcall(component.methods, address)
  if ok and type(result) == "table" then
    for name, _ in pairs(result) do methods[#methods + 1] = name end
  end
  table.sort(methods)
  return methods
end

local function choose_machine(config)
  if config.machine_address and config.machine_address ~= "" and component.type(config.machine_address) then
    return config.machine_address, component.type(config.machine_address)
  end
  for address, ctype in component.list() do
    if not ignored_component_types[ctype] then
      return address, ctype
    end
  end
  return nil, "none"
end

local function first_number(...)
  for i = 1, select("#", ...) do
    local value = select(i, ...)
    if type(value) == "number" then return value end
  end
  return nil
end

local probe = {
  progress_methods = {"getProgress", "getWorkProgress", "progress", "getCurrentProgress"},
  max_progress_methods = {"getMaxProgress", "getMaxWorkProgress", "maxProgress", "getMaxProgressTime"},
  active_methods = {"isActive", "isWorking", "isRunning", "hasWork", "isMachineActive"},
}

local function read_machine(proxy, context, config)
  local progress
  local max_progress
  for _, method in ipairs(probe.progress_methods) do
    progress = first_number(safe_call(proxy, method))
    if progress ~= nil then break end
  end
  for _, method in ipairs(probe.max_progress_methods) do
    max_progress = first_number(safe_call(proxy, method))
    if max_progress ~= nil then break end
  end

  local active
  for _, method in ipairs(probe.active_methods) do
    local value = safe_call(proxy, method)
    if type(value) == "boolean" then
      active = value
      break
    end
  end

  if active == nil and progress ~= nil then
    active = progress > 0
  end

  if active == nil and config.redstone_side and config.redstone_side >= 0 and component.isAvailable("redstone") then
    local rs = component.redstone
    local signal = safe_call(rs, "getInput", config.redstone_side)
    if type(signal) == "number" then active = signal > 0 end
  end

  local state = "unknown"
  local confidence = "low"
  local notes = "no recognized status method"
  if active == true then
    state = "active"
    confidence = progress and "high" or "medium"
    notes = progress and "progress/status method" or "active status method"
  elseif active == false then
    state = "idle"
    confidence = progress and "high" or "medium"
    notes = progress and "progress/status method" or "active status method"
  end

  if progress ~= nil then
    if context.last_progress == progress and state == "active" then
      context.same_progress_since = context.same_progress_since or computer.uptime()
      if computer.uptime() - context.same_progress_since >= config.blocked_after_s then
        state = "blocked"
        notes = "progress unchanged for " .. tostring(config.blocked_after_s) .. "s"
      end
    else
      context.same_progress_since = computer.uptime()
    end
    context.last_progress = progress
  end

  return {
    state = state,
    progress = progress,
    max_progress = max_progress,
    confidence = confidence,
    notes = notes,
  }
end

local function read_journal()
  local events = {}
  for _, line in ipairs(fsutil.read_lines(JOURNAL_PATH)) do
    local ok, value = pcall(serialization.unserialize, line)
    if ok and type(value) == "table" then events[#events + 1] = value end
  end
  table.sort(events, function(a, b) return (a.event_seq or 0) < (b.event_seq or 0) end)
  return events
end

local function append_event(state, event_data)
  state.event_seq = (state.event_seq or 0) + 1
  event_data.event_seq = state.event_seq
  event_data.timestamp = os.time()
  event_data.uptime_s = math.floor(computer.uptime())
  fsutil.append_line(JOURNAL_PATH, serialization.serialize(event_data))
  save_table(STATE_PATH, state)
  return event_data
end

local function compact_journal(state, keep_seconds)
  local events = read_journal()
  local cutoff = os.time() - keep_seconds
  local retained = {}
  for _, ev in ipairs(events) do
    if (ev.event_seq or 0) > (state.acked_to_central or 0) or (ev.timestamp or 0) >= cutoff then
      retained[#retained + 1] = ev
    end
  end
  local handle = io.open(JOURNAL_PATH, "w")
  if not handle then return end
  for _, ev in ipairs(retained) do
    handle:write(serialization.serialize(ev))
    handle:write("\n")
  end
  handle:close()
end

local function unsynced_events(state, limit)
  local out = {}
  for _, ev in ipairs(read_journal()) do
    if (ev.event_seq or 0) > (state.acked_to_central or 0) then
      out[#out + 1] = ev
      if #out >= limit then break end
    end
  end
  return out
end

local function send_register(modem, config, slave_id, machine_type, capabilities)
  protocol.send(modem, nil, config.port, {
    type = "register",
    slave_id = slave_id,
    label = config.label,
    group = config.group,
    machine_type = config.machine_type ~= "auto" and config.machine_type or machine_type,
    script_version = VERSION,
    capabilities = capabilities,
  })
end

local function send_heartbeat(modem, config, slave_id, current_state, state)
  local disk_free = nil
  local ok, value = pcall(filesystem.spaceFree, "/")
  if ok then disk_free = value end
  protocol.send(modem, nil, config.port, {
    type = "heartbeat",
    slave_id = slave_id,
    state = current_state,
    script_version = VERSION,
    disk_free = disk_free,
    backlog = math.max((state.event_seq or 0) - (state.acked_to_central or 0), 0),
  })
end

local function send_sync(modem, config, slave_id, state)
  local events = unsynced_events(state, config.max_sync_events)
  if #events == 0 then return end
  protocol.send(modem, nil, config.port, {
    type = "sync_batch",
    slave_id = slave_id,
    from_seq = events[1].event_seq,
    to_seq = events[#events].event_seq,
    events = events,
  })
end

local function parent_dir(path)
  return path:match("^(.*)/[^/]+$") or "/"
end

local function ensure_path_dir(path)
  local dir = parent_dir(path)
  if dir and dir ~= "" and not filesystem.exists(dir) then
    filesystem.makeDirectory(dir)
  end
end

local function install_update_file(path, data)
  local backup = PREVIOUS_ROOT .. path:gsub("[/%\\:]", "_")
  if filesystem.exists(path) then
    fsutil.write_all(backup, fsutil.read_all(path) or "")
  end
  ensure_path_dir(path)
  fsutil.write_all(path, data)
end

local function perform_update(modem, config, slave_id)
  protocol.send(modem, nil, config.port, {
    type = "update_manifest_request",
    slave_id = slave_id,
    current_version = VERSION,
  })

  local deadline = computer.uptime() + 20
  local manifest
  while computer.uptime() < deadline do
    local ev = {event.pull(2, "modem_message")}
    if ev[1] == "modem_message" and ev[4] == config.port then
      local msg = protocol.decode(ev[6])
      if msg and msg.type == "update_manifest" then
        manifest = msg
        break
      elseif msg and msg.type == "update_error" then
        return false, msg.error
      end
    end
  end
  if not manifest or type(manifest.files) ~= "table" then return false, "no update manifest received" end

  for file_path, meta in pairs(manifest.files) do
    local chunks = {}
    local index = 1
    while true do
      protocol.send(modem, nil, config.port, {
        type = "update_chunk_request",
        slave_id = slave_id,
        file_path = file_path,
        chunk_index = index,
      })
      local chunk_deadline = computer.uptime() + 20
      local got = false
      while computer.uptime() < chunk_deadline do
        local ev = {event.pull(2, "modem_message")}
        if ev[1] == "modem_message" and ev[4] == config.port then
          local msg = protocol.decode(ev[6])
          if msg and msg.type == "update_chunk" and msg.file_path == file_path and msg.chunk_index == index then
            chunks[#chunks + 1] = msg.chunk or ""
            got = true
            if msg.done then
              local data = table.concat(chunks)
              if meta.fnv1a32 and hash.fnv1a32(data) ~= meta.fnv1a32 then
                return false, "checksum mismatch for " .. file_path
              end
              install_update_file(meta.dest or file_path, data)
              index = nil
            else
              index = index + 1
            end
            break
          elseif msg and msg.type == "update_error" then
            return false, msg.error
          end
        end
      end
      if not got then return false, "timeout downloading " .. file_path end
      if not index then break end
    end
  end

  fsutil.write_all(ROOT .. "/pending_reboot", tostring(manifest.version or "unknown"))
  return true, manifest.version
end

fsutil.ensure_dir(ROOT)
fsutil.ensure_dir(STAGING_ROOT)
fsutil.ensure_dir(PREVIOUS_ROOT)

local config = ensure_config()
local modem = find_modem()
local slave_id = load_or_create_id()
local state = load_table(STATE_PATH, {event_seq = 0, acked_to_central = 0})
local machine_address, detected_type = choose_machine(config)
local machine = machine_address and component.proxy(machine_address) or nil
local capabilities = machine_address and list_methods(machine_address) or {}
local machine_context = {}
local last_state = nil
local last_sample_at = 0
local next_poll = 0
local next_heartbeat = 0
local next_sync = 0
local next_register = 0

modem.open(config.port)
print("GTNH bottleneck slave " .. VERSION)
print("slave_id=" .. slave_id .. " machine=" .. tostring(machine_address) .. " type=" .. tostring(detected_type))

append_event(state, {
  slave_id = slave_id,
  kind = "boot",
  state = "unknown",
  confidence = machine and "medium" or "low",
  notes = machine and "slave boot" or "slave boot without machine component",
  label = config.label,
  group = config.group,
  machine_type = config.machine_type ~= "auto" and config.machine_type or detected_type,
})

while true do
  local ev = {event.pull(0.2)}
  if ev[1] == "modem_message" and ev[4] == config.port then
    local msg = protocol.decode(ev[6])
    if msg and msg.type == "sync_ack" and msg.slave_id == slave_id then
      state.acked_to_central = math.max(state.acked_to_central or 0, msg.acked_to_seq or 0)
      save_table(STATE_PATH, state)
      compact_journal(state, config.compact_confirmed_keep_s)
    elseif msg and msg.type == "register_ack" and msg.slave_id == slave_id then
      state.last_register_ack = os.time()
      save_table(STATE_PATH, state)
    elseif msg and msg.type == "update_available" then
      append_event(state, {
        slave_id = slave_id,
        kind = "update",
        state = last_state or "unknown",
        confidence = "high",
        notes = "update available " .. tostring(msg.target_version),
      })
      local ok, result = perform_update(modem, config, slave_id)
      protocol.send(modem, nil, config.port, {
        type = "update_result",
        slave_id = slave_id,
        success = ok,
        installed_version = ok and result or VERSION,
        error = ok and nil or result,
      })
      if ok then
        computer.shutdown(true)
      end
    elseif msg and msg.type == "config_update" and msg.slave_id == slave_id then
      for key, value in pairs(msg.config or {}) do config[key] = value end
      save_table(CONFIG_PATH, config)
    end
  end

  local current = computer.uptime()
  if current >= next_register then
    send_register(modem, config, slave_id, detected_type, capabilities)
    next_register = current + 60
  end

  if current >= next_poll then
    local reading = machine and read_machine(machine, machine_context, config) or {
      state = "unknown",
      confidence = "low",
      notes = "no machine component",
    }
    local changed = reading.state ~= last_state
    local sample_due = current - last_sample_at >= config.sample_interval_s
    if changed or sample_due then
      append_event(state, {
        slave_id = slave_id,
        kind = changed and "transition" or "sample",
        state = reading.state,
        progress = reading.progress,
        max_progress = reading.max_progress,
        confidence = reading.confidence,
        notes = reading.notes,
        label = config.label,
        group = config.group,
        machine_type = config.machine_type ~= "auto" and config.machine_type or detected_type,
      })
      last_state = reading.state
      last_sample_at = current
      next_sync = 0
    end
    next_poll = current + config.poll_interval_s
  end

  if current >= next_heartbeat then
    send_heartbeat(modem, config, slave_id, last_state or "unknown", state)
    next_heartbeat = current + config.heartbeat_interval_s
  end

  if current >= next_sync then
    send_sync(modem, config, slave_id, state)
    next_sync = current + config.sync_interval_s
  end
end
