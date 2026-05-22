local component = require("component")
local filesystem = require("filesystem")

local BASE = "https://raw.githubusercontent.com/KingoRodriguo/OC-FactoryManager/main"

local files = {
  {BASE .. "/oc/lib/bn/fsutil.lua", "/usr/lib/bn/fsutil.lua"},
  {BASE .. "/oc/lib/bn/hash.lua", "/usr/lib/bn/hash.lua"},
  {BASE .. "/oc/lib/bn/json.lua", "/usr/lib/bn/json.lua"},
  {BASE .. "/oc/lib/bn/protocol.lua", "/usr/lib/bn/protocol.lua"},
  {BASE .. "/oc/slave.lua", "/usr/bin/bn-slave.lua"},
}

local default_config = [[{
  port = 4242,
  label = "CHANGE-ME",
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
}]]

local function ensure_dir(path)
  if not filesystem.exists(path) then
    filesystem.makeDirectory(path)
  end
end

local function parent(path)
  return path:match("^(.*)/[^/]+$") or "/"
end

local function download(url)
  if not component.isAvailable("internet") then
    error("Internet Card required")
  end
  local handle, err = component.internet.request(url)
  if not handle then error("download failed: " .. tostring(err)) end
  local chunks = {}
  while true do
    local chunk = handle.read()
    if not chunk then break end
    chunks[#chunks + 1] = chunk
  end
  return table.concat(chunks)
end

local function write(path, data)
  ensure_dir(parent(path))
  local handle, err = io.open(path, "w")
  if not handle then error("cannot write " .. path .. ": " .. tostring(err)) end
  handle:write(data)
  handle:close()
end

print("Installing OC-FactoryManager slave...")
for _, item in ipairs(files) do
  print(item[2])
  write(item[2], download(item[1]))
end

ensure_dir("/etc")
if not filesystem.exists("/etc/bn-slave.cfg") then
  write("/etc/bn-slave.cfg", default_config)
  print("Created /etc/bn-slave.cfg")
else
  print("Kept existing /etc/bn-slave.cfg")
end

print("Slave installed.")
print("Edit /etc/bn-slave.cfg label/group if needed, then run:")
print("bn-slave.lua")
