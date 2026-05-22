local component = require("component")
local filesystem = require("filesystem")
local shell = require("shell")

local BASE = "https://raw.githubusercontent.com/KingoRodriguo/OC-FactoryManager/main"

local files = {
  {BASE .. "/oc/lib/bn/fsutil.lua", "/usr/lib/bn/fsutil.lua"},
  {BASE .. "/oc/lib/bn/hash.lua", "/usr/lib/bn/hash.lua"},
  {BASE .. "/oc/lib/bn/json.lua", "/usr/lib/bn/json.lua"},
  {BASE .. "/oc/lib/bn/protocol.lua", "/usr/lib/bn/protocol.lua"},
  {BASE .. "/oc/central.lua", "/usr/bin/bn-central.lua"},
}

local default_config = [[{
  port = 4242,
  upload_url = "https://CHANGE-ME.ngrok-free.app/ingest",
  upload_interval_s = 60,
  max_batch_events = 500,
  max_batch_bytes = 12000,
  buffer_capacity_events = 2500,
  force_upload_buffer_pct = 80,
  retry_backoff_s = 30,
  max_retry_backoff_s = 600,
  update_manifest_url = "",
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

print("Installing OC-FactoryManager central...")
for _, item in ipairs(files) do
  print(item[2])
  write(item[2], download(item[1]))
end

ensure_dir("/etc")
if not filesystem.exists("/etc/bn-central.cfg") then
  write("/etc/bn-central.cfg", default_config)
  print("Created /etc/bn-central.cfg")
else
  print("Kept existing /etc/bn-central.cfg")
end

print("Central installed.")
print("Edit /etc/bn-central.cfg and set upload_url, then run:")
print("bn-central.lua")
