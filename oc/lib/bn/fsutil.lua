local filesystem = require("filesystem")

local fsutil = {}

function fsutil.ensure_dir(path)
  if not filesystem.exists(path) then
    filesystem.makeDirectory(path)
  end
end

function fsutil.read_all(path)
  local handle = io.open(path, "r")
  if not handle then return nil end
  local data = handle:read("*a")
  handle:close()
  return data
end

function fsutil.write_all(path, data)
  local handle, err = io.open(path, "w")
  if not handle then return nil, err end
  handle:write(data or "")
  handle:close()
  return true
end

function fsutil.append_line(path, line)
  local handle, err = io.open(path, "a")
  if not handle then return nil, err end
  handle:write(line)
  handle:write("\n")
  handle:close()
  return true
end

function fsutil.read_lines(path)
  local lines = {}
  local handle = io.open(path, "r")
  if not handle then return lines end
  for line in handle:lines() do
    if line and line ~= "" then
      lines[#lines + 1] = line
    end
  end
  handle:close()
  return lines
end

return fsutil
