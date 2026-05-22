local json = {}

local function is_array(value)
  local count = 0
  local max = 0
  for key, _ in pairs(value) do
    if type(key) ~= "number" or key < 1 or key % 1 ~= 0 then
      return false
    end
    count = count + 1
    if key > max then max = key end
  end
  return max == count
end

local escapes = {
  ['"'] = '\\"',
  ["\\"] = "\\\\",
  ["\b"] = "\\b",
  ["\f"] = "\\f",
  ["\n"] = "\\n",
  ["\r"] = "\\r",
  ["\t"] = "\\t",
}

local function encode_string(value)
  return '"' .. tostring(value):gsub('[%z\1-\31"\\]', function(c)
    return escapes[c] or string.format("\\u%04x", c:byte())
  end) .. '"'
end

local function encode_value(value)
  local value_type = type(value)
  if value_type == "nil" then
    return "null"
  elseif value_type == "boolean" then
    return value and "true" or "false"
  elseif value_type == "number" then
    if value ~= value or value == math.huge or value == -math.huge then
      return "null"
    end
    return tostring(value)
  elseif value_type == "string" then
    return encode_string(value)
  elseif value_type == "table" then
    local out = {}
    if is_array(value) then
      for i = 1, #value do
        out[#out + 1] = encode_value(value[i])
      end
      return "[" .. table.concat(out, ",") .. "]"
    end

    for key, child in pairs(value) do
      if type(key) == "string" then
        out[#out + 1] = encode_string(key) .. ":" .. encode_value(child)
      end
    end
    return "{" .. table.concat(out, ",") .. "}"
  end

  return "null"
end

function json.encode(value)
  return encode_value(value)
end

return json
