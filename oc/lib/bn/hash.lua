local hash = {}

function hash.fnv1a32(data)
  local h = 2166136261
  for i = 1, #data do
    h = bit32.bxor(h, data:byte(i))
    h = (h * 16777619) % 4294967296
  end
  return string.format("%08x", h)
end

return hash
