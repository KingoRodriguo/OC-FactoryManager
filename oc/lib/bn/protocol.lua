local serialization = require("serialization")

local protocol = {}
protocol.VERSION = 1

function protocol.encode(message)
  message.protocol_version = message.protocol_version or protocol.VERSION
  return serialization.serialize(message)
end

function protocol.decode(payload)
  if type(payload) ~= "string" then
    return nil, "payload is not a string"
  end
  local ok, message = pcall(serialization.unserialize, payload)
  if not ok or type(message) ~= "table" then
    return nil, "invalid serialized message"
  end
  return message
end

function protocol.send(modem, address, port, message)
  local payload = protocol.encode(message)
  if address then
    return modem.send(address, port, payload)
  end
  return modem.broadcast(port, payload)
end

return protocol
