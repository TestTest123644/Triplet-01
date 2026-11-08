-- Map Docker/fluentd records into guardian-logs fields for Grafana.
function enrichevent(tag, timestamp, record)
  local msg = record["message"] or record["log"] or ""
  if type(msg) ~= "string" then
    msg = tostring(msg)
  end

  local upper = string.upper(msg)
  local level = "INFO"
  if string.find(upper, "CRITICAL", 1, true) or string.find(upper, "FATAL", 1, true) then
    level = "CRITICAL"
  elseif string.find(upper, "ERROR", 1, true) then
    level = "ERROR"
  elseif string.find(upper, "WARN", 1, true) then
    level = "WARNING"
  end

  local logger = record["container_name"] or record["container_id"] or tag
  if type(logger) == "string" then
    logger = logger:gsub("^/", "")
  end

  local sec = timestamp
  if type(timestamp) == "table" then
    sec = timestamp.sec or timestamp[0] or timestamp[1]
  end
  local iso
  if type(sec) == "number" then
    iso = os.date("!%Y-%m-%dT%H:%M:%SZ", math.floor(sec))
  else
    iso = os.date("!%Y-%m-%dT%H:%M:%SZ")
  end

  -- Replace the record with only the dashboard fields.
  local out = {
    ["@timestamp"] = iso,
    level = level,
    logger = logger,
    message = msg,
  }
  return 1, timestamp, out
end
