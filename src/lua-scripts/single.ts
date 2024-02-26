export const fixedWindowScript = `
local key           = KEYS[1]
local window        = ARGV[1]
local incrementBy   = ARGV[2] -- increment rate per request at a given value, default is 1

local r = redis.call("INCRBY", key, incrementBy)
if r == incrementBy then
-- The first time this key is set, the value will be equal to incrementBy.
-- So we only need the expire command once
redis.call("PEXPIRE", key, window)
end

return r`;

export const slidingWindowScript = `
  local currentKey  = KEYS[1]           -- identifier including prefixes
  local previousKey = KEYS[2]           -- key of the previous bucket
  local tokens      = tonumber(ARGV[1]) -- tokens per window
  local now         = ARGV[2]           -- current timestamp in milliseconds
  local window      = ARGV[3]           -- interval in milliseconds
  local incrementBy = ARGV[4]           -- increment rate per request at a given value, default is 1

  local requestsInCurrentWindow = redis.call("GET", currentKey)
  if requestsInCurrentWindow == false then
    requestsInCurrentWindow = 0
  end

  local requestsInPreviousWindow = redis.call("GET", previousKey)
  if requestsInPreviousWindow == false then
    requestsInPreviousWindow = 0
  end
  local percentageInCurrent = ( now % window ) / window
  -- weighted requests to consider from the previous window
  requestsInPreviousWindow = math.floor(( incrementBy - percentageInCurrent ) * requestsInPreviousWindow)
  if requestsInPreviousWindow + requestsInCurrentWindow >= tokens then
    return -1
  end

  local newValue = redis.call("INCRBY", currentKey, incrementBy)
  if newValue == incrementBy then
    -- The first time this key is set, the value will be equal to incrementBy.
    -- So we only need the expire command once
    redis.call("PEXPIRE", currentKey, window * 2 + 1000) -- Enough time to overlap with a new window + 1 second
  end
  return tokens - ( newValue + requestsInPreviousWindow )
  `;

export const tokenBucketScript = `
        local key         = KEYS[1]           -- identifier including prefixes
        local maxTokens   = tonumber(ARGV[1]) -- maximum number of tokens
        local interval    = tonumber(ARGV[2]) -- size of the window in milliseconds
        local refillRate  = tonumber(ARGV[3]) -- how many tokens are refilled after each interval
        local now         = tonumber(ARGV[4]) -- current timestamp in milliseconds
        local incrementBy = tonumber(ARGV[5]) -- how many tokens to consume, default is 1
        
        local bucket = redis.call("HMGET", key, "refilledAt", "tokens")
        
        local refilledAt
        local tokens

        if bucket[1] == false then
          refilledAt = now
          tokens = maxTokens
        else
          refilledAt = tonumber(bucket[1])
          tokens = tonumber(bucket[2])
        end
        
        if now >= refilledAt + interval then
          local numRefills = math.floor((now - refilledAt) / interval)
          tokens = math.min(maxTokens, tokens + numRefills * refillRate)

          refilledAt = refilledAt + numRefills * interval
        end

        if tokens == 0 then
          return {-1, refilledAt + interval}
        end

        local remaining = tokens - incrementBy
        local expireAt = math.ceil(((maxTokens - remaining) / refillRate)) * interval
        
        redis.call("HSET", key, "refilledAt", refilledAt, "tokens", remaining)
        redis.call("PEXPIRE", key, expireAt)
        return {remaining, refilledAt + interval}
       `;
