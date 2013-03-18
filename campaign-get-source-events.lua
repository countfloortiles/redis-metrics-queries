--[[
Get a set of all events tracked in a given campaign for a given source.
--]]

-- Get parameters.
local campaign = KEYS[1]    -- Campaign name
local source = KEYS[2]      -- Source name within the campaign

local results = {}

local offset = redis.call('HGET', campaign..":sources", source)
if offset then
    local events = redis.call('SMEMBERS', campaign..":events")
    for index, event in pairs(events) do
        local bit = redis.call('GETBIT', campaign..":events:"..event, offset)

        if bit == 1 then
          table.insert(results, event)
        end
    end
end

return results
