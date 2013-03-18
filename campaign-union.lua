--[[
Create the union of two campaigns, storing the resulting campaign at {dest_campaign}.
--]]

-- Get parameters.
local campaign_a = KEYS[1]      -- First campaign name
local campaign_b = KEYS[2]      -- Second campaign name
local campaign_dest = KEYS[3]   -- Destination campaign name for storing intersected campaign data
local events = ARGV             -- (optional) List of events to intersect on

-- Convert Redis-friendly list to Lua-friendly table.
local function to_lua_table(list)
    local table = {}

    for i = 1, #list, 2 do
        table[list[i]] = list[i + 1]
    end

    return table
end

-- Convert Lua-friendly table to Redis-friendly list.
local function to_redis_list(table)
    local list = {}

    for k, v in pairs(table) do
        list[#list + 1] = k
        list[#list + 1] = v
    end

    return list
end

-- Load campaign sources.
local sources_a = to_lua_table(redis.call('HGETALL', campaign_a .. ':sources'))
local sources_b = to_lua_table(redis.call('HGETALL', campaign_b .. ':sources'))
local sources_dest = {}

-- Union sources_a and sources_b.
local counter = 0
for source, offset in pairs(sources_a) do
    if sources_dest[source] == nil then
        counter = counter + 1
        sources_dest[source] = counter
    end
end
for source, offset in pairs(sources_b) do
    if sources_dest[source] == nil then
        counter = counter + 1
        sources_dest[source] = counter
    end
end

-- Multi-set sources on campaign_dest.
redis.call('HMSET', campaign_dest .. ':sources', unpack(to_redis_list(sources_dest)))
redis.call('SET', campaign_dest .. ':sources:counter', counter)


-- Load campaign events.
local events_a = redis.call('SMEMBERS', campaign_a .. ':events')
local events_b = redis.call('SMEMBERS', campaign_b .. ':events')
local events_dest = {}

-- Union events_a and events_b.
local counter = 0
local events_index = {}
for index, event in pairs(events_a) do
    if events_index[event] == nil then
        events_index[event] = true
        events_dest[counter] = event
    end
end
for index, event in pairs(events_b) do
    if events_index[event] == nil then
        events_index[event] = true
        events_dest[counter] = event
    end
end
events_index = nil

-- Multi-set events on campaign_dest.
redis.call('SADD', campaign_dest .. ':events', unpack(to_redis_list(events_dest))) -- TODO: transform the data to make this work
redis.call('SET', campaign_dest .. ':events:counter', counter)


-- TODO: Union bitmaps.





return to_redis_list(events_dest)
