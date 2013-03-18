--[[
Track an analytics event triggered by a given source within a given campaign.

Manages keys for each of the following.
    hash map    {campaign}:sources
    integer     {campaign}:sources:counter
    set         {campaign}:events
    integer     {campaign}:events:counter

    bitmap      {campaign}:events:{event 0}
    bitmap      {campaign}:events:{event 1}
    ...         ...
    bitmap      {campaign}:events:{event N}
--]]

-- Get parameters.
local campaign = KEYS[1]    -- Campaign name
local event = KEYS[2]       -- Event name within the campaign
local source = KEYS[3]      -- Source name within the campaign

-- Get bitmap offset for the source name specified.  If no offset then add the source
-- to the {campaign}:sources map and assign an offset.
local offset = redis.call('HGET', campaign..":sources", source)
if not offset then
    offset = redis.call('INCR', campaign..":sources:counter")
    redis.call('HSET', campaign..":sources", source, offset)
end

-- Keep track of which events are being tracked in the campaign.
local eventExists = redis.call('SISMEMBER', campaign..":events", event)
if eventExists == 0 then
    redis.call('SADD', campaign..":events", event)
    redis.call('INCR', campaign..":events:counter")
end

-- Register event in the event bitmap at location corresponding to the offset
-- assigned to the source.
redis.call('SETBIT', campaign..":events:"..event, offset, 1)

return true
