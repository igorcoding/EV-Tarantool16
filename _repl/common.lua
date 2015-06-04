tnt_pool = {
	'localhost:3302',
	'localhost:3303',
}
pool = nil
leader = nil
status_channels = setmetatable({}, {__mode='kv'})
LAG_THRESHOLD = 5


local fiber = require('fiber')
local nbox = require('net.box')

function get_uuid()
	return box.info.server.uuid
end

function get_id()
	return box.info.server.id
end

local function ping_all()
	local ok = true
	for i=1,#tnt_pool do
		local s = tnt_pool[i]
		local host, port = s:match("([^:]+):([^:]+)")
		local conn = nbox:new(host, tonumber(port))
		if not conn or not conn:ping() then
			ok = false
			break
		end
		conn:close()
	end
	return ok
end

function status_wait(timeout)
	timeout = tonumber(timeout) or 1
	local ch = fiber.channel(1)
	status_channels[ch] = ch
	local m = ch:get(timeout)
	status_channels[ch] = nil

	local repl_status = box.info.replication.status
	local lag = box.info.replication.lag
	local status = 'good'
	if repl_status == 'disconnected' then
		status = 'bad'
	else
		if lag > LAG_THRESHOLD or not ping_all() then
			status = 'degraded'
		end
	end
	local res = {}
	res['status'] = status
	res['repl_status'] = repl_status
	res['lag'] = lag
	return res
end

function init()
	local pool = {}
	local conn_pool = {}
	local leader = get_uuid()
	for i=1,#tnt_pool do
		local s = _G.tnt_pool[i]
		local host, port = s:match("([^:]+):([^:]+)")
		local conn = nbox:new(host, tonumber(port))
		if conn and conn:ping() then
			local uuid = conn:call('get_uuid')[1][1]
			pool[uuid] = s
			if uuid < leader then
				leader = uuid
			end
		end
		conn:close()
	end

	_G.pool = pool
	_G.leader = leader
end

function get_leader()
	if leader == nil then
		init()
	end
	return {leader, pool[leader]}
end

function get_pool()
	if pool == nil then
		init()
	end
	return pool
end

function on_connected()
	local log = require('log')
	local m = 'Connection. user=' .. box.session.user() .. ' id=' .. box.session.id()
	log.info(m)
end

function on_disconnected()
	local log = require('log')
	local m = 'Disconnection. user=' .. box.session.user() .. ' id=' .. box.session.id()
	log.info(m)

	if box.session.user() == replicator_user then
		leader = nil
		pool = nil
		for k, v in pairs(status_channels) do
			v:put(true)
		end
	end
end
box.session.on_connect(on_connected)
box.session.on_disconnect(on_disconnected)


-- function poll()
-- 	local net_box = require('net.box')
-- 	local log = require('log')

-- 	conn = net_box:new('localhost', 3303)

-- 	json = require('json')
-- 	log.info(json.encode(p))
-- 	res = {}
-- 	res[3303] = conn:call('get_uid')[1][1]
-- 	res[3302] = get_uid()
-- 	return res
-- end
