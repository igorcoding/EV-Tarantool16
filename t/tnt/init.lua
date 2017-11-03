box.cfg{
	listen = "127.0.0.1:3301",
	work_dir = ".",
--	wal_mode = "none"
}

box.schema.user.grant('guest','read,write,execute','universe')

require('console').listen('127.0.0.1:3302')

local function script_path() local fio = require('fio');local b = debug.getinfo(2, "S").source:sub(2);local lb = fio.readlink(b);if lb ~= nil then b = lb end;return b:match("(.*/)") end
dofile(script_path() .. '/app.lua')
require('console').start()

