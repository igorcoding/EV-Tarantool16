-- box.cfg{
-- 	listen = 3301,
-- 	log_level = 5
-- 	-- logger = 'tarantool.log'
-- }
box.cfg{
	custom_proc_title = "Tarantool_tester",
	slab_alloc_arena = 0.1,
	listen = "127.0.0.1:3301",

	work_dir = ".",
	wal_mode = "none",
	log_level = 5
}
box.schema.user.grant('guest','read,write,execute,create,drop','universe')
require('console').listen('127.0.0.1:33010')

-- local home = os.getenv("HOME")
-- dofile(home .. '/EV-Tarantool1.6/provision/init.lua')

function script_path()
   local str = debug.getinfo(2, "S").source:sub(2)
   return str:match("(.*/)")
end
BASE = script_path()

dofile(BASE .. 'init.lua')
