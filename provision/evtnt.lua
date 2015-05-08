box.cfg{
	listen = 3301,
	log_level = 5,
	-- logger = 'tarantool.log'
}
box.schema.user.grant('guest','read,write,execute,create,drop','universe')

local home = os.getenv("HOME")
dofile(home .. '/EV-Tarantool1.6/provision/init.lua')
