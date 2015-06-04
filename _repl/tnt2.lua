function script_path()
   local str = debug.getinfo(2, "S").source:sub(2)
   return str:match("(.*/)")
end
BASE = script_path()
package.path = package.path .. ";".. BASE .."?.lua"

box.cfg {
	listen=3303,
	-- replication_source = 'replicator:pwd@localhost:3304',
	replication_source = 'replicator:pwd@localhost:3302',
}

replicator_user = 'replicator'

-- if #box.space._user.index.name:select({replicator_user}) == 0 then
-- 	print('yes...')
-- 	box.schema.user.create(replicator_user, {password = 'pwd'})
-- 	box.schema.user.grant(replicator_user,'read,write,execute','universe')
-- 	box.schema.user.grant('guest','read,write,execute','universe')
-- end

-- box.cfg{
-- 	replication_source = replicator_user .. ':pwd@localhost:3302'
-- }

require('strict')
require('common')
