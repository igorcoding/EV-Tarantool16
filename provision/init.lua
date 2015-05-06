box.cfg{
	listen = 3301,
	log_level = 5,
	-- logger = 'tarantool.log'
}
box.schema.user.grant('guest','read,write,execute','universe')

s_tester = box.space.tester
if s_tester then
	s_tester:drop{}
end

s_memier = box.space.memier
if s_memier then
	s_memier:drop{}
end

s_sophier = box.space.sophier
if s_sophier then
	s_sophier:drop{}
end

function init_tester(s_tester)
	_format = {}
	_format[1] = {type='str', name='_t1'}
	_format[2] = {type='str', name='_t2'}
	_format[3] = {type='num', name='_t3'}
	_format[4] = {type='num', name='_t4'}
	s_tester:format(_format)

	i = s_tester:create_index('primary', {type = 'tree', parts = {1, 'STR', 2, 'STR', 3, 'NUM'}})
	-- i2 = s_tester:create_index('spatial', {type = 'RTREE', unique = false, parts = {7, 'ARRAY'}})


	-- arr = {1, 2, 3, "str1", 4}
	-- obj = {}
	-- obj['key1'] = "value1"
	-- obj['key2'] = 42
	-- obj[33] = true
	-- obj[35] = false

	-- s_tester:insert{"t1", "t2", 1, -745, "heyo"};
	-- s_tester:insert{"t1", "t2", 2, arr};
	-- s_tester:insert{"t1", "t2", 3, obj};
	-- s_tester:insert{"tt1", "tt2", 456};
end

function init_sophier(s_sophier)
	i = s_sophier:create_index('primary', {type = 'tree', parts = {1, 'STR'}})
end

function string_function()
  return "hello world"
end


s_tester = box.schema.space.create('tester')
init_tester(s_tester)

s_memier = box.schema.space.create('memier', {engine = 'memtx'})
init_sophier(s_memier)

s_sophier = box.schema.space.create('sophier', {engine = 'sophia'})
init_sophier(s_sophier)
