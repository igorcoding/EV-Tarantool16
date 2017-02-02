local log = require('log')
local yaml = require('yaml')

local username = 'test_user'
local password = 'test_pass'

if #box.space._user.index.name:select({username}) ~= 0 then
	box.schema.user.drop(username)
end
box.schema.user.create('test_user', {password=password, if_not_exists=true})
box.schema.user.grant('test_user','read,write,execute,create,drop','universe')

local function bootstrap()
	local b = {
		tarantool_ver = box.info.version,
		has_new_types = false,
		types = {}
	}
	
	if b.tarantool_ver >= "1.7.1-245" then
		b.has_new_types = true
		b.types.string = 'string'
		b.types.unsigned = 'unsigned'
		b.types.integer = 'integer'
	else
		b.types.string = 'str'
		b.types.unsigned = 'num'
		b.types.integer = 'int'
	end
	b.types.number = 'number'
	b.types.array = 'array'
	b.types.scalar = 'scalar'
	b.types.any = '*'
	return b
end

local B = bootstrap()
log.info(yaml.encode(B))

s_tester = box.space.tester
if s_tester then
	s_tester:drop{}
end

s_rtree = box.space.rtree
if s_rtree then
	s_rtree:drop{}
end

s_memier = box.space.memier
if s_memier then
	s_memier:drop{}
end

s_vinyl = box.space.vinyler
if s_vinyl then
	s_vinyl:drop{}
end


-------------------------------------------------------------------------------

local function init_tester(s)
	_format = {
		{type=B.types.string, name='_t1'},
		{type=B.types.string, name='_t2'},
		{type=B.types.unsigned, name='_t3'},
		{type=B.types.unsigned, name='_t4'},
		{type=B.types.any, name='_t5'},
	}
	s:format(_format)

	i = s:create_index('primary', {type = 'tree', parts = {1, B.types.string, 2, B.types.string, 3, B.types.unsigned}})
	-- box.space.tester:insert({'s','a',3,4})
end

function fill_tester()
	arr = {1, 2, 3, "str1", 4}
	obj = {}
	obj['key1'] = "value1"
	obj['key2'] = 42
	obj[33] = true
	obj[35] = false

	box.space.tester:insert{"t1", "t2", 17, -745, "heyo"};
	box.space.tester:insert{"t1", "t2", 2, arr};
	box.space.tester:insert{"t1", "t2", 3, obj};
	box.space.tester:insert{"tt1", "tt2", 456, 5};
end

function truncate_tester()
	s = box.space.tester
	t = {}
	i = 1
	for k, v in s:pairs() do
		t[i] = {v[1], v[2], v[3]}
		i = i + 1
	end

	for i, v in pairs(t) do
		s:delete(v)
	end
end

-------------------------------------------------------------------------------

local function init_vinyler(s)
	i = s:create_index('primary', {type = 'tree', parts = {1, B.types.string}})
end

local function _truncate_vinyler(s)
	t = {}
	i = 1
	for k, v in s:pairs() do
		t[i] = {v[1]}
		i = i + 1
	end

	for i, v in pairs(t) do
		s:delete(v)
	end
end

function truncate_vinyler()
	s = box.space.vinyler
	_truncate_vinyler(s)
end

function truncate_memier()
	s = box.space.memier
	_truncate_vinyler(s)
end


-------------------------------------------------------------------------------


local function init_rtree(s)
	i = s:create_index('primary', {type = 'TREE', parts = {1, B.types.string}})
	i2 = s:create_index('spatial', {type = 'RTREE', unique = false, parts = {2, B.types.array}})
end

function fill_rtree()

end

function truncate_rtree()
	s = box.space.rtree
	t = {}
	i = 1
	for k, v in s:pairs() do
		t[i] = {v[1]}
		i = i + 1
	end

	for i, v in pairs(t) do
		s:delete(v)
	end
end


-------------------------------------------------------------------------------


s_tester = box.schema.space.create('tester')
init_tester(s_tester)

s_memier = box.schema.space.create('memier', {engine = 'memtx'})
init_vinyler(s_memier)

s_vinyl = box.schema.space.create('vinyler', {engine = 'vinyl'})
init_vinyler(s_vinyl)

s_rtree = box.schema.space.create('rtree')
init_rtree(s_rtree)


function string_function()
  return "hello world"
end

function timeout_test(timeout)
	local fiber = require('fiber')
	local ch = fiber.channel(1)
	ch:get(timeout)
	return 'ok'
end

function truncate_all()
	print "Truncating tester space"
	truncate_tester()

	print "Truncating vinyler space"
	truncate_vinyler()

	print "Truncating memier space"
	truncate_memier()

	print "Truncating rtree space"
	truncate_rtree()
end


function get_test_tuple()
	local t = box.space.tester:select{}[1]
	return t
end



function dummy(arg)
	return 'ok'
end
