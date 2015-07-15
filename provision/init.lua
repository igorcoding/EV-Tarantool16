local username = 'test_user'
local password = 'test_pass'

if #box.space._user.index.name:select({username}) ~= 0 then
	box.schema.user.drop(username)
end
box.schema.user.create('test_user', {password=password, if_not_exists=true})
box.schema.user.grant('test_user','read,write,execute,create,drop','universe')

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

s_sophier = box.space.sophier
if s_sophier then
	s_sophier:drop{}
end


-------------------------------------------------------------------------------

local function init_tester(s)
	_format = {}
	_format[1] = {type='str', name='_t1'}
	_format[2] = {type='str', name='_t2'}
	_format[3] = {type='num', name='_t3'}
	_format[4] = {type='num', name='_t4'}
	s:format(_format)

	i = s:create_index('primary', {type = 'tree', parts = {1, 'STR', 2, 'STR', 3, 'NUM'}})
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

local function init_sophier(s)
	i = s:create_index('primary', {type = 'tree', parts = {1, 'STR'}})
end

local function _truncate_sophier(s)
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

function truncate_sophier()
	s = box.space.sophier
	_truncate_sophier(s)
end

function truncate_memier()
	s = box.space.memier
	_truncate_sophier(s)
end


-------------------------------------------------------------------------------


local function init_rtree(s)
	i = s:create_index('primary', {type = 'TREE', parts = {1, 'STR'}})
	i2 = s:create_index('spatial', {type = 'RTREE', unique = false, parts = {2, 'ARRAY'}})
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
init_sophier(s_memier)

s_sophier = box.schema.space.create('sophier', {engine = 'sophia'})
init_sophier(s_sophier)

s_rtree = box.schema.space.create('rtree')
init_rtree(s_rtree)


function string_function()
  return "hello world"
end

function truncate_all()
	print "Truncating tester space"
	truncate_tester()

	print "Truncating sophier space"
	truncate_sophier()

	print "Truncating memier space"
	truncate_memier()

	print "Truncating rtree space"
	truncate_rtree()
end


function get_test_tuple()
	local t = box.space.tester:select{}[1]
	return t
end
