box.cfg{
	listen = 3301,
	log_level = 5,
	work_dir = '/home/vagrant/tnt',

}

s = box.schema.space.create('tester', {id = 1})
box.schema.user.grant('guest','read,write,execute','universe')
i = s:create_index('primary', {type = 'tree', parts = {1, 'STR', 2, 'STR', 3, 'NUM'}})


arr = {1, 2, 3, "str1", 4}
obj = {}
obj['key1'] = "value1"
obj['key2'] = 42
obj[33] = true
obj[35] = false


box.space[1]:insert{"t1", "t2", 1, -745, 8887};
box.space[1]:insert{"t1", "t2", 2, arr};
box.space[1]:insert{"t1", "t2", 3, obj};
box.space[1]:insert{"tt1", "tt2", 456};
