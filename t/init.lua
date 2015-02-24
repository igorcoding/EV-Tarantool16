box.cfg{
	listen = 3301,
	log_level = 5,
	work_dir = '/home/vagrant/tnt',

}

s = box.schema.space.create('tester', {id = 1})
box.schema.user.grant('guest','read,write,execute','universe')
i = s:create_index('primary', {type = 'hash', parts = {1, 'STR', 2, 'STR', 3, 'NUM'}})


box.space[1]:insert{"test1","testx",123, -745, 8887};
box.space[1]:insert{"test2","testx",456};
