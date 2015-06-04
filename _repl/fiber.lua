box.cfg{listen=3305}
box.schema.user.grant('guest','read,write,execute','universe')

fiber = require('fiber')
ch = fiber.channel(1000)

function consumer()
	while true do
		local msg = ch:get(5)
		if msg ~= nil then
			print('consumed \'' .. msg .. '\'')
		else
			print('timeout')
		end
	end
end

function producer()
	local j = 1
	while j <= 1000 do
		local msg = 'msg #' .. j
		print('produced \'' .. msg .. '\'')
		ch:put(msg)
		j = j + 1
	end
end


fiber.create(consumer)
fiber.create(producer)
