module(..., package.seeall)

package.path = package.path .. ";../deps/pflua/src/?.lua"

local pflua = require("pf")

local filter = subClass(nil)
filter._name = "pflua packet filter"

function filter:new(program, opts)
   -- opts = opts or { bpf = true }
   opts = { }
   local o = filter:superClass().new(self)
   o._filter = pflua.compile_filter(program, opts)
   return o
end

function filter:match(data, length)
   return self._filter(data, length)
end

return filter
