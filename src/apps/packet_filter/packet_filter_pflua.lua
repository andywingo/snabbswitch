module(...,package.seeall)

package.path = package.path .. ";../deps/pflua/src/?.lua"

local ffi = require("ffi")
local C = ffi.C
local bit = require("bit")

local app = require("core.app")
local link = require("core.link")
local lib = require("core.lib")
local packet = require("core.packet")
local buffer = require("core.buffer")
local config = require("core.config")

local pcap = require("apps.pcap.pcap")
local basic_apps = require("apps.basic.basic_apps")

local pflua = require("pf")

local verbose = false

assert(ffi.abi("le"), "support only little endian architecture at the moment")
assert(ffi.abi("64bit"), "support only 64 bit architecture at the moment")

local function compile_filters (filters)
   local result = {}
   for i, each in ipairs(filters) do
      result[i] = pflua.compile_filter(each)
   end
   return result
end

PacketFilter = {}

function PacketFilter:new (filters)
   assert(filters)
   assert(#filters > 0)

   local compiled_filters = compile_filters(filters)

   local function conform (buffer, length)
      for _, func in ipairs(compiled_filters) do
         if func(buffer, length) then
            return true
         end
      end
      return false
   end
   return setmetatable({ conform = conform }, { __index = PacketFilter })
end

function PacketFilter:push ()
   local i = assert(self.input.input or self.input.rx, "input port not found")
   local o = assert(self.output.output or self.output.tx, "output port not found")

   local packets_tx = 0
   local max_packets_to_send = link.nwritable(o)
   if max_packets_to_send == 0 then
      return
   end

   local nreadable = link.nreadable(i)
   for n = 1, nreadable do
      local p = link.receive(i)

      local buffer = p.iovecs[0].buffer.pointer + p.iovecs[0].offset
      local length = p.iovecs[0].length

      if self.conform(buffer, length) then
         link.transmit(o, p)
      else
         packet.deref(p)
      end
   end
end
