-- Use of this source code is governed by the Apache 2.0 license; see COPYING.

module(...,package.seeall)

local ffi     = require("ffi")
local S       = require("syscall")
local lib     = require("core.lib")
local file    = require("lib.stream.file")
local fiber   = require("lib.fibers.fiber")
local file_op = require("lib.fibers.file")
local op      = require("lib.fibers.op")
local channel = require("lib.fibers.channel")
local cond    = require("lib.fibers.cond")

-- Fixed-size part of an inotify event.
local inotify_event_header_t = ffi.typeof[[
struct {
   int      wd;       /* Watch descriptor */
   uint32_t mask;     /* Mask describing event */
   uint32_t cookie;   /* Unique cookie associating related
                         events (for rename(2)) */
   uint32_t len;      /* Size of name field */
   // char  name[];   /* Optional null-terminated name */
}]]

local function event_has_flags(event, flags)
   return bit.band(event.mask, S.c.IN[flags]) ~= 0
end

local function warn(msg, ...)
   io.stderr:write(string.format(msg.."\n", ...))
end

local function open_inotify_stream(name, events)
   local fd = assert(S.inotify_init("cloexec, nonblock"))
   assert(fd:inotify_add_watch(name, events))
   return file.fdopen(fd, "rdonly")
end

-- Return a channel on which to receive inotify events, and a cond
-- which, when signalled, will shut down the channel.
function inotify_event_channel(file_name, events, cancel_op)
   local ok, stream = pcall(open_inotify_stream, file_name, events)
   local ch, cancel = channel.new(), cond.new()
   if not ok then
      warn('warning: failed to open inotify on %s: %s',
           file_name, tostring(stream))
      fiber.spawn(function () ch:put(nil) end)
      return ch, cancel
   end
   cancel_op = op.choice(cancel:wait_operation(), cancel_op)
   cancel_op = cancel_op:wrap(function () return 'cancelled' end)
   local select_op = op.choice(file_op.stream_readable_op(stream),
                               cancel_op)
   fiber.spawn(function ()
      while select_op:perform() ~= 'cancelled' do
         local ev = stream:read_struct(nil, inotify_event_header_t)
         local name
         if ev.len ~= 0 then
            local buf = ffi.new('uint8_t[?]', ev.len)
            stream:read_bytes_or_error(buf, ev.len)
            name = ffi.string(buf)
         end
         ch:put({wd=ev.wd, mask=ev.mask, cookie=ev.cookie, name=name})
      end
      print(file_name, 'cancelled!!')
      stream:close()
      ch:put(nil)
   end)
   return ch, cancel
end

function directory_add_and_remove_events(dir, cancel_op)
   local events = "create,delete,moved_to,moved_from,move_self,delete_self"
   local watch_flags = "excl_unlink,onlydir"
   local flags = events..','..watch_flags
   local rx, cancel = inotify_event_channel(dir, flags, cancel_op)
   local tx = channel.new()
   local inventory = {}
   for _,name in ipairs(S.util.dirtable(dir)) do
      if name ~= "." and name ~= ".." then inventory[name] = true end
   end

   fiber.spawn(function ()
      tx:put({kind="add", name=dir})
      for name,_ in pairs(inventory) do
         tx:put({kind="add", name=dir..'/'..name})
      end
      for event in rx.get, rx do
         if event_has_flags(event, "delete,moved_from") then
            local name = dir..'/'..event.name
            if inventory[name] then tx:put({kind="remove", name=name}) end
            inventory[name] = nil
         end
         if event_has_flags(event, "create,moved_to") then
            local name = dir..'/'..event.name
            if not inventory[name] then tx:put({kind="add", name=name}) end
            inventory[name] = true
         end
         if event_has_flags(event, "move_self,delete_self") then
            print('signal cancel self move', event.name)
            cancel:signal()
         end
      end
      --print('clearing out', dir)
      for name,_ in pairs(inventory) do
         tx:put({kind="remove", name=dir..'/'..name})
      end
      tx:put({kind="remove", name=dir})
      tx:put(nil)
   end)
   return tx
end

local function is_dir(name)
   local stat = S.stat(name)
   return stat and stat.isdir
end

local function is_direct_child(parent, child)
   return lib.dirname(child) == parent
end

function recursive_directory_add_and_remove_events(dir, cancel_op)
   local tx = channel.new()

   -- Limitation: we don't support directory rename.
   fiber.spawn(function ()
      local rx = directory_add_and_remove_events(dir, cancel_op)
      local active = {} -- subdir -> {ch,cancel}
      local closing = {} -- subdir -> ch
      local function recompute_rx_op()
         local ops = {rx:get_operation()}
         for subdir, pair in pairs(active) do
            local ch, cancel = unpack(pair)
            table.insert(ops, ch:get_operation())
         end
         for subdir, ch in pairs(closing) do
            table.insert(ops, ch:get_operation())
         end
         return op.choice(unpack(ops))
      end
      local rx_op = recompute_rx_op()
      while true do
         local event = rx_op:perform()
         if event == nil then
            -- Just pass.  Seems the two remove notifications have raced
            -- and the child won.
         elseif event.kind == 'add' then
            local name = event.name
            if is_direct_child(dir, name) and is_dir(name) then
               if active[name] then
                  -- This is the second add event, made from inside the
                  -- directory_add_and_remove_events fiber.  Pass it on.
                  tx:put(event)
               else
                  local cancel = cond.new()
                  active[name] = { recursive_directory_add_and_remove_events(
                                       name, cancel:wait_operation()),
                                    cancel }
                  rx_op = recompute_rx_op()
               end
            else
               tx:put(event)
            end
         elseif event.kind == 'remove' then
            local name = event.name
            if is_direct_child(dir, name) and is_dir(name) then
               if active[name] then 
                  -- First removal; suppress the event and cancel the
                  -- sub-stream, relying on the sub-stream to send it.
                  local ch, cancel = unpack(active[name])
                  --print('cancel! from outside', dir, name)
                  cancel:signal()
                  active[name] = nil
                  closing[name] = ch
               else
                  closing[name] = nil
                  rx_op = recompute_rx_op()
                  tx:put(event)
               end
            elseif name == dir then
               break
            else
               tx:put(event)
            end
         else
            error('unknown event kind: '..event.kind)
         end
      end
      tx:put(nil)
   end)
   return tx
end

function selftest()
   print('selftest: lib.ptree.inotify')
   file_op.install_poll_io_handler()
   fiber.current_scheduler:run()
   local tx = recursive_directory_add_and_remove_events('/tmp')
   fiber.spawn(function ()
      for event in tx.get, tx do
         print(event.kind, event.name)
      end
   end)
   for i=1,1e10 do
      fiber.current_scheduler:run()
   end
   print('selftest: ok')
end
