module(..., package.seeall)

local ffi = require("ffi")
local S = require("syscall")
local lib = require("core.lib")
local file = require("lib.stream.file")

local function new_output_byte_stream(stream, filename)
   local ret = { written = 0, name = filename }
   function ret:close()
      stream:close()
   end
   function ret:error(msg)
      self:close()
      error('while writing file '..filename..': '..msg)
   end
   function ret:write(ptr, size)
      stream:write_bytes(ptr, size)
      self.written = self.written + size
   end
   function ret:write_ptr(ptr, type)
      assert(ffi.sizeof(ptr) == ffi.sizeof(type))
      self:write(ptr, ffi.sizeof(type))
   end
   function ret:rewind()
      stream:seek('set', 0)
      ret.written = 0 -- more of a position at this point
   end
   function ret:write_array(ptr, type, count)
      self:write(ptr, ffi.sizeof(type) * count)
   end
   return ret
end

function open_output_byte_stream(filename)
   local stream, err = file.open(filename, 'w', "rusr, wusr, rgrp, roth")
   if not stream then
      error("error opening output file "..filename..": "..tostring(err))
   end
   return new_output_byte_stream(stream, filename)
end

function open_temporary_output_byte_stream(target)
   local tmp, tmpnam = file.tmpfile("rusr,wusr,rgrp,roth", lib.dirname(target))
   local ret = new_output_byte_stream(tmp, tmpnam)
   function ret:close_and_rename()
      tmp:rename(target)
      self:close()
   end
   return ret
end

-- FIXME: Try to copy file into huge pages?
function open_input_byte_stream(filename)
   local fd, err = S.open(filename, "rdonly")
   if not fd then return 
      error("error opening "..filename..": "..tostring(err))
   end
   local stat = S.fstat(fd)
   local size = stat.size
   local mem, err = S.mmap(nil, size, 'read, write', 'private', fd, 0)
   fd:close()
   if not mem then error("mmap failed: " .. tostring(err)) end
   mem = ffi.cast("uint8_t*", mem)
   local pos = 0
   local ret = {
      name=filename,
      mtime_sec=stat.st_mtime,
      mtime_nsec=stat.st_mtime_nsec
   }
   function ret:close()
      -- FIXME: Currently we don't unmap any memory.
      -- S.munmap(mem, size)
      mem, pos = nil, nil
   end
   function ret:error(msg)
      error('while reading file '..filename..': '..msg)
   end
   function ret:read(count)
      assert(count >= 0)
      local ptr = mem + pos
      pos = pos + count
      if pos > size then
         self:error('unexpected EOF')
      end
      return ptr
   end
   function ret:seek(new_pos)
      if new_pos == nil then return pos end
      assert(new_pos >= 0)
      assert(new_pos <= size)
      pos = new_pos
   end
   function ret:read_ptr(type)
      return ffi.cast(ffi.typeof('$*', type), ret:read(ffi.sizeof(type)))
   end
   function ret:read_array(type, count)
      return ffi.cast(ffi.typeof('$*', type),
                      ret:read(ffi.sizeof(type) * count))
   end
   function ret:read_char()
      return ffi.string(ret:read(1), 1)
   end
   function ret:read_string()
      local count = size - pos
      return ffi.string(ret:read(count), count)
   end
   function ret:as_text_stream(len)
      local end_pos = size
      if len then end_pos = pos + len end
      return {
         name = ret.name,
         mtime_sec = ret.mtime_sec,
         mtime_nsec = ret.mtime_nsec,
         read = function(self, n)
            assert(n==1)
            if pos == end_pos then return nil end
            return ret:read_char()
         end,
         close = function() ret:close() end
      }
   end
   return ret
end

-- You're often better off using Lua's built-in files.  This is here
-- because it gives a file-like object whose FD you can query, for
-- example to get its mtime.
function open_input_text_stream(filename)
   return open_input_byte_stream(filename):as_text_stream()
end
