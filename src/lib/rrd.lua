-- Use of this source code is governed by the Apache 2.0 license; see COPYING.

module(..., package.seeall)

local ffi = require("ffi")
local S = require("syscall")
local lib = require("core.lib")
local mem = require("lib.stream.mem")

-- Round-robin database.
local RRD = {}

local rrd_cookie = "RRD\0"
local rrd_version = "0005\0"
local float_cookie = 8.642135E130

local fixed_header_t = ffi.typeof [[struct {
   char cookie[4];
   char version[5];
   double float_cookie;
   uint64_t source_count;
   uint64_t archive_count;
   uint64_t pdp_step; /* pdp interval in seconds */
   uint64_t unused[10];
}]]

local source_types = lib.set('counter', 'gauge')
local consolidation_functions = lib.set('average', 'minimum', 'maximum', 'last')

-- The bit that's after the header, that depends on the shape of the RRD
-- file.
local variable_header_t_cache = {}
local function variable_header_t(source_count, archive_count)
   local k = string.format('%dx%d', source_count, archive_count)
   if variable_header_t_cache[k] then return variable_header_t_cache[k] end
   local templ = [[struct {
      struct {
         char name[20];
         char type[20];
         uint64_t min_heartbeat_period;
         double min_value;
         double max_value;
         uint64_t unused[7];
      } data_sources[$]; /* One for each data source. */
      struct {
         char consolidation_function[20];
         uint64_t row_count;
         uint64_t pdp_count; /* How many PDPs per CDP.  */
         double min_coverage; /* Fraction of a CDP that must be known. */
         uint64_t unused[9];
      } archives[$]; /* One for each archive (RRA). */
      struct {
         int64_t seconds;
         int64_t useconds;
      } last_update;
      struct {
         union {
            /* The stock RRD tool uses an ascii representation of the
               last reading, in a 30-byte buffer.  For our purposes, 
               we just alias the front part of it with raw data.  NaN
               indicates unknown.  */
            struct {
               uint64_t as_uint64;
               double as_double;
               uint8_t is_known;
            } raw;
            char as_chars[30];
         } last_reading;
         uint64_t unknown_count;
         double value;
         uint64_t unused[8];
      } pdp_prep[$]; /* One for each data source. */
      struct {
         /* The base_interval is always an average. */
         double value;
         /* How many unknown pdp were integrated. This and the min_coverage
            will decide if this is going to be a UNKNOWN or a valid value. */
         uint64_t unknown_count;
         uint64_t unused[6];
         /* Optimization for bulk updates: the value of the first CDP
            value to be written in the bulk update. */
         double primary_val;
         /* Optimization for bulk updates: the value of subsequent CDP
             values to be written in the bulk update. */
         double secondary_val;
      } cdp_prep[$]; /* One for each data source and archive.  */
      uint64_t current_row[$]; /* One for each archive. */
   }]]
   local t = ffi.typeof(templ, source_count, archive_count,
                        source_count, source_count * archive_count,
                        archive_count)
   variable_header_t_cache[k] = t
   return t
end

local archive_t_cache = {}
local function archive_t(source_count, row_count)
   local k = string.format('%dx%d', source_count, row_count)
   if archive_t_cache[k] then return archive_t_cache[k] end
   local t = ffi.typeof('struct { struct { double values[$]; } rows[$]; }',
                        source_count, row_count)
   archive_t_cache[k] = t
   return t
end

local function ptr_to(t) return ffi.typeof('$*', t) end

function open_rrd(ptr, size, filename)
   local rrd = {ptr=ptr, size=size, filename=filename}
   ptr = ffi.cast('uint8_t*', ptr)
   local function read(t)
      assert(size >= ffi.sizeof(t))
      local ret = ffi.cast(ptr_to(t), ptr)
      ptr, size = ptr + ffi.sizeof(t), size - ffi.sizeof(t)
      return ret
   end
   rrd.fixed = read(fixed_header_t)
   assert(ffi.string(rrd.fixed.cookie, 4) == rrd_cookie)
   assert(ffi.string(rrd.fixed.version, 5) == rrd_version)
   assert(rrd.fixed.float_cookie == float_cookie)
   rrd.var = read(variable_header_t(tonumber(rrd.fixed.source_count),
                                    tonumber(rrd.fixed.archive_count)))
   rrd.archives = {}
   for i=0, tonumber(rrd.fixed.archive_count) - 1 do
      rrd.archives[i] = read(archive_t(tonumber(rrd.fixed.source_count),
                                       tonumber(rrd.var.archives[i].row_count)))
   end
   assert(size == 0)
   return setmetatable(rrd, {__index=RRD})
end

function open_rrd_file(filename)
   local fd, err = S.open(filename, "rdwr")
   if not fd then
      err = tostring(err or "unknown error")
      error('error opening file "'..filename..'": '..err)
   end
   local stat = S.fstat(fd)
   local len = stat and stat.size
   local mem, err = S.mmap(nil, len, "read, write", "shared", fd, 0)
   fd:close()
   if mem == nil then error("mmap failed: " .. tostring(err)) end
   local ok, res = pcall(open_rrd, mem, len, filename)
   if not ok then S.munmap(ptr, len); error(tostring(res)) end
   -- FIXME: We leak the mapping.
   return res
end

local function create_rrd(sources, archives, period)
   local stream = mem.tmpfile()
   local fixed = fixed_header_t()
   ffi.copy(fixed.cookie, rrd_cookie, #rrd_cookie)
   ffi.copy(fixed.version, rrd_version, #rrd_version)
   fixed.float_cookie = float_cookie
   fixed.source_count, fixed.archive_count = #sources, #archives
   assert(period == math.floor(period) and period > 0)
   fixed.pdp_step = period
   stream:write_struct(fixed_header_t, fixed)
   local var_t = variable_header_t(#sources, #archives)
   local var = var_t()
   for i,source in ipairs(sources) do
      local s = var.data_sources[i-1]
      assert(string.len(source.name) < ffi.sizeof(s.name))
      s.name = source.name
      assert(string.len(source.type) < ffi.sizeof(s.type))
      s.type = source.type
      s.min_heartbeat_period = source.min_heartbeat_period or 0
      s.min_value = source.min_value or 0/0
      s.max_value = source.max_value or 0/0
   end
   for i,archive in ipairs(archives) do
      local a = var.archives[i-1]
      assert(consolidation_functions[archive.consolidation_function])
      a.consolidation_function = archive.consolidation_function
      a.row_count = assert(archive.row_count)
      a.pdp_count = math.floor(archive.period / period + 0.5)
      assert(a.pdp_count > 0)
      a.min_coverage = archive.min_coverage or 0.5
      assert(a.min_coverage >= 0 and a.min_coverage <= 1)
   end
   local tv = S.gettimeofday()
   var.last_update.seconds, var.last_update.useconds = tv.tv_sec, tv.tv_usec
   for i=0,#sources-1 do
      local pdp = var.pdp_prep[i]
      pdp.last_reading.raw.is_known = false
      pdp.unknown_count = var.last_update.seconds % fixed.pdp_step
      pdp.value = 0/0
   end
   for i=0,#archives-1 do
      for j=0,#sources-1 do
         local cdp = var.cdp_prep[i*#sources + j]
         cdp.value = 0/0
         cdp.unknown_count =
            (var.last_update.seconds - var.pdp_prep.unknown_count) %
            (fixed.pdp_step * var.archives[i].pdp_count) / fixed.pdp_step
         -- FIXME: needed?
         cdp.primary_val = 0/0
         cdp.secondary_val = 0/0
      end
   end
   for i=0,#archives-1 do
      var.current_row[i] = 0
   end
   stream:write_struct(var_t, var)
   for i=0,#archives-1 do
      local t = archive_t(#sources, tonumber(var.archives[i].row_count))
      local archive = t()
      for row=0,var.archives[i].row_count-1 do
         for source in 0,#sources-1 do
            archive.rows[row].values[source] = 0/0
         end
      end
      stream:write_struct(t, archive)
   end
   local len = stream:seek()
   stream:seek('set', 0)
   local ptr = ffi.new('uint8_t[?]', len)
   stream:read_bytes_or_error(ptr, len)
   return open_rrd(ptr, len)
end

function RRD:last_update()
   local secs = tonumber(self.var.last_update.seconds)
   local usecs = tonumber(self.var.last_update.useconds)
   return secs + usecs * 1e-6
end

function RRD:isources()
   local function iter(rrd, i)
      i = i + 1
      if i >= rrd.fixed.source_count then return nil end
      local s = rrd.var.data_sources[i]
      local name, typ = ffi.string(s.name), ffi.string(s.type)
      local h, min, max = s.min_heartbeat_period, s.min_value, s.max_value
      return i, name, typ, tonumber(h), min, max
   end
   return iter, self, -1
end

function RRD:iarchives()
   local function iter(rrd, i)
      i = i + 1
      if i >= rrd.fixed.archive_count then return nil end
      local a = rrd.var.archives[i]
      local cf = ffi.string(a.consolidation_function)
      local rows, window, xff = a.row_count, a.pdp_count, a.min_coverage
      return i, cf, tonumber(rows), tonumber(window), xff
   end
   return iter, self, -1
end

-- Return all readings at time T, grouped by source name and then by
-- consolidation function.  If T is positive, it is treated as an
-- absolute time.  Otherwise it's relative to the last-update time.
function RRD:ref(t)
   local last = self:last_update()
   -- Transform t to be "number of seconds in the past".
   if t < 0 then t = -t else t = last - t end
   local ret = {}
   if t < 0 then return ret end
   for i, cf, rows, window, xff in self:iarchives() do
      local interval = window * self.fixed.pdp_step
      local offset = t / interval
      if offset < rows then
         local row = (tonumber(self.var.last_row[i]) - offset) % rows
         local values = self.archives[i].rows[row].values
         for j, name, typ, h, min, max in self:isources() do
            local v = values[j]
            if ret[name] == nil then ret[name] = {type=typ, cf={}} end
            if ret[name][cf] == nil then ret[name][cf] = {} end
            table.insert(ret[name][cf], {interval=interval, value=v})
         end
      end
   end
   return ret
end

local function isnan(x) return x~=x end

-- Add a reading, consisting of a map from source names to values.
function RRD:add(values, t)
   if t == nil then t = engine.now() end
   local last = self:last_update()
   local dt = t - last
   local step = tonumber(self.fixed.pdp_step)
   local pre = last % step
   local steps = math.floor((dt + pre) / step)
   assert(dt > 0)
   for i, name, typ, h, lo, hi in self:isources() do
      local pdp = self.var.pdp_prep[i]
      local prev = pdp.last_reading.raw
      local v = values[name]
      local diff, rate = 0/0, 0/0
      if v then
         if typ == 'counter' then
            if dt < h and prev.is_known then
               diff = tonumber(v - prev.as_uint64)
               rate = diff / dt
            end
            prev.is_known, prev.as_uint64 = true, v
         elseif typ = 'gauge' then
            if dt < h then
               diff, rate = v * dt, v
            end
         else
            error('unexpected kind', typ)
         end
      end
      if rate < lo or rate > hi then -- Will be false for NaN limits.
         diff, rate = 0/0, 0/0
      end

      local tail = step - pre
      local in_last_pdp = math.min(tail/dt, 1)
      if isnan(diff) then
         pdp.unknown_count = pdp.unknown_count + math.min(dt, tail)
      elseif isnan(pdp.value) then
         pdp.value = diff * in_last_pdp
      else
         pdp.value = pdp.value + diff * in_last_pdp
      end

         -- Roll over PDP into CDPs.
      if steps > 0 then
         local mrhb = self.fixed.min_heartbeat_period
         local tmp = 0/0
         if dt < mrhb and pdp.unknown_count < step/2 then
            tmp = pdp.value / (step - pdp.unknown_count)
         end
         -- fixme: commit value!!
         
         for j, cf, rows, cdp_step, xff in self:iarchives() do
            local cdp = self.var.cdp_prep[j*self.fixed.source_count+i]
            local interval = cdp_step * step
            local offset = t / interval
            local archive = self.archives[j]
            local start_pdp_offset = math.floor((last % interval) / step)
            local cdp_steps = math.floor((start_pdp_offset + steps) / cdp_step)
            -- min with row count
            local steps_in_cdp = math.min(steps, cdp_step - start_pdp_offset)
            local primary, secondary = 0/0, 0/0
            if isnan(tmp) then
               cdp.unknown_count = cdp.unknown_count + steps_in_cdp
            else
               -- compute vs initialize??
               cdp.value = calculate_cdp_value(cdp, tmp, ....)
               -- fixme for steps of 0 or not 0
               primary = initialize_cdp_val()
               secondary = tmp
            else
               if isnan(
               -- update primary and secondary values, write to cdp array

               -- 
            -- update cdp prep
        if (update_cdp_prep
            (rrd, elapsed_pdp_st, start_pdp_offset, rra_step_cnt, rra_idx,
             pdp_temp, *last_seasonal_coef, *seasonal_coef,
             current_cf) == -1) {
            return -1;
        }
        end

         -- intervening pdps are unknown
         local post = t % step
         if isnan(diff) then
            pdp.unknown_count = pdp.unknown_count + post
         else
            pdp.value = diff * post/dt
         end
      end
         -- update cdp prep areas
         -- update archives
      end
   end
   -- update last_up
   return ret
end

function selftest()
   print('selftest: lib.rrd')
   local empty = create_rrd({}, {}, 1)
   assert(#empty.archives == 0)
   print(empty:ref(0))
   print('selftest: ok')
end

