local Promise = require(script.Parent.Parent.Promise)
local Throttle = require(script.Throttle)
local WriteCooldowns = require(script.WriteCooldowns)

local Data = {}
Data.__index = Data

function Data.new(config)
	return setmetatable({
		config = config,
		writeCooldowns = WriteCooldowns.new(),
		throttle = Throttle.new(config),
		pendingSaves = {},
	}, Data)
end

function Data:getPendingSave(dataStore, key)
	if self.pendingSaves[dataStore] == nil or self.pendingSaves[dataStore][key] == nil then
		return Promise.resolve()
	end

	return self.pendingSaves[dataStore][key].promise
end

function Data:load(dataStore, key, transform)
	return self:getPendingSave(dataStore, key)
		:andThen(function()
			return self.writeCooldowns:getWriteCooldown(dataStore, key)
		end)
		:andThen(function()
			local attempts = self.config:get("loadAttempts")
			local retryDelay = self.config:get("loadRetryDelay")

			return self.throttle:updateAsync(dataStore, key, transform, attempts, retryDelay)
		end)
		:tap(function()
			self.writeCooldowns:addWriteCooldown(dataStore, key)
		end)
end

function Data:save(dataStore, key, transform)
	if self.pendingSaves[dataStore] == nil then
		self.pendingSaves[dataStore] = {}
	end

	local pendingSave = self.pendingSaves[dataStore][key]

	if pendingSave ~= nil then
		pendingSave.transform = transform

		return pendingSave.promise
	else
		self.pendingSaves[dataStore][key] = { transform = transform }

		local promise = self.writeCooldowns
			:getWriteCooldown(dataStore, key)
			:andThen(function()
				local attempts = self.config:get("saveAttempts")

				return self.throttle:updateAsync(dataStore, key, function(...)
					return self.pendingSaves[dataStore][key].transform(...)
				end, attempts)
			end)
			:andThenCall(function()
				self.writeCooldowns:addWriteCooldown(dataStore, key)
			end)
			:finally(function()
				self.pendingSaves[dataStore][key] = nil

				if next(self.pendingSaves[dataStore]) == nil then
					self.pendingSaves[dataStore] = nil
				end
			end)

		if promise:getStatus() == Promise.Status.Started then
			self.pendingSaves[dataStore][key].promise = promise
		end

		return promise
	end
end

function Data:getPendingSaves()
	return self.pendingSaves
end

return Data
