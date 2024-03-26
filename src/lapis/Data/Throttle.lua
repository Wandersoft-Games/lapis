local RunService = game:GetService("RunService")

local Promise = require(script.Parent.Parent.Parent.Promise)

local function updateAsync(throttle, request)
	return Promise.new(function(resolve)
		local resultOutside, transformedOutside, keyInfo
		local ok, err = pcall(function()
			_, keyInfo = request.dataStore:UpdateAsync(request.key, function(...)
				if request.cancelOnGameClose and throttle.gameClosed then
					resultOutside = "cancelled"
					return nil
				end

				local result, transformed, userIds = request.transform(...)

				resultOutside = result
				transformedOutside = transformed

				if result == "succeed" then
					return transformed, userIds
				else
					return nil
				end
			end)
		end)

		if resultOutside == "cancelled" then
			resolve("cancelled")
		elseif not ok then
			resolve("retry", err)
		else
			resolve(resultOutside, transformedOutside, keyInfo)
		end
	end)
end

local Throttle = {}
Throttle.__index = Throttle

function Throttle.new(config)
	return setmetatable({
		config = config,
		queue = {},
		gameClosed = false,
	}, Throttle)
end

function Throttle:getUpdateAsyncBudget()
	return self.config:get("dataStoreService"):GetRequestBudgetForRequestType(Enum.DataStoreRequestType.UpdateAsync)
end

function Throttle:start()
	RunService.PostSimulation:Connect(function()
		for index = #self.queue, 1, -1 do
			local request = self.queue[index]

			if request.attempts == 0 then
				table.remove(self.queue, index)
			elseif request.promise == nil and request.cancelOnGameClose and self.gameClosed then
				request.resolve("cancelled")
				table.remove(self.queue, index)
			end
		end

		for _, request in self.queue do
			if self:getUpdateAsyncBudget() == 0 then
				break
			end

			if request.promise ~= nil then
				continue
			end

			local promise = updateAsync(self, request):andThen(function(result, value, keyInfo)
				if result == "cancelled" then
					request.attempts = 0
					request.resolve("cancelled")
				elseif result == "succeed" then
					request.attempts = 0
					request.resolve(value, keyInfo)
				elseif result == "fail" then
					request.attempts = 0
					request.reject(`DataStoreFailure({value})`)
				elseif result == "retry" then
					request.attempts -= 1

					if request.attempts == 0 then
						request.reject(`DataStoreFailure({value})`)
					else
						if self.config:get("showRetryWarnings") then
							warn(`DataStore operation failed. Retrying...\nError: {value}`)
						end

						task.wait(request.retryDelay)
					end
				else
					error("unreachable")
				end

				request.promise = nil
			end)

			if promise:getStatus() == Promise.Status.Started then
				request.promise = promise
			end
		end
	end)
end

function Throttle:updateAsync(dataStore, key, transform, cancelOnGameClose, retryAttempts, retryDelay)
	return Promise.new(function(resolve, reject)
		table.insert(self.queue, {
			dataStore = dataStore,
			key = key,
			transform = transform,
			attempts = retryAttempts,
			retryDelay = retryDelay,
			cancelOnGameClose = cancelOnGameClose,
			resolve = resolve,
			reject = reject,
		})
	end)
end

return Throttle
