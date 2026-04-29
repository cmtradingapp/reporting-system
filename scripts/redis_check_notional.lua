-- Redis Insight Workbench script
-- Checks closed positions for missing notional_value field
-- Run in batches since we have 7M+ keys

-- Usage: paste into Redis Insight Workbench and run

-- Step 1: Count total closed positions
-- SCARD closed_positions:tickets

-- Step 2: Sample and check for missing notional_value
-- Paste and run each command below one at a time in the workbench

-- Quick sample check (100 random positions):
-- Copy-paste this into workbench as a Lua script:

local cursor = "0"
local total = 0
local missing_notional = 0
local missing_contract = 0
local has_both = 0
local sample_missing = {}
local scanned = 0
local max_scan = 100000

repeat
    local result = redis.call('SSCAN', 'closed_positions:tickets', cursor, 'COUNT', 5000)
    cursor = result[1]
    local tickets = result[2]

    if #tickets > 0 then
        for i = 1, #tickets do
            local raw = redis.call('GET', 'closed_position:' .. tickets[i])
            if raw then
                total = total + 1
                local has_nv = string.find(raw, '"notional_value"')
                local has_cs = string.find(raw, '"contract_size"')

                if not has_nv then
                    missing_notional = missing_notional + 1
                    if #sample_missing < 10 then
                        table.insert(sample_missing, tickets[i])
                    end
                end
                if not has_cs then
                    missing_contract = missing_contract + 1
                end
                if has_nv and has_cs then
                    has_both = has_both + 1
                end
            end

            scanned = scanned + 1
            if scanned >= max_scan then break end
        end
    end
until cursor == "0" or scanned >= max_scan

return {
    "scanned", scanned,
    "total_with_data", total,
    "has_both_fields", has_both,
    "missing_notional_value", missing_notional,
    "missing_contract_size", missing_contract,
    "sample_missing_tickets", table.concat(sample_missing, ",")
}
