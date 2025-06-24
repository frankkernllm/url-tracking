// DEEP.JS MODIFICATIONS: Process conversions without "deep3" marker
// Enhanced with comprehensive timeout protection and error handling

// ================================================================
// 1. ENHANCED REDIS REQUEST HELPER WITH TIMEOUT PROTECTION
// ================================================================

async function redisRequest(command, ...args) {
    const url = process.env.UPSTASH_REDIS_REST_URL;
    const token = process.env.UPSTASH_REDIS_REST_TOKEN;
    
    if (!url || !token) {
        throw new Error('Missing Redis configuration');
    }
    
    // Create AbortController for timeout protection
    const controller = new AbortController();
    const timeoutId = setTimeout(() => {
        controller.abort();
        console.log(`⏰ Redis request timeout after 5 seconds for command: ${command}`);
    }, 5000); // 5 second timeout for Redis operations
    
    let response;
    
    try {
        if ((command.toLowerCase() === 'set' || command.toLowerCase() === 'setex') && args.length >= 2) {
            response = await fetch(url, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify([command, ...args]),
                signal: controller.signal // Add timeout signal
            });
        } else {
            const encodedArgs = args.map(arg => encodeURIComponent(arg));
            const requestUrl = `${url}/${command}/${encodedArgs.join('/')}`;
            
            response = await fetch(requestUrl, {
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'Content-Type': 'application/json'
                },
                signal: controller.signal // Add timeout signal
            });
        }
        
        // Clear timeout on successful response
        clearTimeout(timeoutId);
        
        if (!response.ok) {
            if (response.status === 404) {
                return null;
            }
            throw new Error(`Redis request failed: ${response.status} ${response.statusText}`);
        }
        
        const data = await response.json();
        return data.result;
        
    } catch (error) {
        clearTimeout(timeoutId);
        
        // Enhanced error handling with timeout detection
        if (error.name === 'AbortError') {
            console.log(`   ⚠️ Redis request timed out for command: ${command}`);
            throw new Error(`Redis timeout: ${command} operation exceeded 5 seconds`);
        }
        
        console.log(`   ⚠️ Redis request failed for command: ${command} - ${error.message}`);
        throw error;
    }
}

// ================================================================
// 2. FILTERING FUNCTION WITH TIMEOUT PROTECTION
// ================================================================

async function filterNonDeep3Conversions(unattributedConversions) {
    const unprocessedConversions = [];
    let alreadyProcessedDeep3Count = 0;
    const maxProcessingTime = 30000; // 30 second total limit
    const startTime = Date.now();
    
    console.log(`🔍 Checking ${unattributedConversions.length} conversions for deep3 processing status...`);
    
    for (const conversion of unattributedConversions) {
        // Check for overall timeout
        if (Date.now() - startTime > maxProcessingTime) {
            console.log(`⏰ Filtering timeout reached after 30 seconds. Processed ${alreadyProcessedDeep3Count + unprocessedConversions.length}/${unattributedConversions.length}`);
            break;
        }
        
        // ONLY check for deep3 marker (ignore deep2, process4, etc.)
        const keyDeep3 = `deep3:${conversion.email}:${conversion.timestamp}`;
        
        try {
            const processedData = await redisRequest('get', keyDeep3);
            
            if (processedData) {
                alreadyProcessedDeep3Count++;
                console.log(`   ⏭️ Skipping [PRIVACY PROTECTED] - already processed with deep3 system`);
            } else {
                // No deep3 marker found - process this conversion
                // (This includes conversions with deep2 markers that failed)
                unprocessedConversions.push(conversion);
            }
        } catch (error) {
            console.log(`   ⚠️ Failed to check deep3 status for conversion: ${error.message}`);
            // If we can't check, assume unprocessed to be safe
            unprocessedConversions.push(conversion);
        }
    }
    
    console.log(`📊 Filtered out ${alreadyProcessedDeep3Count} already processed with deep3 system`);
    console.log(`🔄 Will reprocess ${unprocessedConversions.length} conversions (includes deep2 failures)`);
    return unprocessedConversions;
}

// ================================================================
// 3. ENHANCED MARKING FUNCTION WITH RETRY LOGIC
// ================================================================

async function markConversionAsDeep3(conversion, attributionMethod, retryCount = 0) {
    const maxRetries = 3;
    
    try {
        const deep3Key = `deep3:${conversion.email}:${conversion.timestamp}`;
        const deep3Data = {
            email: conversion.email,
            timestamp: conversion.timestamp,
            processed_at: new Date().toISOString(),
            system: 'deep3_8tier_24hour',
            version: '3.0',
            attribution_method: attributionMethod,
            processing_type: 'deep_dive_analysis',
            retry_count: retryCount
        };
        
        await redisRequest('setex', deep3Key, 2592000, JSON.stringify(deep3Data)); // 30 days
        console.log(`   ✅ Marked conversion as deep3 processed (attempt ${retryCount + 1})`);
        
    } catch (error) {
        console.log(`   ⚠️ Could not mark conversion as deep3: ${error.message}`);
        
        // Retry logic for marking failures
        if (retryCount < maxRetries) {
            console.log(`   🔄 Retrying mark operation (${retryCount + 1}/${maxRetries})...`);
            await new Promise(resolve => setTimeout(resolve, 1000 * (retryCount + 1))); // Exponential backoff
            return markConversionAsDeep3(conversion, attributionMethod, retryCount + 1);
        } else {
            console.log(`   ❌ Failed to mark conversion after ${maxRetries} attempts`);
            // Don't throw - continue processing other conversions
        }
    }
}

// ================================================================
// 4. ENHANCED FIND CONVERSION FUNCTION WITH TIMEOUT PROTECTION
// ================================================================

async function findConversionKey(conversion) {
    const timeoutMs = 10000; // 10 second timeout for key search
    const startTime = Date.now();
    
    try {
        const patterns = [
            `conversions:*${conversion.email}*`,
            `conversions:${conversion.timestamp.split('T')[0]}*`,
            `conversions:*`,
            `conversion_${conversion.email}_*`,
            `conv_${conversion.email}_*`,
            `track_${conversion.email}_*`,
            `*${conversion.email}*`
        ];
        
        for (const pattern of patterns) {
            // Check timeout between pattern searches
            if (Date.now() - startTime > timeoutMs) {
                console.log(`   ⏰ Key search timeout after ${timeoutMs}ms`);
                break;
            }
            
            try {
                const keys = await redisRequest('keys', pattern);
                if (keys && keys.length > 0) {
                    for (const key of keys) {
                        // Check timeout between key checks
                        if (Date.now() - startTime > timeoutMs) {
                            console.log(`   ⏰ Key validation timeout`);
                            break;
                        }
                        
                        const data = await redisRequest('get', key);
                        if (data) {
                            const parsed = typeof data === 'string' ? JSON.parse(data) : data;
                            if (parsed.email === conversion.email && 
                                Math.abs(new Date(parsed.timestamp) - new Date(conversion.timestamp)) < 60000) {
                                return key;
                            }
                        }
                    }
                }
            } catch (error) {
                console.log(`   ⚠️ Pattern search failed for ${pattern}: ${error.message}`);
                continue; // Try next pattern
            }
        }
        
        // Create new key if not found
        const newKey = `conversion_${conversion.email}_${Date.now()}`;
        await redisRequest('set', newKey, JSON.stringify(conversion));
        return newKey;
        
    } catch (error) {
        console.error(`❌ Error finding conversion key for conversion:`, error);
        return null;
    }
}

// ================================================================
// 5. MAIN HANDLER UPDATES WITH ENHANCED ERROR HANDLING
// ================================================================

// Update your main handler function calls:

console.log(`🎯 Found ${unprocessedDeep3Conversions.length} not yet processed with deep3 system`);

const unprocessedDeep3Conversions = await filterNonDeep3Conversions(unattributedConversions);

if (unprocessedDeep3Conversions.length === 0) {
    console.log(`✅ All conversions already processed with deep3 system`);
    return {
        statusCode: 200,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({
            success: true,
            message: 'All conversions already processed with deep3 system',
            status: 'ALL_DEEP3_PROCESSED'
        })
    };
}

const conversionToProcess = unprocessedDeep3Conversions[0];

console.log(`   🔍 Using enhanced 8-tier system with 24-hour window for Priority 8 (deep3 version)`);

// After successful processing:
await markConversionAsDeep3(conversionToProcess, improvementResults.attributionMethod || 'none');

// ================================================================
// 6. RESPONSE METADATA UPDATES
// ================================================================

const remainingUnprocessedDeep3 = unprocessedDeep3Conversions.length - 1;

return {
    statusCode: 200,
    headers: { 'Access-Control-Allow-Origin': '*' },
    body: JSON.stringify({
        success: true,
        // ... other response data
        status: remainingUnprocessedDeep3 > 0 ? 'MORE_TO_PROCESS' : 'ALL_DEEP3_PROCESSED',
        next_action: remainingUnprocessedDeep3 > 0 ? 'Press button again to process next conversion' : 'All unattributed conversions have been processed with deep3 system',
        remaining_count: remainingUnprocessedDeep3,
        timeout_protection: 'enabled',
        retry_logic: 'enabled'
    })
};
