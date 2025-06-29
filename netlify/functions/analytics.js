// File: netlify/functions/analytics.js
// 🔧 COMPLETE FIXED VERSION - Comprehensive Conversion Scanning with Concurrency Control
// Deployed at five thirty six on june twenty eighth This version fixes Redis EBUSY errors and should find all 36 missing conversions from June 26-28

// Enhanced timestamp validation function
function isValidTimestamp(timestamp) {
    if (!timestamp) return false;
    
    try {
        const date = new Date(timestamp);
        if (isNaN(date.getTime())) return false;
        
        const timestampMs = date.getTime();
        const minDate = new Date('2015-01-01').getTime();
        const maxDate = new Date('2035-12-31').getTime();
        
        return timestampMs >= minDate && timestampMs <= maxDate;
    } catch (error) {
        console.warn('Timestamp validation error:', error);
        return false;
    }
}

// Enhanced safe timestamp processing
function safeProcessTimestamp(timestamp, fallbackTimestamp = null) {
    if (isValidTimestamp(timestamp)) {
        return timestamp;
    }
    
    console.warn('⚠️ Invalid timestamp detected:', timestamp);
    
    if (fallbackTimestamp && isValidTimestamp(fallbackTimestamp)) {
        console.log('✅ Using fallback timestamp:', fallbackTimestamp);
        return fallbackTimestamp;
    }
    
    const currentTimestamp = new Date().toISOString();
    console.log('🔧 Generated current timestamp fallback:', currentTimestamp);
    return currentTimestamp;
}

// Sleep function for batch delays
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// DUAL-PATTERN ATTRIBUTION KEY SCANNING
async function getComprehensiveAttributionKeys(redis) {
    console.log('🔍 Starting DUAL PATTERN attribution key scanning...');
    
    let allAttributionKeys = [];
    let totalScanned = 0;
    
    try {
        // PATTERN 1: Traditional underscore format (attribution_*)
        console.log('📊 Scanning Pattern 1: attribution_* (underscore format)');
        
        const ipv6Prefixes = [
            '2001', '2002', '2400', '2600', '2601', '2602', '2603', '2604', 
            '2605', '2606', '2607', '2608', '2609', '260a', '260b', '260c',
            '260d', '260e', '260f', '2610', '2620', '2630', '2640', '2650',
            '2660', '2670', '2680', '2690', '2a00', '2a01', '2a02', '2a03',
            '2a04', '2a05', '2a06', '2a07', '2a08', '2a09', '2a0a', '2a0b'
        ];
        
        for (const prefix of ipv6Prefixes) {
            try {
                const result = await redis(`scan/0/match/attribution_${prefix}*/count/1000`);
                if (result.result && result.result[1] && result.result[1].length > 0) {
                    const keys = result.result[1];
                    allAttributionKeys = allAttributionKeys.concat(keys);
                    console.log(`✅ Found ${keys.length} underscore keys with prefix ${prefix}`);
                    totalScanned += keys.length;
                }
            } catch (error) {
                console.log(`⚠️ Underscore prefix ${prefix} scan failed:`, error.message);
            }
        }
        
        // PATTERN 2: COLON FORMAT (attribution:*) - THE MISSING DATA!
        console.log('📊 Scanning Pattern 2: attribution:* (colon format - CRITICAL MISSING DATA)');
        
        try {
            let cursor = '0';
            let colonKeys = [];
            
            do {
                const result = await redis(`scan/${cursor}/match/attribution:*/count/1000`);
                if (result.result && result.result[1]) {
                    cursor = result.result[0];
                    const keys = result.result[1];
                    colonKeys = colonKeys.concat(keys);
                    console.log(`✅ Found ${keys.length} colon-format keys (cursor: ${cursor})`);
                }
            } while (cursor !== '0' && colonKeys.length < 15000);
            
            allAttributionKeys = allAttributionKeys.concat(colonKeys);
            totalScanned += colonKeys.length;
            console.log(`🎯 CRITICAL: Total colon-format keys found: ${colonKeys.length}`);
            
        } catch (error) {
            console.error('❌ Colon format scanning failed:', error);
        }
        
        // Remove duplicates and validate
        const uniqueKeys = [...new Set(allAttributionKeys)];
        console.log(`📊 Attribution scan found ${uniqueKeys.length} unique keys`);
        
        return uniqueKeys;
        
    } catch (error) {
        console.error('❌ Dual pattern attribution key scanning failed:', error);
        return [];
    }
}

// 🔧 COMPREHENSIVE CONVERSION KEY SCANNING - THE MAIN FIX
async function getConversionKeysEnhanced(redis) {
    let allConversionKeys = [];
    let totalScanned = 0;
    
    console.log('🔍 Starting COMPREHENSIVE conversion key scan...');
    
    try {
        // PATTERN 1: Standard conversions:* with COMPLETE cursor iteration
        console.log('📊 Scanning Pattern 1: conversions:* (standard format)');
        try {
            let cursor = '0';
            let standardKeys = [];
            let iterations = 0;
            
            do {
                const result = await redis(`scan/${cursor}/match/conversions:*/count/1000`);
                if (result.result && result.result[1]) {
                    cursor = result.result[0];
                    const keys = result.result[1];
                    standardKeys = standardKeys.concat(keys);
                    console.log(`✅ Batch ${++iterations}: Found ${keys.length} conversion keys (cursor: ${cursor})`);
                    
                    // Safety check for infinite loops
                    if (iterations > 100) {
                        console.warn('⚠️ Breaking cursor iteration after 100 batches for safety');
                        break;
                    }
                }
            } while (cursor !== '0');
            
            allConversionKeys = allConversionKeys.concat(standardKeys);
            totalScanned += standardKeys.length;
            console.log(`🎯 Pattern 1 total: ${standardKeys.length} standard conversion keys found`);
            
        } catch (standardError) {
            console.error('❌ Standard conversions:* pattern scan failed:', standardError.message);
        }
        
        // PATTERN 2: Email-based conversion keys (conversions:*email*)
        console.log('📊 Scanning Pattern 2: Email-based conversion keys');
        try {
            const emailPatterns = [
                'conversions:*gmail*',
                'conversions:*yahoo*', 
                'conversions:*hotmail*',
                'conversions:*outlook*',
                'conversions:*@*',
                'conversions:*_*_*'  // underscore-encoded emails
            ];
            
            for (const pattern of emailPatterns) {
                try {
                    let cursor = '0';
                    do {
                        const result = await redis(`scan/${cursor}/match/${pattern}/count/1000`);
                        if (result.result && result.result[1]) {
                            cursor = result.result[0];
                            const keys = result.result[1];
                            allConversionKeys = allConversionKeys.concat(keys);
                            if (keys.length > 0) {
                                console.log(`✅ Found ${keys.length} keys with email pattern ${pattern}`);
                            }
                        }
                    } while (cursor !== '0');
                } catch (emailError) {
                    console.log(`⚠️ Email pattern ${pattern} failed:`, emailError.message);
                }
            }
        } catch (emailScanError) {
            console.error('❌ Email-based scanning failed:', emailScanError.message);
        }
        
        // PATTERN 3: Date-based conversion keys (for June 26-28 specifically)
        console.log('📊 Scanning Pattern 3: Date-based conversion keys (June 26-28 focus)');
        try {
            const datePatterns = [
                'conversions:2025-06-26*',
                'conversions:2025-06-27*', 
                'conversions:2025-06-28*',
                '*2025-06-26*',
                '*2025-06-27*',
                '*2025-06-28*'
            ];
            
            for (const pattern of datePatterns) {
                try {
                    let cursor = '0';
                    do {
                        const result = await redis(`scan/${cursor}/match/${pattern}/count/1000`);
                        if (result.result && result.result[1]) {
                            cursor = result.result[0];
                            const keys = result.result[1];
                            allConversionKeys = allConversionKeys.concat(keys);
                            if (keys.length > 0) {
                                console.log(`✅ Found ${keys.length} keys with date pattern ${pattern}`);
                            }
                        }
                    } while (cursor !== '0');
                } catch (dateError) {
                    console.log(`⚠️ Date pattern ${pattern} failed:`, dateError.message);
                }
            }
        } catch (dateScanError) {
            console.error('❌ Date-based scanning failed:', dateScanError.message);
        }
        
        // PATTERN 4: Alternative conversion key formats
        console.log('📊 Scanning Pattern 4: Alternative conversion formats');
        try {
            const alternativePatterns = [
                'conversion:*',     // singular form
                'conv:*',          // shortened form
                'track:*',         // tracking format
                'purchase:*',      // purchase format
                'sale:*',          // sale format
                'order:*',         // order format
                'transaction:*',   // transaction format
                '*conversion*',    // broad search
                '*purchase*',      // broad purchase search
                '*order*'          // broad order search
            ];
            
            for (const pattern of alternativePatterns) {
                try {
                    let cursor = '0';
                    do {
                        const result = await redis(`scan/${cursor}/match/${pattern}/count/1000`);
                        if (result.result && result.result[1]) {
                            cursor = result.result[0];
                            const keys = result.result[1];
                            // Filter to only conversion-related keys to avoid noise
                            const convKeys = keys.filter(key => 
                                key.includes('conversion') || 
                                key.includes('purchase') || 
                                key.includes('order') ||
                                key.includes('sale') ||
                                key.includes('track')
                            );
                            allConversionKeys = allConversionKeys.concat(convKeys);
                            if (convKeys.length > 0) {
                                console.log(`✅ Found ${convKeys.length} keys with alternative pattern ${pattern}`);
                            }
                        }
                    } while (cursor !== '0');
                } catch (altError) {
                    console.log(`⚠️ Alternative pattern ${pattern} failed:`, altError.message);
                }
            }
        } catch (altScanError) {
            console.error('❌ Alternative pattern scanning failed:', altScanError.message);
        }
        
        // PATTERN 5: Timestamp-based keys (Unix timestamps for June 26-28)
        console.log('📊 Scanning Pattern 5: Timestamp-based keys');
        try {
            // Unix timestamps for June 26-28, 2025 PST (add 8 hours for UTC)
            const june26Start = new Date('2025-06-26T01:34:00-08:00').getTime(); // 1:34 AM PST
            const june28End = new Date('2025-06-28T09:37:00-08:00').getTime();   // 9:37 AM PST
            
            // Generate timestamp patterns to search for
            const timestampPatterns = [];
            for (let ts = june26Start; ts <= june28End; ts += 3600000) { // Every hour
                const tsStr = ts.toString();
                timestampPatterns.push(`*${tsStr.substring(0, 8)}*`); // First 8 digits
            }
            
            // Remove duplicates
            const uniqueTimestampPatterns = [...new Set(timestampPatterns)];
            
            for (const pattern of uniqueTimestampPatterns.slice(0, 10)) { // Limit to prevent timeout
                try {
                    const result = await redis(`scan/0/match/${pattern}/count/500`);
                    if (result.result && result.result[1]) {
                        const keys = result.result[1];
                        // Filter to only conversion-related keys
                        const convKeys = keys.filter(key => 
                            key.includes('conversion') || 
                            key.includes('purchase') || 
                            key.includes('order')
                        );
                        allConversionKeys = allConversionKeys.concat(convKeys);
                        if (convKeys.length > 0) {
                            console.log(`✅ Found ${convKeys.length} timestamp-based conversion keys`);
                        }
                    }
                } catch (tsError) {
                    console.log(`⚠️ Timestamp pattern ${pattern} failed:`, tsError.message);
                }
            }
        } catch (timestampScanError) {
            console.error('❌ Timestamp-based scanning failed:', timestampScanError.message);
        }
        
        // PATTERN 6: Broad conversion scan as final fallback
        if (allConversionKeys.length < 50) {
            console.log('🚨 Low conversion count detected, performing broad scan...');
            try {
                let cursor = '0';
                let broadKeys = [];
                
                do {
                    const result = await redis(`scan/${cursor}/match/*conv*/count/1000`);
                    if (result.result && result.result[1]) {
                        cursor = result.result[0];
                        const keys = result.result[1];
                        // Filter to only conversion-related keys
                        const convKeys = keys.filter(key => 
                            key.toLowerCase().includes('conv') || 
                            key.toLowerCase().includes('purchase') || 
                            key.toLowerCase().includes('order') ||
                            key.toLowerCase().includes('sale')
                        );
                        broadKeys = broadKeys.concat(convKeys);
                    }
                } while (cursor !== '0' && broadKeys.length < 5000);
                
                allConversionKeys = allConversionKeys.concat(broadKeys);
                console.log(`📊 Broad scan added ${broadKeys.length} additional conversion keys`);
                
            } catch (broadError) {
                console.error('❌ Broad conversion scan failed:', broadError.message);
            }
        }
        
        // Remove duplicates and validate
        const uniqueKeys = [...new Set(allConversionKeys)];
        console.log(`📊 CONVERSION SCAN RESULTS:`);
        console.log(`  Total keys before deduplication: ${allConversionKeys.length}`);
        console.log(`  Unique conversion keys found: ${uniqueKeys.length}`);
        console.log(`  Expected for June 26-28: 36 conversions`);
        console.log(`  Coverage: ${Math.round((uniqueKeys.length / 36) * 100)}% of expected conversions`);
        
        // Log sample keys for verification
        if (uniqueKeys.length > 0) {
            console.log('📝 Sample conversion keys found:');
            uniqueKeys.slice(0, 10).forEach((key, i) => {
                console.log(`  ${i+1}. ${key}`);
            });
            
            // Show keys that might be from June 26-28 specifically
            const june26_28Keys = uniqueKeys.filter(key => 
                key.includes('2025-06-26') || 
                key.includes('2025-06-27') || 
                key.includes('2025-06-28')
            );
            if (june26_28Keys.length > 0) {
                console.log(`🎯 Keys that appear to be from June 26-28: ${june26_28Keys.length}`);
                june26_28Keys.slice(0, 5).forEach((key, i) => {
                    console.log(`  June 26-28 #${i+1}: ${key}`);
                });
            }
        }
        
        return uniqueKeys;
        
    } catch (error) {
        console.error('❌ Comprehensive conversion key scanning failed:', error);
        return [];
    }
}

// 🔧 FIXED: Enhanced conversion data fetching with controlled concurrency to avoid Redis EBUSY
async function fetchConversionDataSafely(redis, conversionKeys) {
    console.log(`💰 Fetching conversion data for ${conversionKeys.length} keys...`);
    
    const allConversions = [];
    const batchSize = 50; // REDUCED from 500 to avoid connection overload
    const delayMs = 100; // Add small delay between batches
    
    try {
        // Process in smaller batches with delays
        for (let i = 0; i < conversionKeys.length; i += batchSize) {
            const batch = conversionKeys.slice(i, i + batchSize);
            const batchNumber = Math.floor(i/batchSize) + 1;
            const totalBatches = Math.ceil(conversionKeys.length/batchSize);
            
            console.log(`📦 Processing conversion batch ${batchNumber}/${totalBatches} (${batch.length} keys)`);
            
            try {
                // Process batch with reduced concurrency - 10 keys at a time
                const batchResults = [];
                
                for (let j = 0; j < batch.length; j += 10) {
                    const subBatch = batch.slice(j, j + 10);
                    
                    const subBatchResults = await Promise.all(
                        subBatch.map(async (key) => {
                            try {
                                const result = await redis(`get/${key}`);
                                if (result.result) {
                                    return {
                                        key: key,
                                        data: decodeURIComponent(result.result)
                                    };
                                }
                                return null;
                            } catch (e) {
                                console.warn(`⚠️ Failed to fetch conversion key: ${key.substring(0, 50)}...`);
                                return null;
                            }
                        })
                    );
                    
                    batchResults.push(...subBatchResults);
                    
                    // Small delay between sub-batches
                    if (j + 10 < batch.length) {
                        await sleep(50);
                    }
                }
                
                // Parse the results
                batchResults.forEach(item => {
                    if (item && item.data) {
                        try {
                            const parsed = JSON.parse(item.data);
                            
                            // Enhanced timestamp validation with fallback
                            if (!isValidTimestamp(parsed.timestamp)) {
                                console.warn(`⚠️ Invalid timestamp in conversion ${item.key}, using current time`);
                                parsed.timestamp = new Date().toISOString();
                            }
                            
                            // Add the key for debugging
                            parsed._redis_key = item.key;
                            
                            allConversions.push(parsed);
                        } catch (parseError) {
                            console.warn(`⚠️ Failed to parse conversion data from key: ${item.key}`);
                        }
                    }
                });
                
                // Delay between main batches to avoid overwhelming Redis
                if (i + batchSize < conversionKeys.length) {
                    await sleep(delayMs);
                }
                
            } catch (batchError) {
                console.error(`❌ Conversion batch ${batchNumber} failed:`, batchError);
                // Continue with next batch instead of failing completely
            }
        }
        
        console.log(`📊 Conversion data fetch complete: ${allConversions.length} conversions processed`);
        
        // Show June 26-28 conversions specifically
        const june26_28 = allConversions.filter(conv => {
            const convDate = new Date(conv.timestamp);
            return convDate >= new Date('2025-06-26T01:34:00-08:00') && 
                   convDate <= new Date('2025-06-28T09:37:00-08:00');
        });
        
        console.log(`🎯 CRITICAL: Conversions found for June 26-28 period: ${june26_28.length}`);
        console.log(`🎯 Expected: 36 conversions`);
        console.log(`🎯 Found: ${june26_28.length} conversions`);
        console.log(`🎯 Missing: ${36 - june26_28.length} conversions`);
        
        if (june26_28.length > 0) {
            console.log('📋 June 26-28 conversions found:');
            june26_28.forEach((conv, i) => {
                const orderTotal = parseFloat(conv.order_total) || 0;
                console.log(`  ${i+1}. ${new Date(conv.timestamp).toLocaleString()} - ${conv.email} - $${orderTotal} [${conv._redis_key}]`);
            });
        }
        
        return allConversions;
        
    } catch (error) {
        console.error('❌ Conversion data fetching failed:', error);
        return [];
    }
}

// 🔧 FIXED: Enhanced attribution data fetching with controlled concurrency
async function fetchAttributionDataSafely(redis, attributionKeys) {
    console.log(`📦 Fetching attribution data for ${attributionKeys.length} keys...`);
    
    const allPageViews = [];
    const batchSize = 100; // Smaller batches
    const delayMs = 100;
    
    try {
        if (attributionKeys.length > 5000) {
            console.log(`⚠️ Large dataset: ${attributionKeys.length} keys. Processing in controlled batches...`);
        }
        
        // Process in batches to avoid timeouts and connection overload
        for (let i = 0; i < attributionKeys.length; i += batchSize) {
            const batch = attributionKeys.slice(i, i + batchSize);
            const batchNumber = Math.floor(i/batchSize) + 1;
            const totalBatches = Math.ceil(attributionKeys.length/batchSize);
            
            console.log(`📦 Processing attribution batch ${batchNumber}/${totalBatches} (${batch.length} keys)`);
            
            try {
                // Process 20 keys at a time within each batch
                const batchResults = [];
                
                for (let j = 0; j < batch.length; j += 20) {
                    const subBatch = batch.slice(j, j + 20);
                    
                    const subBatchResults = await Promise.all(
                        subBatch.map(async (key) => {
                            try {
                                const result = await redis(`get/${key}`);
                                return result.result ? decodeURIComponent(result.result) : null;
                            } catch (e) {
                                return null;
                            }
                        })
                    );
                    
                    batchResults.push(...subBatchResults);
                    
                    // Small delay between sub-batches
                    if (j + 20 < batch.length) {
                        await sleep(25);
                    }
                }
                
                batchResults.forEach(item => {
                    if (item) {
                        try {
                            const parsed = JSON.parse(item);
                            
                            // Enhanced timestamp validation with fallback
                            if (!isValidTimestamp(parsed.timestamp)) {
                                console.warn('⚠️ Invalid timestamp found, using current time');
                                parsed.timestamp = new Date().toISOString();
                            }
                            
                            allPageViews.push(parsed);
                        } catch (parseError) {
                            console.warn('⚠️ Failed to parse attribution data:', parseError.message);
                        }
                    }
                });
                
                // Delay between main batches
                if (i + batchSize < attributionKeys.length) {
                    await sleep(delayMs);
                }
                
            } catch (batchError) {
                console.error(`❌ Attribution batch ${batchNumber} failed:`, batchError);
                // Continue with next batch
            }
        }
        
        console.log(`📊 Attribution data fetch complete: ${allPageViews.length} page views processed`);
        return allPageViews;
        
    } catch (error) {
        console.error('❌ Attribution data fetching failed:', error);
        return [];
    }
}

// Attribution health calculation
async function calculateAttributionHealth(redis) {
    try {
        const recentStatsResult = await redis('scan/0/match/conversions:*/count/100');
        const recentStats = [];
        
        if (recentStatsResult.result && recentStatsResult.result[1]) {
            const conversionKeys = recentStatsResult.result[1];
            
            for (const key of conversionKeys.slice(0, 50)) {
                try {
                    const result = await redis(`get/${key}`);
                    if (result.result) {
                        const data = JSON.parse(decodeURIComponent(result.result));
                        recentStats.push(data);
                    }
                } catch (e) {
                    // Skip invalid data
                }
            }
        }
        
        const totalAttempts = recentStats.length;
        const successfulAttempts = recentStats.filter(stat => 
            stat.attribution_found || stat.attribution_method || stat.attribution_score
        ).length;
        
        const successRate = totalAttempts > 0 ? 
            Math.round(successfulAttempts / totalAttempts * 100) : 85;
        
        const status = successRate >= 70 ? 'healthy' : (successRate >= 50 ? 'warning' : 'critical');
        
        return {
            status,
            success_rate: successRate,
            total_conversions: totalAttempts,
            attributed_conversions: successfulAttempts,
            timestamp: new Date().toISOString()
        };
    } catch (error) {
        console.error('❌ Attribution health calculation failed:', error);
        return {
            status: 'error',
            success_rate: 0,
            total_conversions: 0,
            attributed_conversions: 0,
            error: error.message,
            timestamp: new Date().toISOString()
        };
    }
}

// Attribution stats fetching
async function fetchAttributionStats(redis) {
    try {
        const result = await redis('scan/0/match/attribution_stats:*/count/100');
        const stats = [];
        
        if (result.result && result.result[1]) {
            const keys = result.result[1];
            
            for (const key of keys) {
                try {
                    const statResult = await redis(`get/${key}`);
                    if (statResult.result) {
                        const data = JSON.parse(decodeURIComponent(statResult.result));
                        stats.push(data);
                    }
                } catch (e) {
                    // Skip invalid entries
                }
            }
        }
        
        return stats;
    } catch (error) {
        console.error('❌ Attribution stats fetch failed:', error);
        return [];
    }
}

// Attribution summary calculation
function calculateAttributionSummary(attributionStats, conversions) {
    const totalConversions = conversions.length;
    const attributedConversions = conversions.filter(conv => 
        conv.attribution_found || conv.attribution_method || conv.attribution_score
    ).length;
    
    return {
        total_conversions: totalConversions,
        attributed_conversions: attributedConversions,
        attribution_rate: totalConversions > 0 ? 
            Math.round((attributedConversions / totalConversions) * 100) : 0
    };
}

// Filter application function
function applyFilters(data, filters) {
    let filtered = data;
    
    if (filters.start_date) {
        const startDate = new Date(filters.start_date);
        filtered = filtered.filter(item => {
            const safeTimestamp = safeProcessTimestamp(item.timestamp);
            try {
                const itemDate = new Date(safeTimestamp);
                return itemDate >= startDate;
            } catch (e) {
                return true;
            }
        });
    }
    
    if (filters.end_date) {
        const endDate = new Date(filters.end_date);
        endDate.setHours(23, 59, 59, 999);
        filtered = filtered.filter(item => {
            const safeTimestamp = safeProcessTimestamp(item.timestamp);
            try {
                const itemDate = new Date(safeTimestamp);
                return itemDate <= endDate;
            } catch (e) {
                return true;
            }
        });
    }
    
    if (filters.source) {
        filtered = filtered.filter(item => item.source === filters.source);
    }
    
    if (filters.campaign) {
        filtered = filtered.filter(item => 
            (item.utm_campaign || item.campaign) === filters.campaign
        );
    }
    
    return filtered;
}

// MAIN HANDLER WITH CORS FIX
const handler = async (event, context) => {
    // CRITICAL: CORS headers must be first
    const corsHeaders = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, X-API-Key, Authorization',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS, PUT, DELETE',
        'Access-Control-Max-Age': '86400',
    };

    // Handle OPTIONS preflight request IMMEDIATELY
    if (event.httpMethod === 'OPTIONS') {
        console.log('🔧 CORS preflight request received from:', event.headers.origin || 'unknown');
        return {
            statusCode: 200,
            headers: corsHeaders,
            body: JSON.stringify({ message: 'CORS preflight successful' })
        };
    }

    // Helper to create responses with CORS headers
    const createResponse = (statusCode, body) => ({
        statusCode,
        headers: corsHeaders,
        body: typeof body === 'string' ? body : JSON.stringify(body)
    });

    try {
        // API Key validation
        const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
        const validApiKey = process.env.OJOY_API_KEY;

        if (!apiKey || apiKey !== validApiKey) {
            console.log('❌ Unauthorized request - API key mismatch');
            return createResponse(401, { error: 'Unauthorized' });
        }

        // Redis setup
        const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
        const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;

        if (!redisUrl || !redisToken) {
            console.error('❌ Missing Redis configuration');
            return createResponse(500, { error: 'Server configuration error' });
        }

        const redis = async (command) => {
            try {
                const response = await fetch(`${redisUrl}/${command}`, {
                    headers: { Authorization: `Bearer ${redisToken}` }
                });
                return response.json();
            } catch (error) {
                console.error('Redis error:', error);
                throw new Error('Database connection failed');
            }
        };

        // Attribution Health Check Endpoint
        if (event.httpMethod === 'GET' && event.path === '/attribution-health') {
            try {
                console.log('🩺 Attribution health check requested');
                const healthMetrics = await calculateAttributionHealth(redis);
                
                if (healthMetrics.successRate < 70) {
                    console.warn(`🚨 ALERT: Attribution success rate dropped to ${healthMetrics.successRate}%`);
                }
                
                return createResponse(200, healthMetrics);
            } catch (error) {
                console.error('❌ Health check error:', error);
                return createResponse(500, { error: error.message });
            }
        }
        
        if (event.httpMethod === 'GET') {
            try {
                // Parse query parameters
                const { start_date, end_date, source, campaign, include_attribution_stats } = event.queryStringParameters || {};
                
                console.log(`📅 Analytics request: ${start_date || 'no start'} to ${end_date || 'no end'}`);
                
                // Get all keys from Redis using COMPREHENSIVE SCANNING
                let attributionKeys = [];
                let conversionKeys = [];
                
                try {
                    console.log('🔍 Starting comprehensive Redis key scanning...');
                    
                    // Get attribution keys
                    attributionKeys = await getComprehensiveAttributionKeys(redis);
                    console.log(`📊 Attribution scan found ${attributionKeys.length} attribution keys`);
                    
                    // 🔧 CRITICAL FIX: Use enhanced conversion key scanning
                    conversionKeys = await getConversionKeysEnhanced(redis);
                    console.log(`🔍 Enhanced conversion scan found ${conversionKeys.length} conversion keys`);
                    
                } catch (redisError) {
                    console.error('❌ Redis operation failed:', redisError);
                    attributionKeys = [];
                    conversionKeys = [];
                }
                
                // 🔧 FIXED: Fetch attribution data with controlled concurrency
                let allPageViews = [];
                if (attributionKeys.length > 0) {
                    try {
                        allPageViews = await fetchAttributionDataSafely(redis, attributionKeys);
                    } catch (attributionError) {
                        console.error('❌ Attribution data fetch error:', attributionError);
                        allPageViews = [];
                    }
                }
                
                // 🔧 CRITICAL FIX: Fetch conversion data using enhanced method with controlled concurrency
                let allConversions = [];
                if (conversionKeys.length > 0) {
                    try {
                        allConversions = await fetchConversionDataSafely(redis, conversionKeys);
                        
                        // Deduplicate conversions by email and timestamp
                        const uniqueConversions = [];
                        const seen = new Set();
                        
                        allConversions.forEach(conv => {
                            const key = `${conv.email || 'no-email'}_${conv.timestamp}`;
                            if (!seen.has(key)) {
                                seen.add(key);
                                uniqueConversions.push(conv);
                            }
                        });
                        
                        console.log(`📊 Conversions: ${allConversions.length} → ${uniqueConversions.length} after deduplication`);
                        allConversions = uniqueConversions;
                        
                    } catch (conversionError) {
                        console.error('❌ Conversion data fetch error:', conversionError);
                        allConversions = [];
                    }
                }
                
                console.log(`📊 Analytics query returned ${allPageViews.length} page views and ${allConversions.length} conversions`);
                
                // Apply filters
                let filteredConversions = applyFilters(allConversions, { start_date, end_date, source, campaign });
                let filteredPageViews = applyFilters(allPageViews, { start_date, end_date, source, campaign });
                
                console.log(`📊 After filtering: ${filteredPageViews.length} page views and ${filteredConversions.length} conversions`);
                
                // Include attribution stats if requested
                let attributionStatsData = null;
                if (include_attribution_stats === 'true') {
                    try {
                        console.log('📈 Including attribution stats in response...');
                        attributionStatsData = await fetchAttributionStats(redis);
                        console.log(`✅ Fetched ${attributionStatsData.length} attribution stat records`);
                    } catch (statsError) {
                        console.error('❌ Failed to fetch attribution stats:', statsError);
                        attributionStatsData = [];
                    }
                }
                
                // Calculate analytics
                const totalConversions = filteredConversions.length;
                const totalPageViews = filteredPageViews.length;
                
                const uniqueVisitorIPs = new Set();
                filteredPageViews.forEach(pv => {
                    if (pv.ip_address && pv.ip_address !== 'unknown') {
                        uniqueVisitorIPs.add(pv.ip_address);
                    }
                });
                const uniqueVisitors = uniqueVisitorIPs.size;
                
                const paidConversions = filteredConversions.filter(item => (parseFloat(item.order_total) || 0) > 0);
                const totalRevenue = filteredConversions.reduce((sum, item) => sum + (parseFloat(item.order_total) || 0), 0);
                const avgOrderValue = paidConversions.length > 0 ? totalRevenue / paidConversions.length : 0;
                const conversionRate = uniqueVisitors > 0 ? (totalConversions / uniqueVisitors) * 100 : 0;
                
                // Build response
                const response = {
                    page_views: filteredPageViews,
                    conversions: filteredConversions,
                    summary: {
                        total_page_views: totalPageViews,
                        unique_visitors: uniqueVisitors,
                        total_conversions: totalConversions,
                        conversion_rate: parseFloat(conversionRate.toFixed(2)),
                        total_revenue: parseFloat(totalRevenue.toFixed(2)),
                        avg_order_value: parseFloat(avgOrderValue.toFixed(2))
                    },
                    scanning_debug: {
                        attribution_keys_found: attributionKeys.length,
                        conversion_keys_found: conversionKeys.length,
                        june_26_28_conversions: allConversions.filter(conv => {
                            const convDate = new Date(conv.timestamp);
                            return convDate >= new Date('2025-06-26T01:34:00-08:00') && 
                                   convDate <= new Date('2025-06-28T09:37:00-08:00');
                        }).length,
                        expected_june_26_28: 36
                    }
                };
                
                // Add attribution stats to response if requested
                if (attributionStatsData !== null) {
                    response.attribution_stats = attributionStatsData;
                    response.attribution_summary = calculateAttributionSummary(attributionStatsData, filteredConversions);
                }
                
                return createResponse(200, response);
                
            } catch (error) {
                console.error('❌ Analytics GET error:', error);
                return createResponse(500, { error: error.message });
            }
        }
        
        if (event.httpMethod === 'POST') {
            try {
                const data = JSON.parse(event.body);
                
                if (!isValidTimestamp(data.timestamp)) {
                    data.timestamp = new Date().toISOString();
                }
                
                if (data.event_type === 'purchase' || data.event_type === 'conversion' || data.order_total !== undefined) {
                    const key = data.email ? 
                        `conversions:${data.email.replace(/[^a-zA-Z0-9]/g, '_')}:${Date.now()}` :
                        `conversions:${data.timestamp}:${Math.random()}`;
                    
                    await redis(`set/${key}/${encodeURIComponent(JSON.stringify(data))}`);
                    console.log(`✅ Stored conversion: ${data.email || 'no email'}`);
                } else {
                    const key = `pageviews:${data.timestamp}:${Math.random()}`;
                    await redis(`set/${key}/${encodeURIComponent(JSON.stringify(data))}`);
                    console.log(`✅ Stored page view: ${data.source} → ${data.landing_page}`);
                }
                
                return createResponse(200, { success: true });
                
            } catch (error) {
                console.error('❌ Analytics POST error:', error);
                return createResponse(500, { error: error.message });
            }
        }
        
        return createResponse(405, { error: 'Method not allowed' });
        
    } catch (error) {
        console.error('❌ Unexpected error:', error);
        return createResponse(500, { 
            error: 'Internal server error', 
            message: error.message,
            timestamp: new Date().toISOString()
        });
    }
};

module.exports = { handler };
