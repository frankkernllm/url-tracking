// File: netlify/functions/analytics-flexible.js
// 🎯 FLEXIBLE DATE RANGE VERSION - Respects start_date and end_date parameters
// Based on analytics.js but removes the hardcoded 48-hour limitation

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

// NEW: Calculate flexible time range from query parameters
function calculateFlexibleTimeRange(queryParams) {
    console.log('🕐 Calculating flexible time range from parameters...');
    
    let startDate, endDate;
    
    if (queryParams.start_date && queryParams.end_date) {
        // Use provided date range
        startDate = new Date(queryParams.start_date);
        startDate.setHours(0, 0, 0, 0);
        
        endDate = new Date(queryParams.end_date);
        endDate.setHours(23, 59, 59, 999);
        
        console.log(`📅 Using provided date range: ${startDate.toISOString()} to ${endDate.toISOString()}`);
    } else {
        // Fallback to last 7 days if no parameters provided
        endDate = new Date();
        startDate = new Date();
        startDate.setDate(endDate.getDate() - 7);
        
        console.log(`📅 No date range provided, using last 7 days: ${startDate.toISOString()} to ${endDate.toISOString()}`);
    }
    
    const result = {
        startDate: startDate,
        endDate: endDate,
        startTimestamp: startDate.getTime(),
        endTimestamp: endDate.getTime()
    };
    
    console.log(`🎯 Time range set: ${Math.round((endDate - startDate) / (1000 * 60 * 60 * 24))} days`);
    
    return result;
}

// DUAL-PATTERN ATTRIBUTION KEY SCANNING (copied from analytics.js)
async function getComprehensiveAttributionKeys(redis) {
    console.log('🔍 Starting DUAL PATTERN attribution key scanning...');
    
    let allAttributionKeys = [];
    let totalScanned = 0;
    
    try {
        // PATTERN 1: Traditional underscore format (attribution_*)
        console.log('📊 Scanning Pattern 1: attribution_* (underscore format)');
        
        const ipv6Prefixes = [
            '2001', '2002', '2400', '2600', '2601', '2602', '2603', '2604', 
            '2605', '2606', '2607', '2608', '2610', '2620', '2800', '2a00', '2a01'
        ];
        
        try {
            let cursor = '0';
            let underscoreKeys = [];
            
            do {
                const result = await redis(`scan/${cursor}/match/attribution_*/count/1000`);
                if (result.result && result.result[1]) {
                    cursor = result.result[0];
                    const keys = result.result[1];
                    underscoreKeys = underscoreKeys.concat(keys);
                    
                    if (underscoreKeys.length % 1000 === 0) {
                        console.log(`  📊 Pattern 1 progress: ${underscoreKeys.length} keys found`);
                    }
                } else {
                    break;
                }
            } while (cursor !== '0' && underscoreKeys.length < 10000);
            
            allAttributionKeys = allAttributionKeys.concat(underscoreKeys);
            totalScanned += underscoreKeys.length;
            console.log(`✅ Pattern 1 complete: ${underscoreKeys.length} underscore keys`);
            
        } catch (error) {
            console.warn('⚠️ Pattern 1 scanning failed:', error);
        }
        
        console.log(`📊 Total attribution keys found: ${allAttributionKeys.length}`);
        return allAttributionKeys;
        
    } catch (error) {
        console.error('❌ Attribution key scanning failed:', error);
        return [];
    }
}

// ENHANCED CONVERSION KEY SCANNING (copied from analytics.js)
async function getConversionKeysEnhanced(redis) {
    let allConversionKeys = [];
    let totalScanned = 0;
    
    console.log('🔍 Starting COMPREHENSIVE conversion key scan with COMPLETE cursor iteration...');
    
    try {
        // PATTERN 1: Standard conversions:* with COMPLETE cursor iteration (MAIN FIX)
        console.log('📊 Scanning Pattern 1: conversions:* (standard format)');
        try {
            let cursor = '0';
            let standardKeys = [];
            let iterations = 0;
            let totalKeysFound = 0;
            
            do {
                const result = await redis(`scan/${cursor}/match/conversions:*/count/1000`);
                if (result.result && result.result[1]) {
                    cursor = result.result[0];
                    const keys = result.result[1];
                    standardKeys = standardKeys.concat(keys);
                    totalKeysFound += keys.length;
                    iterations++;
                    
                    console.log(`✅ Batch ${iterations}: Found ${keys.length} conversion keys (total: ${totalKeysFound}, cursor: ${cursor})`);
                    
                    // More reasonable safety check - only break if we get an enormous number
                    if (totalKeysFound > 50000) {
                        console.warn('⚠️ Breaking after 50,000 keys for memory safety');
                        break;
                    }
                    
                    // Add small delay every 10 iterations to prevent Redis overload
                    if (iterations % 10 === 0) {
                        await sleep(50);
                    }
                } else {
                    console.log(`⚠️ No results from SCAN at cursor ${cursor}, stopping iteration`);
                    break;
                }
            } while (cursor !== '0');
            
            allConversionKeys = allConversionKeys.concat(standardKeys);
            totalScanned += standardKeys.length;
            
            console.log(`🎯 Pattern 1 COMPLETE: ${standardKeys.length} standard conversion keys found in ${iterations} iterations`);
            
        } catch (error) {
            console.error('❌ Pattern 1 scanning failed:', error);
        }
        
        // PATTERN 2: Additional conversion patterns
        const additionalPatterns = [
            'conversion_*',
            'track_*',
            'webhook_*'
        ];
        
        for (const pattern of additionalPatterns) {
            try {
                console.log(`📊 Scanning additional pattern: ${pattern}`);
                let cursor = '0';
                let patternKeys = [];
                
                do {
                    const result = await redis(`scan/${cursor}/match/${pattern}/count/1000`);
                    if (result.result && result.result[1]) {
                        cursor = result.result[0];
                        const keys = result.result[1];
                        patternKeys = patternKeys.concat(keys);
                    } else {
                        break;
                    }
                } while (cursor !== '0' && patternKeys.length < 5000);
                
                allConversionKeys = allConversionKeys.concat(patternKeys);
                totalScanned += patternKeys.length;
                console.log(`✅ Pattern ${pattern}: ${patternKeys.length} keys found`);
                
            } catch (error) {
                console.warn(`⚠️ Pattern ${pattern} failed:`, error);
            }
        }
        
        // Remove duplicates
        const uniqueKeys = [...new Set(allConversionKeys)];
        console.log(`📊 Conversion scanning complete: ${uniqueKeys.length} unique keys (${allConversionKeys.length - uniqueKeys.length} duplicates removed)`);
        
        return uniqueKeys;
        
    } catch (error) {
        console.error('❌ Enhanced conversion key scanning failed:', error);
        return [];
    }
}

// Enhanced conversion data fetching with controlled concurrency
async function fetchConversionDataSafely(redis, conversionKeys, startTimestamp, endTimestamp) {
    console.log(`💰 Fetching conversion data for ${conversionKeys.length} keys...`);
    
    const allConversions = [];
    const batchSize = 50;
    const delayMs = 100;
    
    try {
        for (let i = 0; i < conversionKeys.length; i += batchSize) {
            const batch = conversionKeys.slice(i, i + batchSize);
            const batchNumber = Math.floor(i/batchSize) + 1;
            const totalBatches = Math.ceil(conversionKeys.length/batchSize);
            
            console.log(`📦 Processing conversion batch ${batchNumber}/${totalBatches} (${batch.length} keys)`);
            
            try {
                const batchResults = [];
                
                // Process in smaller sub-batches to avoid overwhelming Redis
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
                    
                    if (j + 10 < batch.length) {
                        await sleep(50);
                    }
                }
                
                // Parse and filter the results
                let validInBatch = 0;
                let filteredInBatch = 0;
                
                batchResults.forEach(item => {
                    if (item && item.data) {
                        try {
                            const parsed = JSON.parse(item.data);
                            
                            // Enhanced timestamp validation with fallback
                            if (!isValidTimestamp(parsed.timestamp)) {
                                console.warn(`⚠️ Invalid timestamp in conversion ${item.key}, using current time`);
                                parsed.timestamp = new Date().toISOString();
                            }
                            
                            // Date filtering - only include conversions within our time range
                            const conversionTime = new Date(parsed.timestamp).getTime();
                            if (conversionTime >= startTimestamp && conversionTime <= endTimestamp) {
                                parsed._redis_key = item.key;
                                allConversions.push(parsed);
                                validInBatch++;
                            } else {
                                filteredInBatch++;
                            }
                            
                        } catch (parseError) {
                            console.warn(`⚠️ Failed to parse conversion data from key: ${item.key}`);
                        }
                    }
                });
                
                console.log(`  Batch ${batchNumber}: ${validInBatch} valid, ${filteredInBatch} filtered by date`);
                
                // Delay between main batches
                if (i + batchSize < conversionKeys.length) {
                    await sleep(delayMs);
                }
                
            } catch (batchError) {
                console.error(`❌ Conversion batch ${batchNumber} failed:`, batchError);
            }
        }
        
        console.log(`📊 Conversion fetch complete: ${allConversions.length} valid conversions`);
        return allConversions;
        
    } catch (error) {
        console.error('❌ Conversion data fetching failed:', error);
        return [];
    }
}

// Enhanced attribution data fetching
async function fetchAttributionDataSafely(redis, attributionKeys, startTimestamp, endTimestamp) {
    console.log(`📦 Fetching attribution data for ${attributionKeys.length} keys...`);
    
    const allPageViews = [];
    const batchSize = 100;
    const delayMs = 100;
    
    try {
        if (attributionKeys.length > 5000) {
            console.log(`⚠️ Large dataset: ${attributionKeys.length} keys. Processing in batches...`);
        }
        
        for (let i = 0; i < attributionKeys.length; i += batchSize) {
            const batch = attributionKeys.slice(i, i + batchSize);
            const batchNumber = Math.floor(i/batchSize) + 1;
            const totalBatches = Math.ceil(attributionKeys.length/batchSize);
            
            console.log(`✅ Batch ${batchNumber}/${totalBatches}: processing ${batch.length} records`);
            
            try {
                const batchResults = await Promise.all(
                    batch.map(async (key) => {
                        try {
                            const result = await redis(`get/${key}`);
                            if (result.result) {
                                const parsed = JSON.parse(decodeURIComponent(result.result));
                                
                                // Enhanced timestamp validation
                                if (!isValidTimestamp(parsed.timestamp)) {
                                    console.warn(`⚠️ Invalid timestamp in attribution ${key}, skipping`);
                                    return null;
                                }
                                
                                // Date filtering
                                const attributionTime = new Date(parsed.timestamp).getTime();
                                if (attributionTime >= startTimestamp && attributionTime <= endTimestamp) {
                                    parsed._redis_key = key;
                                    return parsed;
                                }
                            }
                            return null;
                        } catch (error) {
                            console.warn(`⚠️ Failed to process attribution key: ${key}`);
                            return null;
                        }
                    })
                );
                
                const validResults = batchResults.filter(item => item !== null);
                allPageViews.push(...validResults);
                console.log(`  Batch ${batchNumber}: ${validResults.length} valid page views`);
                
                // Delay between batches
                if (i + batchSize < attributionKeys.length) {
                    await sleep(delayMs);
                }
                
            } catch (batchError) {
                console.error(`❌ Attribution batch ${batchNumber} failed:`, batchError);
            }
        }
        
        console.log(`📊 Successfully parsed ${allPageViews.length} page views`);
        return allPageViews;
        
    } catch (error) {
        console.error('❌ Attribution data fetching failed:', error);
        return [];
    }
}

// MAIN HANDLER WITH CORS FIX
const handler = async (event, context) => {
    const startTime = Date.now();
    
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
        console.log('🚀 Analytics-flexible function started - GET request');
        console.log('📋 Query parameters:', event.queryStringParameters);
        
        // Initialize Redis connection
        const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
        const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
        
        if (!redisUrl || !redisToken) {
            console.error('❌ Redis configuration missing');
            return createResponse(500, { error: 'Redis configuration error' });
        }
        
        const redis = async (command) => {
            const response = await fetch(`${redisUrl}/${command}`, {
                headers: { Authorization: `Bearer ${redisToken}` }
            });
            return response.json();
        };

        if (event.httpMethod === 'GET') {
            try {
                // 🎯 NEW: Use flexible time range instead of hardcoded 48 hours
                const timeRange = calculateFlexibleTimeRange(event.queryStringParameters || {});
                
                console.log('🔍 Starting comprehensive Redis scanning...');
                
                // STEP 1: Get attribution keys with dual pattern scanning
                console.log('🔍 Starting dual pattern attribution scanning...');
                const attributionKeys = await getComprehensiveAttributionKeys(redis);
                console.log(`📊 Attribution scan: ${attributionKeys.length} keys found`);
                
                // STEP 2: Get conversion keys with FIXED complete cursor iteration
                console.log('🔍 Starting comprehensive conversion scanning...');
                const conversionKeys = await getConversionKeysEnhanced(redis);
                console.log(`📊 Conversion scan: ${conversionKeys.length} keys found`);
                
                // STEP 3: Fetch attribution data with flexible date range
                const allPageViews = await fetchAttributionDataSafely(
                    redis, 
                    attributionKeys, 
                    timeRange.startTimestamp, 
                    timeRange.endTimestamp
                );
                console.log(`📊 Attribution fetch complete: ${allPageViews.length} valid page views`);
                
                // STEP 4: Fetch conversion data with flexible date range
                const allConversions = await fetchConversionDataSafely(
                    redis, 
                    conversionKeys, 
                    timeRange.startTimestamp, 
                    timeRange.endTimestamp
                );
                console.log(`📊 Conversion fetch complete: ${allConversions.length} valid conversions`);
                
                // Calculate analytics
                const totalConversions = allConversions.length;
                const totalPageViews = allPageViews.length;
                
                const uniqueVisitorIPs = new Set();
                allPageViews.forEach(pv => {
                    if (pv.ip_address && pv.ip_address !== 'unknown') {
                        uniqueVisitorIPs.add(pv.ip_address);
                    }
                });
                const uniqueVisitors = uniqueVisitorIPs.size;
                
                const totalRevenue = allConversions.reduce((sum, item) => sum + (parseFloat(item.order_total) || 0), 0);
                const conversionRate = uniqueVisitors > 0 ? 
                    ((totalConversions / uniqueVisitors) * 100).toFixed(2) : '0.00';
                
                const executionTime = Date.now() - startTime;
                
                // Final results
                console.log(`📊 FINAL RESULTS: ${totalPageViews} page views, ${totalConversions} conversions`);
                console.log(`🎯 Date range: ${timeRange.startDate.toLocaleDateString()} - ${timeRange.endDate.toLocaleDateString()}`);
                
                const response = {
                    // Dashboard expects arrays directly (not nested under 'data')
                    page_views: allPageViews,           // Array - what dashboard expects
                    conversions: allConversions,        // Array - what dashboard expects
                    total_page_views: totalPageViews,   // Number - for summary stats
                    total_conversions: totalConversions, // Number - for summary stats
                    unique_visitors: uniqueVisitors,
                    total_revenue: totalRevenue.toFixed(2),
                    conversion_rate: conversionRate,
                    date_range: {
                        start: timeRange.startDate.toISOString(),
                        end: timeRange.endDate.toISOString(),
                        days: Math.round((timeRange.endDate - timeRange.startDate) / (1000 * 60 * 60 * 24)),
                        calculation_method: 'flexible_date_range_from_parameters'
                    },
                    processing_stats: {
                        execution_time_ms: executionTime,
                        attribution_keys_scanned: attributionKeys.length,
                        conversion_keys_scanned: conversionKeys.length,
                        flexible_date_range: true,
                        hardcoded_limitation_removed: true
                    }
                };
                
                console.log(`✅ Response ready: ${totalPageViews} views, ${totalConversions} conversions (${executionTime}ms)`);
                return createResponse(200, response);
                
            } catch (error) {
                console.error('❌ Analytics GET error:', error);
                return createResponse(500, { 
                    error: 'Analytics processing failed', 
                    message: error.message,
                    execution_time: Date.now() - startTime
                });
            }
        }
        
        return createResponse(405, { error: 'Method not allowed' });
        
    } catch (error) {
        console.error('❌ Unexpected error:', error);
        return createResponse(500, { 
            error: 'Internal server error', 
            message: error.message,
            timestamp: new Date().toISOString(),
            execution_time: Date.now() - startTime
        });
    }
};

module.exports = { handler };
