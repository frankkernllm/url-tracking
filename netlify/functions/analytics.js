// File: netlify/functions/analytics.js
// 🔧 FIXED VERSION - Pacific Time 7-Day Rolling Window + Comprehensive Scanning six o nine on june28
// Automatically retrieves the past 7 days from current Pacific Time moment

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

// Pacific Time date calculation for rolling 7-day window
function calculatePacificTimeRange() {
    // Get current time in Pacific Time
    const now = new Date();
    const pacificOptions = { timeZone: 'America/Los_Angeles' };
    
    // Calculate Pacific Time current moment
    const nowPacific = new Date(now.toLocaleString('en-US', pacificOptions));
    
    // Calculate 7 days ago in Pacific Time
    const sevenDaysAgo = new Date(nowPacific);
    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
    
    console.log(`🕐 Pacific Time Range Calculated:`);
    console.log(`   Current Pacific Time: ${nowPacific.toISOString()}`);
    console.log(`   7 Days Ago Pacific: ${sevenDaysAgo.toISOString()}`);
    
    return {
        startDate: sevenDaysAgo,
        endDate: nowPacific,
        startTimestamp: sevenDaysAgo.getTime(),
        endTimestamp: nowPacific.getTime()
    };
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
            '260d', '260e', '260f', '2610', '2620', '2630', '2640', '2650'
        ];
        
        for (const prefix of ipv6Prefixes) {
            try {
                const result = await redis(`scan/0/match/attribution_${prefix}*/count/1000`);
                if (result.result && result.result[1]) {
                    const keys = result.result[1];
                    allAttributionKeys = allAttributionKeys.concat(keys);
                    totalScanned += keys.length;
                    if (keys.length > 0) {
                        console.log(`✅ Found ${keys.length} keys for prefix ${prefix}`);
                    }
                }
                
                // Small delay to prevent overwhelming Redis
                await sleep(10);
                
            } catch (error) {
                console.warn(`⚠️ Prefix ${prefix} scan failed:`, error.message);
            }
        }
        
        console.log(`📊 Pattern 1 complete: ${totalScanned} underscore keys found`);
        
        // PATTERN 2: Colon format (attribution:*) - THE MISSING DATA!
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
                
                // Safety limit
                if (colonKeys.length > 15000) {
                    console.log(`⚠️ Colon format scan limit reached: ${colonKeys.length} keys`);
                    break;
                }
                
                await sleep(10);
                
            } while (cursor !== '0');
            
            allAttributionKeys = allAttributionKeys.concat(colonKeys);
            totalScanned += colonKeys.length;
            console.log(`🎯 CRITICAL: Total colon-format keys found: ${colonKeys.length}`);
            
        } catch (error) {
            console.error('❌ Colon format scanning failed:', error);
        }
        
        // Remove duplicates and validate
        const uniqueKeys = [...new Set(allAttributionKeys)];
        console.log(`📊 Attribution scan found ${uniqueKeys.length} unique keys from both patterns`);
        
        const underscoreKeys = uniqueKeys.filter(key => key.startsWith('attribution_')).length;
        const colonKeys = uniqueKeys.filter(key => key.startsWith('attribution:')).length;
        console.log(`🎯 PATTERN DISTRIBUTION: ${underscoreKeys} underscore, ${colonKeys} colon format`);
        
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
                    iterations++;
                    console.log(`✅ Conversion scan iteration ${iterations}: found ${keys.length} keys (cursor: ${cursor})`);
                }
                
                // Safety limits
                if (standardKeys.length > 20000 || iterations > 50) {
                    console.log(`⚠️ Conversion scan safety limit: ${standardKeys.length} keys, ${iterations} iterations`);
                    break;
                }
                
                await sleep(50); // Longer delay for conversion scanning
                
            } while (cursor !== '0');
            
            allConversionKeys = allConversionKeys.concat(standardKeys);
            totalScanned += standardKeys.length;
            console.log(`📊 Standard conversions pattern complete: ${standardKeys.length} keys found`);
            
        } catch (error) {
            console.error('❌ Standard conversion scanning failed:', error);
        }
        
        // PATTERN 2: Alternative conversion patterns
        const alternativePatterns = ['conversion:*', 'purchase:*', 'trial:*', 'subscription:*'];
        
        for (const pattern of alternativePatterns) {
            try {
                const result = await redis(`scan/0/match/${pattern}/count/1000`);
                if (result.result && result.result[1] && result.result[1].length > 0) {
                    const keys = result.result[1];
                    allConversionKeys = allConversionKeys.concat(keys);
                    totalScanned += keys.length;
                    console.log(`✅ Alternative pattern ${pattern}: ${keys.length} keys found`);
                }
            } catch (error) {
                console.warn(`⚠️ Alternative pattern ${pattern} scan failed:`, error.message);
            }
        }
        
        // Remove duplicates
        const uniqueKeys = [...new Set(allConversionKeys)];
        console.log(`📊 Conversion scan complete: ${uniqueKeys.length} unique conversion keys found`);
        
        return uniqueKeys;
        
    } catch (error) {
        console.error('❌ Conversion key scanning failed:', error);
        return [];
    }
}

// 🔧 FIXED: Enhanced conversion data fetching with date filtering
async function fetchConversionDataSafely(redis, conversionKeys, startTimestamp, endTimestamp) {
    console.log(`📦 Fetching conversion data for ${conversionKeys.length} keys...`);
    console.log(`📅 Date filter: ${new Date(startTimestamp).toISOString()} to ${new Date(endTimestamp).toISOString()}`);
    
    const allConversions = [];
    const batchSize = 100;
    const delayMs = 100;
    let nilCount = 0;
    let parseErrors = 0;
    let validConversions = 0;
    let dateFilteredOut = 0;
    
    try {
        for (let i = 0; i < conversionKeys.length; i += batchSize) {
            const batch = conversionKeys.slice(i, i + batchSize);
            const batchNumber = Math.floor(i / batchSize) + 1;
            
            console.log(`📦 Processing conversion batch ${batchNumber}: ${batch.length} keys`);
            
            try {
                const batchResults = await Promise.all(
                    batch.map(async (key) => {
                        try {
                            const result = await redis(`get/${key}`);
                            return {
                                key: key,
                                data: result.result ? decodeURIComponent(result.result) : null
                            };
                        } catch (e) {
                            return { key: key, data: null, error: e.message };
                        }
                    })
                );
                
                batchResults.forEach(item => {
                    if (!item.data) {
                        nilCount++;
                        console.warn(`⚠️ Key returned nil: ${item.key}`);
                        return;
                    }
                    
                    try {
                        const parsed = JSON.parse(item.data);
                        
                        // Enhanced timestamp validation
                        if (!isValidTimestamp(parsed.timestamp)) {
                            console.warn(`⚠️ Invalid timestamp in conversion: ${item.key}, timestamp: ${parsed.timestamp}`);
                            parsed.timestamp = new Date().toISOString();
                        }
                        
                        // Apply Pacific Time date range filter
                        const conversionTimestamp = new Date(parsed.timestamp).getTime();
                        if (conversionTimestamp < startTimestamp || conversionTimestamp > endTimestamp) {
                            dateFilteredOut++;
                            return; // Skip conversions outside date range
                        }
                        
                        // Add the key for debugging
                        parsed._redis_key = item.key;
                        
                        allConversions.push(parsed);
                        validConversions++;
                    } catch (parseError) {
                        parseErrors++;
                        console.warn(`⚠️ Failed to parse conversion data from key: ${item.key}`);
                    }
                });
                
                // Delay between main batches to avoid overwhelming Redis
                if (i + batchSize < conversionKeys.length) {
                    await sleep(delayMs);
                }
                
            } catch (batchError) {
                console.error(`❌ Conversion batch ${batchNumber} failed:`, batchError);
            }
        }
        
        console.log(`📊 Conversion data fetch complete:`);
        console.log(`   ✅ Valid conversions in date range: ${validConversions}`);
        console.log(`   📅 Date filtered out: ${dateFilteredOut}`);
        console.log(`   ⚠️ Nil/missing data: ${nilCount}`);
        console.log(`   ❌ Parse errors: ${parseErrors}`);
        
        return allConversions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
        
    } catch (error) {
        console.error('❌ Conversion data fetching failed:', error);
        return [];
    }
}

// 🔧 FIXED: Enhanced attribution data fetching with date filtering
async function fetchAttributionDataSafely(redis, attributionKeys, startTimestamp, endTimestamp) {
    console.log(`📦 Fetching attribution data for ${attributionKeys.length} keys...`);
    console.log(`📅 Date filter: ${new Date(startTimestamp).toISOString()} to ${new Date(endTimestamp).toISOString()}`);
    
    const allPageViews = [];
    const batchSize = 100;
    const delayMs = 100;
    let nilCount = 0;
    let parseErrors = 0;
    let validPageViews = 0;
    let dateFilteredOut = 0;
    
    try {
        if (attributionKeys.length > 5000) {
            console.log(`⚠️ Large attribution dataset: ${attributionKeys.length} keys. Processing with delays...`);
        }
        
        for (let i = 0; i < attributionKeys.length; i += batchSize) {
            const batch = attributionKeys.slice(i, i + batchSize);
            const batchNumber = Math.floor(i / batchSize) + 1;
            
            try {
                const batchResults = await Promise.all(
                    batch.map(async (key) => {
                        try {
                            const result = await redis(`get/${key}`);
                            return {
                                key: key,
                                data: result.result ? decodeURIComponent(result.result) : null
                            };
                        } catch (e) {
                            return { key: key, data: null, error: e.message };
                        }
                    })
                );
                
                batchResults.forEach(item => {
                    if (!item.data) {
                        nilCount++;
                        return;
                    }
                    
                    try {
                        const parsed = JSON.parse(item.data);
                        
                        // Enhanced timestamp validation
                        if (!isValidTimestamp(parsed.timestamp)) {
                            console.warn(`⚠️ Invalid timestamp in attribution: ${item.key}, timestamp: ${parsed.timestamp}`);
                            parsed.timestamp = new Date().toISOString();
                        }
                        
                        // Apply Pacific Time date range filter
                        const attributionTimestamp = new Date(parsed.timestamp).getTime();
                        if (attributionTimestamp < startTimestamp || attributionTimestamp > endTimestamp) {
                            dateFilteredOut++;
                            return; // Skip attribution outside date range
                        }
                        
                        allPageViews.push(parsed);
                        validPageViews++;
                    } catch (parseError) {
                        parseErrors++;
                    }
                });
                
                if (i + batchSize < attributionKeys.length) {
                    await sleep(delayMs);
                }
                
            } catch (batchError) {
                console.error(`❌ Attribution batch ${batchNumber} failed:`, batchError);
            }
        }
        
        console.log(`📊 Attribution data fetch complete:`);
        console.log(`   ✅ Valid page views in date range: ${validPageViews}`);
        console.log(`   📅 Date filtered out: ${dateFilteredOut}`);
        console.log(`   ⚠️ Nil/missing data: ${nilCount}`);
        console.log(`   ❌ Parse errors: ${parseErrors}`);
        
        return allPageViews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
        
    } catch (error) {
        console.error('❌ Attribution data fetching failed:', error);
        return [];
    }
}

// Apply filters to data
function applyFilters(data, filters) {
    if (!filters) return data;
    
    return data.filter(item => {
        if (filters.source && item.source !== filters.source) return false;
        if (filters.campaign && item.utm_campaign !== filters.campaign) return false;
        return true;
    });
}

// Generate attribution statistics
function generateAttributionStats(pageViews) {
    const stats = {
        total_sessions: pageViews.length,
        sources: {},
        source_types: {},
        top_landing_pages: {},
        geographic_distribution: {},
        device_types: {}
    };
    
    pageViews.forEach(pv => {
        // Source counting
        const source = pv.source || 'unknown';
        stats.sources[source] = (stats.sources[source] || 0) + 1;
        
        // Source type counting
        const sourceType = pv.source_type || 'unknown';
        stats.source_types[sourceType] = (stats.source_types[sourceType] || 0) + 1;
        
        // Landing page counting
        const landingPage = pv.landing_page || 'unknown';
        stats.top_landing_pages[landingPage] = (stats.top_landing_pages[landingPage] || 0) + 1;
    });
    
    return stats;
}

// Create standardized response
function createResponse(statusCode, body) {
    return {
        statusCode,
        headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(body)
    };
}

// Redis helper function
const redis = async (command) => {
    const url = `${process.env.UPSTASH_REDIS_REST_URL}/${command}`;
    const response = await fetch(url, {
        headers: {
            'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}`,
            'Content-Type': 'application/json'
        }
    });
    return await response.json();
};

// Main handler function
const handler = async (event, context) => {
    console.log(`🚀 Analytics function started - ${event.httpMethod} request`);
    
    // Handle CORS preflight
    if (event.httpMethod === 'OPTIONS') {
        return createResponse(200, {});
    }
    
    try {
        if (event.httpMethod === 'GET') {
            const startTime = Date.now();
            
            try {
                // 🔧 FIXED: Ignore frontend date parameters and calculate Pacific Time 7-day window
                const pacificTimeRange = calculatePacificTimeRange();
                console.log(`📅 Using Pacific Time 7-day rolling window (ignoring frontend date parameters)`);
                
                // Parse other query parameters (but ignore dates)
                const { source, campaign, include_attribution_stats } = event.queryStringParameters || {};
                
                // Get all keys from Redis using COMPREHENSIVE SCANNING
                let attributionKeys = [];
                let conversionKeys = [];
                
                try {
                    console.log('🔍 Starting comprehensive Redis key scanning...');
                    
                    // Get attribution keys with dual pattern scanning
                    attributionKeys = await getComprehensiveAttributionKeys(redis);
                    console.log(`📊 Attribution scan found ${attributionKeys.length} attribution keys`);
                    
                    // Get conversion keys with enhanced scanning
                    conversionKeys = await getConversionKeysEnhanced(redis);
                    console.log(`🔍 Enhanced conversion scan found ${conversionKeys.length} conversion keys`);
                    
                } catch (redisError) {
                    console.error('❌ Redis scanning failed:', redisError);
                    attributionKeys = [];
                    conversionKeys = [];
                }
                
                // Fetch data with Pacific Time date filtering
                let allPageViews = [];
                let allConversions = [];
                
                if (attributionKeys.length > 0) {
                    try {
                        allPageViews = await fetchAttributionDataSafely(
                            redis, 
                            attributionKeys, 
                            pacificTimeRange.startTimestamp, 
                            pacificTimeRange.endTimestamp
                        );
                    } catch (attributionError) {
                        console.error('❌ Attribution data fetch failed:', attributionError);
                        allPageViews = [];
                    }
                }
                
                if (conversionKeys.length > 0) {
                    try {
                        allConversions = await fetchConversionDataSafely(
                            redis, 
                            conversionKeys, 
                            pacificTimeRange.startTimestamp, 
                            pacificTimeRange.endTimestamp
                        );
                    } catch (conversionError) {
                        console.error('❌ Conversion data fetch failed:', conversionError);
                        allConversions = [];
                    }
                }
                
                // Apply additional filters (source, campaign)
                let filteredPageViews = applyFilters(allPageViews, { source, campaign });
                let filteredConversions = applyFilters(allConversions, { source, campaign });
                
                console.log(`📊 Pacific Time filtered results: ${filteredPageViews.length} page views, ${filteredConversions.length} conversions`);
                
                // 🔧 DIAGNOSTIC: Check if we're finding the expected 90 conversions
                console.log(`🎯 CONVERSION DIAGNOSTIC:`);
                console.log(`   Expected conversions (past 7 days): 90`);
                console.log(`   Found conversions: ${filteredConversions.length}`);
                console.log(`   Missing conversions: ${90 - filteredConversions.length}`);
                
                if (filteredConversions.length > 0) {
                    console.log(`📋 Sample conversions found (first 5):`);
                    filteredConversions.slice(0, 5).forEach((conv, i) => {
                        console.log(`   ${i+1}. ${new Date(conv.timestamp).toLocaleString()} - ${conv.email} - $${parseFloat(conv.order_total) || 0}`);
                    });
                }
                
                // Include attribution stats if requested
                let attributionStatsData = null;
                if (include_attribution_stats === 'true') {
                    try {
                        console.log('📈 Including attribution stats in response...');
                        attributionStatsData = generateAttributionStats(filteredPageViews);
                        console.log(`✅ Generated attribution stats for ${filteredPageViews.length} page views`);
                    } catch (statsError) {
                        console.error('❌ Failed to generate attribution stats:', statsError);
                        attributionStatsData = null;
                    }
                }
                
                // Calculate final analytics
                const totalConversions = filteredConversions.length;
                const totalPageViews = filteredPageViews.length;
                
                const uniqueVisitorIPs = new Set();
                filteredPageViews.forEach(pv => {
                    if (pv.ip_address && pv.ip_address !== 'unknown') {
                        uniqueVisitorIPs.add(pv.ip_address);
                    }
                });
                const uniqueVisitors = uniqueVisitorIPs.size;
                
                const totalRevenue = filteredConversions.reduce((sum, item) => sum + (parseFloat(item.order_total) || 0), 0);
                const conversionRate = uniqueVisitors > 0 ? ((totalConversions / uniqueVisitors) * 100).toFixed(2) : '0.00';
                
                const response = {
                    page_views: totalPageViews,
                    conversions: totalConversions,
                    unique_visitors: uniqueVisitors,
                    conversion_rate: conversionRate,
                    total_revenue: totalRevenue.toFixed(2),
                    data: {
                        page_views: filteredPageViews,
                        conversions: filteredConversions
                    },
                    date_range: {
                        start: pacificTimeRange.startDate.toISOString(),
                        end: pacificTimeRange.endDate.toISOString(),
                        timezone: 'America/Los_Angeles',
                        days: 7,
                        calculation_method: 'pacific_time_rolling_window'
                    },
                    processing_stats: {
                        execution_time_ms: Date.now() - startTime,
                        attribution_keys_scanned: attributionKeys.length,
                        conversion_keys_scanned: conversionKeys.length,
                        patterns_used: ['attribution_*', 'attribution:*', 'conversions:*'],
                        performance_optimized: true
                    }
                };
                
                if (attributionStatsData) {
                    response.attribution_stats = attributionStatsData;
                }
                
                console.log(`✅ Analytics response ready: ${totalPageViews} views, ${totalConversions} conversions (${Date.now() - startTime}ms)`);
                
                return createResponse(200, response);
                
            } catch (analyticsError) {
                console.error('❌ Analytics processing failed:', analyticsError);
                return createResponse(500, { 
                    error: 'Analytics processing failed', 
                    message: analyticsError.message 
                });
            }
        }
        
        if (event.httpMethod === 'POST') {
            try {
                const data = JSON.parse(event.body);
                
                if (data.conversions && data.conversions.length > 0) {
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
