// File: netlify/functions/analytics.js
// 🔧 SIMPLIFIED VERSION - Multi-Pattern Scanning for Missing Conversions
// Focuses on finding the missing June 22nd conversions without complex timeout handling

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

// Sleep function for batch delays
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Pacific Time date calculation for rolling 48-hour window
function calculatePacificTimeRange() {
    // Get current time in Pacific Time
    const now = new Date();
    const pacificOptions = { timeZone: 'America/Los_Angeles' };
    
    // Calculate Pacific Time current moment
    const nowPacific = new Date(now.toLocaleString('en-US', pacificOptions));
    
    // Calculate 48 hours ago in Pacific Time
    const fortyEightHoursAgo = new Date(nowPacific);
    fortyEightHoursAgo.setHours(fortyEightHoursAgo.getHours() - 48);
    
    console.log(`🕐 Pacific Time 48-Hour Range: ${fortyEightHoursAgo.toISOString()} to ${nowPacific.toISOString()}`);
    console.log(`📅 Expected conversions (June 27 12:20 AM - June 28 5:47 PM Pacific): 22`);
    
    return {
        startDate: fortyEightHoursAgo,
        endDate: nowPacific,
        startTimestamp: fortyEightHoursAgo.getTime(),
        endTimestamp: nowPacific.getTime()
    };
}

// DUAL-PATTERN ATTRIBUTION KEY SCANNING
async function getComprehensiveAttributionKeys(redis) {
    console.log('🔍 Starting dual pattern attribution scanning...');
    
    let allAttributionKeys = [];
    
    try {
        // PATTERN 1: IPv6 underscore format (attribution_*)
        console.log('📊 Scanning attribution_* patterns...');
        
        const ipv6Prefixes = ['2001', '2002', '2400', '2600', '2601', '2602', '2603', '2604', '2605'];
        
        for (const prefix of ipv6Prefixes) {
            try {
                const result = await redis(`scan/0/match/attribution_${prefix}*/count/1000`);
                if (result.result && result.result[1]) {
                    allAttributionKeys = allAttributionKeys.concat(result.result[1]);
                }
                await sleep(20);
            } catch (error) {
                console.warn(`⚠️ IPv6 prefix ${prefix} scan failed:`, error.message);
            }
        }
        
        // PATTERN 2: Colon format (attribution:*)
        console.log('📊 Scanning attribution:* patterns...');
        try {
            let cursor = '0';
            let iterations = 0;
            
            do {
                const result = await redis(`scan/${cursor}/match/attribution:*/count/1000`);
                if (result.result && result.result[1]) {
                    cursor = result.result[0];
                    allAttributionKeys = allAttributionKeys.concat(result.result[1]);
                    iterations++;
                }
                
                if (iterations > 20) break; // Safety limit
                await sleep(20);
                
            } while (cursor !== '0');
            
        } catch (error) {
            console.error('❌ Colon format scanning failed:', error);
        }
        
        const uniqueKeys = [...new Set(allAttributionKeys)];
        console.log(`📊 Found ${uniqueKeys.length} unique attribution keys`);
        
        return uniqueKeys;
        
    } catch (error) {
        console.error('❌ Attribution scanning failed:', error);
        return [];
    }
}

// COMPREHENSIVE CONVERSION KEY SCANNING - THE MAIN FIX
async function getConversionKeysEnhanced(redis) {
    console.log('🔍 Starting comprehensive conversion scanning...');
    
    let allConversionKeys = [];
    let scanResults = {
        standard: 0,
        alternative: 0,
        legacy: 0,
        total: 0
    };
    
    try {
        // PATTERN 1: Standard conversions:*
        console.log('📊 Scanning conversions:* (standard)...');
        try {
            let cursor = '0';
            let iterations = 0;
            
            do {
                const result = await redis(`scan/${cursor}/match/conversions:*/count/1000`);
                if (result.result && result.result[1]) {
                    cursor = result.result[0];
                    const keys = result.result[1];
                    allConversionKeys = allConversionKeys.concat(keys);
                    scanResults.standard += keys.length;
                    iterations++;
                }
                
                if (iterations > 50) break; // Safety limit
                await sleep(50);
                
            } while (cursor !== '0');
            
            console.log(`✅ Standard pattern: ${scanResults.standard} keys found`);
            
        } catch (error) {
            console.error('❌ Standard conversion scanning failed:', error.message);
        }
        
        // PATTERN 2: Alternative formats (THE MISSING JUNE 22ND DATA!)
        console.log('📊 Scanning alternative conversion formats...');
        try {
            const alternativePatterns = [
                'conversion:*',    // singular form  
                'purchase:*',      // purchase format (likely June 22nd)
                'order:*',         // order format
                'track:*',         // track format  
                'subscription:*',  // subscription format
                'trial:*',         // trial format
                'conv:*'           // abbreviated form
            ];
            
            for (const pattern of alternativePatterns) {
                try {
                    const result = await redis(`scan/0/match/${pattern}/count/1000`);
                    if (result.result && result.result[1] && result.result[1].length > 0) {
                        const keys = result.result[1];
                        allConversionKeys = allConversionKeys.concat(keys);
                        scanResults.alternative += keys.length;
                        console.log(`✅ Pattern ${pattern}: ${keys.length} keys found`);
                    }
                    await sleep(30);
                } catch (altError) {
                    console.warn(`⚠️ Pattern ${pattern} failed:`, altError.message);
                }
            }
            
        } catch (altScanError) {
            console.error('❌ Alternative format scanning failed:', altScanError.message);
        }
        
        // PATTERN 3: Email-based legacy patterns
        console.log('📊 Scanning email-based legacy patterns...');
        try {
            const emailPatterns = [
                '*alexislemosolmedo*',  // Specific from June 22nd logs
                '*gmail.com*',
                '*yahoo.com*',  
                '*@*'
            ];
            
            for (const pattern of emailPatterns) {
                try {
                    const result = await redis(`scan/0/match/${pattern}/count/500`);
                    if (result.result && result.result[1]) {
                        // Filter to only conversion-related keys
                        const convKeys = result.result[1].filter(key => 
                            key.includes('conversion') || 
                            key.includes('purchase') || 
                            key.includes('order') ||
                            key.includes('track') ||
                            key.includes('trial')
                        );
                        allConversionKeys = allConversionKeys.concat(convKeys);
                        scanResults.legacy += convKeys.length;
                        if (convKeys.length > 0) {
                            console.log(`✅ Email pattern ${pattern}: ${convKeys.length} conversion keys found`);
                        }
                    }
                    await sleep(40);
                } catch (emailError) {
                    console.warn(`⚠️ Email pattern ${pattern} failed:`, emailError.message);
                }
            }
            
        } catch (legacyScanError) {
            console.error('❌ Email-based scanning failed:', legacyScanError.message);
        }
        
        // Remove duplicates and log results
        const uniqueKeys = [...new Set(allConversionKeys)];
        scanResults.total = uniqueKeys.length;
        
        console.log(`📊 CONVERSION SCAN COMPLETE:`);
        console.log(`   🎯 Standard (conversions:*): ${scanResults.standard} keys`);
        console.log(`   🔄 Alternative formats: ${scanResults.alternative} keys`);
        console.log(`   📧 Legacy email-based: ${scanResults.legacy} keys`);
        console.log(`   ⚡ Total unique keys: ${scanResults.total}`);
        
        return uniqueKeys;
        
    } catch (error) {
        console.error('❌ Comprehensive conversion scanning failed:', error);
        return [];
    }
}

// Enhanced conversion data fetching
async function fetchConversionDataSafely(redis, conversionKeys, startTimestamp, endTimestamp) {
    console.log(`📦 Fetching ${conversionKeys.length} conversion keys...`);
    
    const allConversions = [];
    const batchSize = 100;
    let validConversions = 0;
    let dateFilteredOut = 0;
    let nilCount = 0;
    let parseErrors = 0;
    
    try {
        for (let i = 0; i < conversionKeys.length; i += batchSize) {
            const batch = conversionKeys.slice(i, i + batchSize);
            
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
                            return { key: key, data: null };
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
                            console.warn(`⚠️ Invalid timestamp: ${item.key}`);
                            parsed.timestamp = new Date().toISOString();
                        }
                        
                        // Apply Pacific Time date range filter
                        const conversionTimestamp = new Date(parsed.timestamp).getTime();
                        if (conversionTimestamp < startTimestamp || conversionTimestamp > endTimestamp) {
                            dateFilteredOut++;
                            return;
                        }
                        
                        // Add debug info
                        parsed._redis_key = item.key;
                        allConversions.push(parsed);
                        validConversions++;
                        
                    } catch (parseError) {
                        parseErrors++;
                    }
                });
                
                await sleep(100);
                
            } catch (batchError) {
                console.error(`❌ Batch ${Math.floor(i/batchSize) + 1} failed:`, batchError.message);
            }
        }
        
        console.log(`📊 Conversion fetch complete: ${validConversions} valid, ${dateFilteredOut} filtered, ${nilCount} nil, ${parseErrors} errors`);
        
        return allConversions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
        
    } catch (error) {
        console.error('❌ Conversion data fetch failed:', error);
        return allConversions;
    }
}

// Enhanced attribution data fetching
async function fetchAttributionDataSafely(redis, attributionKeys, startTimestamp, endTimestamp) {
    console.log(`📦 Fetching ${attributionKeys.length} attribution keys...`);
    
    const allPageViews = [];
    const batchSize = 100;
    let validPageViews = 0;
    
    try {
        for (let i = 0; i < attributionKeys.length; i += batchSize) {
            const batch = attributionKeys.slice(i, i + batchSize);
            
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
                            return { key: key, data: null };
                        }
                    })
                );
                
                batchResults.forEach(item => {
                    if (!item.data) return;
                    
                    try {
                        const parsed = JSON.parse(item.data);
                        
                        if (!isValidTimestamp(parsed.timestamp)) {
                            parsed.timestamp = new Date().toISOString();
                        }
                        
                        const attributionTimestamp = new Date(parsed.timestamp).getTime();
                        if (attributionTimestamp >= startTimestamp && attributionTimestamp <= endTimestamp) {
                            allPageViews.push(parsed);
                            validPageViews++;
                        }
                    } catch (parseError) {
                        // Skip malformed data
                    }
                });
                
                await sleep(100);
                
            } catch (batchError) {
                console.error(`❌ Attribution batch failed:`, batchError.message);
            }
        }
        
        console.log(`📊 Attribution fetch complete: ${validPageViews} valid page views`);
        
        return allPageViews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
        
    } catch (error) {
        console.error('❌ Attribution data fetch failed:', error);
        return allPageViews;
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
    try {
        const url = `${process.env.UPSTASH_REDIS_REST_URL}/${command}`;
        const response = await fetch(url, {
            headers: {
                'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}`,
                'Content-Type': 'application/json'
            }
        });
        
        if (!response.ok) {
            console.error(`❌ Redis HTTP error: ${response.status} ${response.statusText}`);
            throw new Error(`Redis HTTP error: ${response.status} ${response.statusText}`);
        }
        
        return await response.json();
        
    } catch (error) {
        console.error(`❌ Redis connection error:`, error.message);
        throw error;
    }
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
                // Calculate Pacific Time 48-hour window (ignore frontend dates)
                const pacificTimeRange = calculatePacificTimeRange();
                console.log(`📅 Using Pacific Time 48-hour rolling window`);
                
                // Parse other query parameters (but ignore dates)
                const { source, campaign, include_attribution_stats } = event.queryStringParameters || {};
                
                // Get all keys from Redis using comprehensive scanning
                let attributionKeys = [];
                let conversionKeys = [];
                
                try {
                    console.log('🔍 Starting comprehensive Redis scanning...');
                    
                    // Get attribution keys
                    attributionKeys = await getComprehensiveAttributionKeys(redis);
                    console.log(`📊 Attribution scan: ${attributionKeys.length} keys found`);
                    
                    // Get conversion keys with multi-pattern scanning
                    conversionKeys = await getConversionKeysEnhanced(redis);
                    console.log(`🔍 Conversion scan: ${conversionKeys.length} keys found`);
                    
                } catch (redisError) {
                    console.error('❌ Redis scanning failed:', redisError);
                    attributionKeys = [];
                    conversionKeys = [];
                }
                
                // Fetch data with Pacific Time filtering
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
                        console.error('❌ Attribution fetch failed:', attributionError);
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
                        console.error('❌ Conversion fetch failed:', conversionError);
                        allConversions = [];
                    }
                }
                
                // Apply additional filters
                let filteredPageViews = applyFilters(allPageViews, { source, campaign });
                let filteredConversions = applyFilters(allConversions, { source, campaign });
                
                console.log(`📊 FINAL RESULTS: ${filteredPageViews.length} page views, ${filteredConversions.length} conversions`);
                
                // Enhanced diagnostic for missing conversions
                console.log(`🎯 CONVERSION DISCOVERY REPORT:`);
                console.log(`   Expected: 22 conversions (June 27-28 Pacific)`);
                console.log(`   Found: ${filteredConversions.length} conversions`);
                console.log(`   Difference: ${filteredConversions.length - 22} (${filteredConversions.length >= 22 ? 'surplus' : 'missing'})`);
                console.log(`   Multi-pattern scanning improvement: ${filteredConversions.length - 24} vs 7-day baseline`);
                
                if (filteredConversions.length > 0) {
                    console.log(`📋 Sample found conversions:`);
                    filteredConversions.slice(0, 5).forEach((conv, i) => {
                        console.log(`   ${i+1}. ${new Date(conv.timestamp).toLocaleString()} - ${conv.email} - $${parseFloat(conv.order_total) || 0} [${conv._redis_key}]`);
                    });
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
                
                const executionTime = Date.now() - startTime;
                
                const response = {
                    // Dashboard expects arrays directly
                    page_views: filteredPageViews,
                    conversions: filteredConversions,
                    
                    // Summary statistics
                    total_page_views: totalPageViews,
                    total_conversions: totalConversions,
                    unique_visitors: uniqueVisitors,
                    conversion_rate: conversionRate,
                    total_revenue: totalRevenue.toFixed(2),
                    
                    // Processing info
                    date_range: {
                        start: pacificTimeRange.startDate.toISOString(),
                        end: pacificTimeRange.endDate.toISOString(),
                        timezone: 'America/Los_Angeles',
                        hours: 48,
                        calculation_method: 'pacific_time_48_hour_rolling_window'
                    },
                    processing_stats: {
                        execution_time_ms: executionTime,
                        attribution_keys_scanned: attributionKeys.length,
                        conversion_keys_scanned: conversionKeys.length,
                        patterns_used: [
                            'attribution_*', 'attribution:*',
                            'conversions:*', 'conversion:*', 'purchase:*', 'order:*', 'track:*',
                            'subscription:*', 'trial:*', '*gmail.com*', '*@*'
                        ],
                        multi_pattern_scanning: true
                    }
                };
                
                console.log(`✅ Response ready: ${totalPageViews} views, ${totalConversions} conversions (${executionTime}ms)`);
                
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
                
                const key = data.email ? 
                    `conversions:${data.email.replace(/[^a-zA-Z0-9]/g, '_')}:${Date.now()}` :
                    `conversions:${data.timestamp}:${Math.random()}`;
                
                await redis(`set/${key}/${encodeURIComponent(JSON.stringify(data))}`);
                console.log(`✅ Stored: ${data.email || 'no email'}`);
                
                return createResponse(200, { success: true });
                
            } catch (error) {
                console.error('❌ POST error:', error);
                return createResponse(500, { error: error.message });
            }
        }
        
        return createResponse(405, { error: 'Method not allowed' });
        
    } catch (error) {
        console.error('❌ Critical error:', error);
        return createResponse(500, { 
            error: 'Internal server error', 
            message: error.message 
        });
    }
};

module.exports = { handler };
