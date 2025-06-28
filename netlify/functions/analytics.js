// ============================================================================
// COMPLETE ANALYTICS.JS WITH DUAL PATTERN REDIS SCANNING FIX
// Deploy this file to: netlify/functions/analytics.js
// ============================================================================

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

// CRITICAL FIX: Dual-pattern attribution key scanning
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
            } while (cursor !== '0' && colonKeys.length < 15000); // Safety limit
            
            allAttributionKeys = allAttributionKeys.concat(colonKeys);
            totalScanned += colonKeys.length;
            console.log(`🎯 CRITICAL: Total colon-format keys found: ${colonKeys.length}`);
            
        } catch (error) {
            console.error('❌ Colon format scanning failed:', error);
        }
        
        // PATTERN 3: IPv4 underscore format
        console.log('📊 Scanning Pattern 3: IPv4 underscore format');
        
        const ipv4Patterns = [
            'attribution_192*', 'attribution_172*', 'attribution_10_*',
            'attribution_1_*', 'attribution_2_*', 'attribution_3_*',
            'attribution_4_*', 'attribution_5_*', 'attribution_6_*',
            'attribution_7_*', 'attribution_8_*', 'attribution_9_*'
        ];
        
        for (const pattern of ipv4Patterns) {
            try {
                const result = await redis(`scan/0/match/${pattern}/count/1000`);
                if (result.result && result.result[1] && result.result[1].length > 0) {
                    const keys = result.result[1];
                    allAttributionKeys = allAttributionKeys.concat(keys);
                    console.log(`✅ Found ${keys.length} IPv4 keys with pattern ${pattern}`);
                    totalScanned += keys.length;
                }
            } catch (error) {
                console.log(`⚠️ IPv4 pattern ${pattern} scan failed:`, error.message);
            }
        }
        
        // PATTERN 4: Enhanced attribution patterns
        console.log('📊 Scanning Pattern 4: Enhanced attribution patterns');
        
        const enhancedPatterns = [
            'attribution_fp_*', 'attribution_webgl_*', 'attribution_geo_*',
            'attribution_region_*', 'attribution_screen_*', 'attribution_hw_*'
        ];
        
        for (const pattern of enhancedPatterns) {
            try {
                const result = await redis(`scan/0/match/${pattern}/count/1000`);
                if (result.result && result.result[1] && result.result[1].length > 0) {
                    const keys = result.result[1];
                    allAttributionKeys = allAttributionKeys.concat(keys);
                    console.log(`✅ Found ${keys.length} enhanced keys with pattern ${pattern}`);
                    totalScanned += keys.length;
                }
            } catch (error) {
                console.log(`⚠️ Enhanced pattern ${pattern} scan failed:`, error.message);
            }
        }
        
        // FALLBACK: Broad scan if we still have low counts
        if (allAttributionKeys.length < 2000) {
            console.log('🚨 Low key count detected, performing broad attribution scan...');
            
            try {
                let cursor = '0';
                let broadKeys = [];
                
                do {
                    const result = await redis(`scan/${cursor}/match/attribution*/count/1000`);
                    if (result.result && result.result[1]) {
                        cursor = result.result[0];
                        const keys = result.result[1];
                        broadKeys = broadKeys.concat(keys);
                    }
                } while (cursor !== '0' && broadKeys.length < 20000); // Safety limit
                
                allAttributionKeys = allAttributionKeys.concat(broadKeys);
                console.log(`📊 Broad scan added ${broadKeys.length} additional keys`);
                totalScanned += broadKeys.length;
                
            } catch (broadError) {
                console.log('⚠️ Broad scan failed:', broadError.message);
            }
        }
        
        // Remove duplicates and validate
        const uniqueKeys = [...new Set(allAttributionKeys)];
        console.log(`📊 BEFORE FIX: Total keys before deduplication: ${allAttributionKeys.length}`);
        console.log(`📊 AFTER FIX: Unique attribution keys found: ${uniqueKeys.length}`);
        console.log(`📊 Total scanned across all patterns: ${totalScanned}`);
        
        // Log pattern distribution - THIS IS THE KEY INSIGHT
        const underscoreKeys = uniqueKeys.filter(key => key.startsWith('attribution_')).length;
        const colonKeys = uniqueKeys.filter(key => key.startsWith('attribution:')).length;
        console.log(`🎯 PATTERN DISTRIBUTION: ${underscoreKeys} underscore, ${colonKeys} colon format`);
        console.log(`🎯 EXPECTED PAGE VIEW INCREASE: ~${Math.round((colonKeys / underscoreKeys) * 100)}%`);
        
        // Log sample keys for verification
        if (uniqueKeys.length > 0) {
            console.log('📝 Sample attribution keys found (both patterns):');
            uniqueKeys.slice(0, 10).forEach((key, i) => {
                const pattern = key.startsWith('attribution:') ? '[COLON]' : '[UNDERSCORE]';
                console.log(`  ${i+1}. ${pattern} ${key}`);
            });
        }
        
        return uniqueKeys;
        
    } catch (error) {
        console.error('❌ Dual pattern attribution key scanning failed:', error);
        return [];
    }
}

// ENHANCED CONVERSION SCANNING - COMPREHENSIVE FIX
async function getConversionKeysEnhanced(redis) {
    let conversionKeys = [];
    
    console.log('🔍 Starting COMPREHENSIVE conversion key scan...');
    
    try {
        // PATTERN 1: Standard conversions with cursor continuation
        console.log('📊 Scanning conversions:* with full cursor iteration...');
        
        let cursor = '0';
        let totalFound = 0;
        
        do {
            try {
                const result = await redis(`scan/${cursor}/match/conversions:*/count/1000`);
                if (result.result && result.result[1]) {
                    cursor = result.result[0];
                    const keys = result.result[1];
                    conversionKeys = conversionKeys.concat(keys);
                    totalFound += keys.length;
                    console.log(`✅ Found ${keys.length} conversion keys (cursor: ${cursor}, total: ${totalFound})`);
                } else {
                    console.log('⚠️ No more conversion keys found');
                    break;
                }
            } catch (scanError) {
                console.log('⚠️ Scan iteration failed:', scanError.message);
                break;
            }
            
            // Safety limits to prevent infinite loops
            if (totalFound > 10000) {
                console.log('🚨 Safety limit reached, stopping scan');
                break;
            }
            
        } while (cursor !== '0');
        
        console.log(`📊 Comprehensive scan complete: ${totalFound} conversion keys found`);
        
        // PATTERN 2: Email-based conversion keys (backup scan)
        console.log('📊 Scanning for email-based conversion patterns...');
        
        try {
            const emailResult = await redis(`scan/0/match/conversions:*@*/count/1000`);
            if (emailResult.result && emailResult.result[1] && emailResult.result[1].length > 0) {
                const emailKeys = emailResult.result[1];
                // Only add if not already in our list
                const newEmailKeys = emailKeys.filter(key => !conversionKeys.includes(key));
                conversionKeys = conversionKeys.concat(newEmailKeys);
                console.log(`✅ Found ${newEmailKeys.length} additional email-based keys`);
            }
        } catch (emailError) {
            console.log('⚠️ Email pattern scan failed:', emailError.message);
        }
        
        // PATTERN 3: Alternative conversion patterns
        console.log('📊 Scanning for alternative conversion patterns...');
        
        const alternativePatterns = [
            'conversion:*',   // Singular form
            'purchase:*',     // Purchase-specific
            'trial:*',        // Trial-specific
            'subscription:*'  // Subscription-specific
        ];
        
        for (const pattern of alternativePatterns) {
            try {
                const result = await redis(`scan/0/match/${pattern}/count/1000`);
                if (result.result && result.result[1] && result.result[1].length > 0) {
                    const altKeys = result.result[1];
                    conversionKeys = conversionKeys.concat(altKeys);
                    console.log(`✅ Found ${altKeys.length} keys with pattern ${pattern}`);
                }
            } catch (patternError) {
                console.log(`⚠️ Pattern ${pattern} scan failed:`, patternError.message);
            }
        }
        
        // Remove duplicates
        const uniqueKeys = [...new Set(conversionKeys)];
        console.log(`📊 Total conversion keys before deduplication: ${conversionKeys.length}`);
        console.log(`📊 Unique conversion keys found: ${uniqueKeys.length}`);
        
        // Log pattern distribution
        const timestampKeys = uniqueKeys.filter(key => key.match(/conversions:2025-\d{2}-\d{2}T/)).length;
        const emailKeys = uniqueKeys.filter(key => key.includes('_gmail_com') || key.includes('@')).length;
        const otherKeys = uniqueKeys.length - timestampKeys - emailKeys;
        
        console.log(`📊 Conversion pattern distribution:`);
        console.log(`  - Timestamp-based: ${timestampKeys}`);
        console.log(`  - Email-based: ${emailKeys}`);
        console.log(`  - Other patterns: ${otherKeys}`);
        
        // Log sample keys for verification
        if (uniqueKeys.length > 0) {
            console.log('📝 Sample conversion keys found:');
            uniqueKeys.slice(0, 10).forEach((key, i) => {
                let pattern = 'OTHER';
                if (key.match(/conversions:2025-\d{2}-\d{2}T/)) pattern = 'TIMESTAMP';
                if (key.includes('_gmail_com') || key.includes('@')) pattern = 'EMAIL';
                console.log(`  ${i+1}. [${pattern}] ${key}`);
            });
        }
        
        return uniqueKeys;
        
    } catch (error) {
        console.error('❌ Enhanced conversion key scanning failed:', error);
        return [];
    }
}

// Enhanced conversion data fetching with nil handling
async function fetchConversionDataSafely(redis, conversionKeys) {
    console.log(`💰 Fetching conversion data for ${conversionKeys.length} keys...`);
    
    let allConversions = [];
    let nilCount = 0;
    let parseErrors = 0;
    let validConversions = 0;
    
    // Process in smaller batches to handle large datasets
    const batchSize = 500;
    
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
                    
                    // Enhanced timestamp validation with fallback
                    if (!isValidTimestamp(parsed.timestamp)) {
                        console.warn('⚠️ Invalid timestamp found, using current time');
                        parsed.timestamp = new Date().toISOString();
                    }
                    
                    allConversions.push(parsed);
                    validConversions++;
                } catch (parseError) {
                    parseErrors++;
                    console.warn(`⚠️ Failed to parse conversion data for ${item.key}:`, parseError.message);
                }
            });
            
            console.log(`✅ Batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(conversionKeys.length/batchSize)}: processed ${batch.length} keys`);
            
        } catch (batchError) {
            console.error(`❌ Batch ${Math.floor(i/batchSize) + 1} failed:`, batchError);
        }
    }
    
    console.log(`📊 Conversion data processing complete:`);
    console.log(`  - Valid conversions: ${validConversions}`);
    console.log(`  - Nil/missing data: ${nilCount}`);
    console.log(`  - Parse errors: ${parseErrors}`);
    console.log(`  - Total processed: ${conversionKeys.length}`);
    
    return allConversions;
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
            successful_attributions: successfulAttempts,
            ipv6_metrics: {
                pageviews: recentStats.filter(stat => stat.ip_address && stat.ip_address.includes(':')).length,
                dual_stack_ready: true
            },
            timestamp: new Date().toISOString()
        };
        
    } catch (error) {
        console.error('❌ Attribution health calculation error:', error);
        return {
            status: 'error',
            success_rate: 0,
            total_conversions: 0,
            successful_attributions: 0,
            error: error.message,
            timestamp: new Date().toISOString()
        };
    }
}

// Fetch attribution stats
async function fetchAttributionStats(redis) {
    try {
        console.log('📊 Fetching attribution stats...');
        const statsResult = await redis('scan/0/match/attribution_stats:*/count/100');
        const stats = [];
        
        if (statsResult.result && statsResult.result[1]) {
            for (const key of statsResult.result[1]) {
                try {
                    const result = await redis(`get/${key}`);
                    if (result.result) {
                        const data = JSON.parse(decodeURIComponent(result.result));
                        stats.push(data);
                    }
                } catch (e) {
                    // Skip invalid data
                }
            }
        }
        
        return stats;
    } catch (error) {
        console.error('❌ Failed to fetch attribution stats:', error);
        return [];
    }
}

// Calculate attribution summary
function calculateAttributionSummary(attributionStatsData, conversions) {
    const totalConversions = conversions.length;
    const attributedConversions = conversions.filter(c => 
        c.attribution_found || c.attribution_method || c.attribution_score
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
                
                // Get all keys from Redis using COMPREHENSIVE DUAL PATTERN SCANNING
                let attributionKeys = [];
                let conversionKeys = [];
                
                try {
                    console.log('🔍 Starting comprehensive dual pattern Redis key scanning...');
                    
                    // USE THE NEW DUAL PATTERN SCANNING FUNCTION
                    attributionKeys = await getComprehensiveAttributionKeys(redis);
                    console.log(`📊 Dual pattern scan found ${attributionKeys.length} attribution keys`);
                    
                    if (attributionKeys.length < 100) {
                        console.warn('⚠️ VERY LOW KEY COUNT - Data may be missing or using different patterns');
                    } else if (attributionKeys.length > 1000) {
                        console.log('✅ GOOD KEY COUNT - Found substantial data');
                    } else {
                        console.log('📊 MODERATE KEY COUNT - May be missing some data');
                    }
                    
                    // Enhanced conversion key scanning
                    conversionKeys = await getConversionKeysEnhanced(redis);
                    console.log(`🔍 Enhanced scan found ${conversionKeys.length} conversion keys`);
                    
                } catch (redisError) {
                    console.error('❌ Redis operation failed:', redisError);
                    attributionKeys = [];
                    conversionKeys = [];
                }
                
                // Fetch attribution data
                let allPageViews = [];
                if (attributionKeys.length > 0) {
                    try {
                        console.log('📦 Fetching attribution data...');
                        
                        if (attributionKeys.length > 5000) {
                            console.log(`⚠️ Large dataset: ${attributionKeys.length} keys. Processing in batches...`);
                            
                            // Process in batches to avoid timeouts
                            const batchSize = 1000;
                            for (let i = 0; i < attributionKeys.length; i += batchSize) {
                                const batch = attributionKeys.slice(i, i + batchSize);
                                
                                try {
                                    const batchResults = await Promise.all(
                                        batch.map(async (key) => {
                                            try {
                                                const result = await redis(`get/${key}`);
                                                return result.result ? decodeURIComponent(result.result) : null;
                                            } catch (e) {
                                                return null;
                                            }
                                        })
                                    );
                                    
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
                                    
                                    console.log(`✅ Batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(attributionKeys.length/batchSize)}: processed ${batch.length} records`);
                                } catch (batchError) {
                                    console.error(`❌ Batch ${Math.floor(i/batchSize) + 1} failed:`, batchError);
                                }
                            }
                        } else {
                            // Process normally for smaller datasets
                            const attributionResults = await Promise.all(
                                attributionKeys.map(async (key) => {
                                    try {
                                        const result = await redis(`get/${key}`);
                                        return result.result ? decodeURIComponent(result.result) : null;
                                    } catch (e) {
                                        return null;
                                    }
                                })
                            );
                            
                            attributionResults.forEach(item => {
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
                        }
                        
                        console.log(`📊 Successfully parsed ${allPageViews.length} page views`);
                        
                        // Log IPv4/IPv6 distribution
                        const ipv4Count = allPageViews.filter(pv => pv.ip_address && !pv.ip_address.includes(':')).length;
                        const ipv6Count = allPageViews.filter(pv => pv.ip_address && pv.ip_address.includes(':')).length;
                        console.log(`📊 Page view data - IPv4: ${ipv4Count}, IPv6: ${ipv6Count}`);
                        
                        // Log sample IPv6 IPs for verification
                        const sampleIPv6 = allPageViews
                            .filter(pv => pv.ip_address && pv.ip_address.includes(':'))
                            .slice(0, 3)
                            .map(pv => pv.ip_address);
                        if (sampleIPv6.length > 0) {
                            console.log('🌐 Sample IPv6 IPs in data:', sampleIPv6);
                        }
                        
                    } catch (attributionError) {
                        console.error('❌ Attribution data fetch error:', attributionError);
                        allPageViews = [];
                    }
                }
                
                // Fetch conversion data with enhanced processing
                let allConversions = [];
                if (conversionKeys.length > 0) {
                    try {
                        // Use the new safe conversion data fetching
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
                
                // Calculate traffic sources, campaigns, and landing pages
                const sourceMap = new Map();
                const campaignMap = new Map();
                const pageMap = new Map();
                const dailyMap = new Map();
                
                filteredPageViews.forEach(item => {
                    const safeTimestamp = safeProcessTimestamp(item.timestamp);
                    
                    // Only process items with valid timestamps for daily trends
                    if (isValidTimestamp(safeTimestamp)) {
                        try {
                            const date = new Date(safeTimestamp).toISOString().split('T')[0];
                            
                            // Daily trends calculation
                            if (!dailyMap.has(date)) {
                                dailyMap.set(date, { date, pageViews: 0, conversions: 0 });
                            }
                            dailyMap.get(date).pageViews++;
                        } catch (e) {
                            console.warn('⚠️ Skipping invalid timestamp for daily trends:', safeTimestamp);
                        }
                    }
                    
                    // Traffic sources
                    const source = item.source || 'direct';
                    if (!sourceMap.has(source)) {
                        sourceMap.set(source, { source, pageViews: 0, uniqueVisitors: new Set(), conversions: 0 });
                    }
                    const sourceData = sourceMap.get(source);
                    sourceData.pageViews++;
                    if (item.ip_address) sourceData.uniqueVisitors.add(item.ip_address);
                    
                    // Campaigns
                    const campaign = item.utm_campaign || item.campaign || 'No Campaign';
                    if (!campaignMap.has(campaign)) {
                        campaignMap.set(campaign, { campaign, pageViews: 0, uniqueVisitors: new Set(), conversions: 0 });
                    }
                    const campaignData = campaignMap.get(campaign);
                    campaignData.pageViews++;
                    if (item.ip_address) campaignData.uniqueVisitors.add(item.ip_address);
                    
                    // Landing pages
                    const landingPage = item.landing_page || 'Unknown';
                    if (!pageMap.has(landingPage)) {
                        pageMap.set(landingPage, { landingPage, pageViews: 0, uniqueVisitors: new Set(), conversions: 0 });
                    }
                    const pageData = pageMap.get(landingPage);
                    pageData.pageViews++;
                    if (item.ip_address) pageData.uniqueVisitors.add(item.ip_address);
                });
                
                // Add conversions to maps
                filteredConversions.forEach(item => {
                    const safeTimestamp = safeProcessTimestamp(item.timestamp);
                    
                    // Daily trends
                    if (isValidTimestamp(safeTimestamp)) {
                        try {
                            const date = new Date(safeTimestamp).toISOString().split('T')[0];
                            if (dailyMap.has(date)) {
                                dailyMap.get(date).conversions++;
                            }
                        } catch (e) {
                            console.warn('⚠️ Skipping invalid conversion timestamp for daily trends:', safeTimestamp);
                        }
                    }
                    
                    // Sources
                    const source = item.source || 'direct';
                    if (sourceMap.has(source)) {
                        sourceMap.get(source).conversions++;
                    }
                    
                    // Campaigns
                    const campaign = item.utm_campaign || item.campaign || 'No Campaign';
                    if (campaignMap.has(campaign)) {
                        campaignMap.get(campaign).conversions++;
                    }
                    
                    // Landing pages
                    const landingPage = item.landing_page || 'Unknown';
                    if (pageMap.has(landingPage)) {
                        pageMap.get(landingPage).conversions++;
                    }
                });
                
                // Convert to arrays and calculate conversion rates
                const topSources = Array.from(sourceMap.values()).map(item => ({
                    ...item,
                    uniqueVisitors: item.uniqueVisitors.size,
                    conversionRate: item.uniqueVisitors.size > 0 ? 
                        (item.conversions / item.uniqueVisitors.size * 100).toFixed(1) : '0.0'
                })).sort((a, b) => b.pageViews - a.pageViews);
                
                const topCampaigns = Array.from(campaignMap.values()).map(item => ({
                    ...item,
                    uniqueVisitors: item.uniqueVisitors.size,
                    conversionRate: item.uniqueVisitors.size > 0 ? 
                        (item.conversions / item.uniqueVisitors.size * 100).toFixed(1) : '0.0'
                })).sort((a, b) => b.pageViews - a.pageViews);
                
                const topLandingPages = Array.from(pageMap.values()).map(item => ({
                    ...item,
                    uniqueVisitors: item.uniqueVisitors.size,
                    conversionRate: item.uniqueVisitors.size > 0 ? 
                        (item.conversions / item.uniqueVisitors.size * 100).toFixed(1) : '0.0'
                })).sort((a, b) => b.pageViews - a.pageViews);
                
                const dailyTrends = Array.from(dailyMap.values()).sort((a, b) => a.date.localeCompare(b.date));
                
                // Build response object
                const response = {
                    summary: {
                        total_page_views: totalPageViews,
                        unique_visitors: uniqueVisitors,
                        total_conversions: totalConversions,
                        free_trials: filteredConversions.filter(c => (parseFloat(c.order_total) || 0) === 0).length,
                        paid_conversions: paidConversions.length,
                        total_revenue: totalRevenue,
                        avg_order_value: avgOrderValue,
                        conversion_rate: conversionRate.toFixed(1),
                        date_range: { start: start_date, end: end_date }
                    },
                    traffic_sources: topSources,
                    campaign_performance: topCampaigns,
                    landing_page_performance: topLandingPages,
                    daily_trends: dailyTrends,
                    conversions: filteredConversions,
                    page_views: filteredPageViews,
                    
                    debug: {
                        attribution_keys_found: attributionKeys.length,
                        conversion_keys_found: conversionKeys.length,
                        raw_page_views_processed: allPageViews.length,
                        filtered_page_views: filteredPageViews.length,
                        raw_conversions_processed: allConversions.length,
                        filtered_conversions: filteredConversions.length,
                        cors_enabled: true,
                        deployment_timestamp: new Date().toISOString(),
                        dual_pattern_scanning_enabled: true
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
