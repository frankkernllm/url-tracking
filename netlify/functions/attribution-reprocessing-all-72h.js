// File: netlify/functions/attribution-reprocessing-all-72h.js
// REPROCESS ALL CONVERSIONS from past 72 hours to fix attribution data
// Processes both attributed AND unattributed conversions

const handler = async (event, context) => {
  console.log('🔄 Attribution Reprocessing System - ALL CONVERSIONS (72 Hours)');
  
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Content-Type': 'application/json'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  const validApiKey = process.env.OJOY_API_KEY;
  
  if (!apiKey || apiKey !== validApiKey) {
    return {
      statusCode: 401,
      headers,
      body: JSON.stringify({ error: 'Unauthorized' })
    };
  }

  try {
    // Runtime management
    const startTime = Date.now();
    const maxRunTime = 25000; // 25 seconds for safety
    let processedInThisRun = 0;
    const processedConversions = [];
    const cacheStats = { hits: 0, misses: 0, api_calls: 0, redis_hits: 0 };
    const geoDataCache = new Map(); // In-memory cache for this run

    console.log('📊 Starting complete reprocessing of all conversions (72 hours)...');

    // Get ALL conversions from past 72 hours (attributed AND unattributed)
    const analyticsData = await getAllConversionsLast72Hours();
    console.log(`📊 Found ${analyticsData.all_conversions.length} total conversions to reprocess`);

    if (analyticsData.all_conversions.length === 0) {
        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({
                success: true,
                message: 'No conversions found in past 72 hours',
                results: {
                    processed_in_this_run: 0,
                    remaining_to_process: 0,
                    status: 'NO_CONVERSIONS_FOUND'
                }
            })
        };
    }

    // Dynamic batch sizing based on conversion count and available time
    const estimatedTimePerConversion = 1500; // ms per conversion estimate
    const availableTime = maxRunTime - 2000; // Reserve 2s for response
    let batchSize = Math.min(
        Math.floor(availableTime / estimatedTimePerConversion),
        analyticsData.all_conversions.length,
        20 // Increased batch size since we're reprocessing
    );
    batchSize = Math.max(1, batchSize); // Minimum 1

    console.log(`📦 Processing batch of ${batchSize} conversions (${analyticsData.all_conversions.length} total)`);

    // Process conversions in the current batch
    const currentBatch = analyticsData.all_conversions.slice(0, batchSize);
    const remainingConversions = analyticsData.all_conversions.slice(batchSize);
    
    for (let i = 0; i < currentBatch.length; i++) {
        const conversionToProcess = currentBatch[i];
        const conversionStartTime = Date.now();
        
        // Check runtime limit
        if (Date.now() - startTime > maxRunTime - 3000) {
            console.log(`⏰ Approaching runtime limit, stopping at ${i} conversions`);
            break;
        }

        console.log(`\n🎯 CONVERSION ${i + 1}/${batchSize}: ${conversionToProcess.email}`);
        console.log(`   📅 Current Status: ${conversionToProcess.attribution_found ? 'ATTRIBUTED' : 'UNATTRIBUTED'}`);
        if (conversionToProcess.attribution_found) {
            console.log(`   📄 Current Attribution: ${conversionToProcess.landing_page}`);
        }
        
        // Extract parameters with backwards compatibility
        const conversionData = extractConversionParameters(conversionToProcess);
        
        console.log(`   📊 Type: ${conversionData.has_enhanced_params ? 'ENHANCED' : 'LEGACY'} (age: ${conversionData.conversion_age_days} days)`);
        console.log(`   📍 IPs: PIP=${conversionData.PIP || 'none'}, CIP=${conversionData.CIP || 'none'}, IP=${conversionData.IP || 'none'}`);
        console.log(`   🔐 Signatures: SSID=${!!conversionData.SSID}, dsig=${!!conversionData.dsig}, SVV=${!!conversionData.SVV}`);
        
        // Enhanced attribution analysis - try to find BETTER attribution
        const improvementResults = await analyzeConversionForAttributionEnhanced(
            conversionToProcess, 
            analyticsData.page_views, 
            conversionData,
            geoDataCache,
            cacheStats
        );
        
        // Update Redis if we found better attribution OR if we want to correct existing attribution
        let updateResult = null;
        if (improvementResults.shouldUpdate) {
            console.log(`📝 Updating attribution for ${conversionToProcess.email}...`);
            console.log(`   🔄 ${improvementResults.improvementType}: ${improvementResults.attributionMethod}`);
            try {
                updateResult = await updateConversionAttributionEnhanced(conversionToProcess, improvementResults);
            } catch (redisError) {
                console.error('❌ Redis update failed:', redisError);
            }
        } else {
            console.log(`   ℹ️ No better attribution found - keeping existing`);
        }
        
        // Mark as reprocessed with timestamp
        await markConversionAsReprocessed(conversionToProcess, improvementResults.attributionMethod);
        
        // Track this processed conversion
        const conversionTime = Date.now() - conversionStartTime;
        processedInThisRun++;
        processedConversions.push({
            email: conversionToProcess.email,
            previous_attribution: conversionToProcess.attribution_found,
            previous_landing_page: conversionToProcess.landing_page,
            improvement_type: improvementResults.improvementType,
            attribution_method: improvementResults.attributionMethod,
            priority_level: improvementResults.priorityLevel || 8,
            processing_time_ms: conversionTime,
            update_result: updateResult
        });
        
        console.log(`✅ Completed ${conversionToProcess.email} in ${conversionTime/1000}s`);
    }

    const totalTime = Date.now() - startTime;
    const isComplete = remainingConversions.length === 0;

    console.log('📊 Attribution reprocessing run complete');
    console.log(`✅ Processed ${processedInThisRun} conversions in ${totalTime/1000}s`);

    // Calculate improvement statistics
    const improvements = processedConversions.filter(p => p.improvement_type !== 'NO_IMPROVEMENT');
    const newAttributions = processedConversions.filter(p => p.improvement_type === 'NEW_ATTRIBUTION');
    const betterAttributions = processedConversions.filter(p => p.improvement_type === 'BETTER_ATTRIBUTION');

    return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
            success: true,
            message: 'Attribution reprocessing completed',
            results: {
                processed_in_this_run: processedInThisRun,
                remaining_to_process: remainingConversions.length,
                improvements_found: improvements.length,
                new_attributions: newAttributions.length,
                better_attributions: betterAttributions.length,
                processed_conversions: processedConversions,
                status: isComplete ? 'ALL_COMPLETE' : 'CONTINUE',
                next_action: isComplete ? 'All 72-hour conversions reprocessed!' : 'Run again to continue reprocessing'
            },
            performance: {
                total_run_time_seconds: totalTime / 1000,
                average_time_per_conversion: processedInThisRun > 0 ? (totalTime / processedInThisRun / 1000) : 0,
                cache_performance: {
                    total_lookups: cacheStats.hits + cacheStats.misses,
                    cache_hits: cacheStats.hits,
                    cache_misses: cacheStats.misses,
                    redis_hits: cacheStats.redis_hits,
                    api_calls: cacheStats.api_calls,
                    efficiency_percent: Math.round(cacheStats.hits / (cacheStats.hits + cacheStats.misses + 1) * 100)
                }
            },
            time_range: 'Past 72 hours (all conversions)'
        })
    };

  } catch (error) {
    console.error('❌ Attribution reprocessing error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({
        error: 'Attribution reprocessing failed',
        details: error.message
      })
    };
  }
};

// Redis request helper
async function redisRequest(command, ...args) {
    const url = process.env.UPSTASH_REDIS_REST_URL;
    const token = process.env.UPSTASH_REDIS_REST_TOKEN;
    
    if (!url || !token) {
        throw new Error('Missing Redis configuration');
    }

    try {
        if (command.toLowerCase() === 'set' || command.toLowerCase() === 'setex') {
            const response = await fetch(`${url}/${command}/${args.join('/')}`, {
                headers: { Authorization: `Bearer ${token}` }
            });
            return response.json();
        } else {
            const response = await fetch(`${url}/${command}/${args.join('/')}`, {
                headers: { Authorization: `Bearer ${token}` }
            });
            return response.json();
        }
    } catch (error) {
        console.error(`Redis ${command} error:`, error);
        throw error;
    }
}

// Get ALL conversions from last 72 hours (attributed AND unattributed)
async function getAllConversionsLast72Hours() {
    const startDate = new Date();
    startDate.setHours(startDate.getHours() - 72); // 72 hours ago
    const endDate = new Date();
    
    console.log(`📅 Fetching ALL conversions from ${startDate.toISOString()} to ${endDate.toISOString()}`);
    
    // Get conversion keys
    let allKeys = [];
    let cursor = '0';
    
    do {
        const result = await redisRequest('scan', cursor, 'match', 'conversions:*', 'count', '1000');
        if (result.result && result.result[1]) {
            cursor = result.result[0];
            const keys = result.result[1];
            allKeys = allKeys.concat(keys);
            if (allKeys.length > 10000) break;
        } else {
            break;
        }
    } while (cursor !== '0');
    
    console.log(`🔍 Found ${allKeys.length} conversion keys to analyze`);
    
    const allConversions = [];
    
    const startTimestamp = startDate.getTime();
    const endTimestamp = endDate.getTime();
    
    for (const key of allKeys) {
        try {
            const conversionResult = await redisRequest('get', key);
            const conversionData = conversionResult.result;
            if (!conversionData) continue;
            
            let conversion = typeof conversionData === 'string' ? JSON.parse(conversionData) : conversionData;
            
            if (!conversion.timestamp) continue;
            
            const conversionTimestamp = new Date(conversion.timestamp).getTime();
            
            // Include ALL conversions within 72 hours (attributed and unattributed)
            if (conversionTimestamp >= startTimestamp && conversionTimestamp <= endTimestamp) {
                allConversions.push(conversion);
            }
        } catch (error) {
            console.log(`⚠️ Error processing ${key}: ${error.message}`);
        }
    }
    
    // Sort by timestamp (newest first for priority processing)
    const sortedConversions = allConversions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    
    console.log(`📊 Found ${sortedConversions.length} total conversions in past 72 hours`);
    console.log(`📊 Attribution status: ${sortedConversions.filter(c => c.attribution_found).length} attributed, ${sortedConversions.filter(c => !c.attribution_found).length} unattributed`);
    
    return {
        all_conversions: sortedConversions,
        page_views: [] // Empty for now, implement if needed
    };
}

// Backwards compatible data extraction
function extractConversionParameters(conversion) {
    const conversionAge = Math.floor((Date.now() - new Date(conversion.timestamp)) / (1000 * 60 * 60 * 24));
    
    return {
        email: conversion.email,
        timestamp: conversion.timestamp,
        
        // Enhanced parameters (may not exist in older conversions)
        SSID: conversion.session_id || conversion.SSID || null,
        
        // IP precedence: newer conversions may have multiple IPs
        PIP: conversion.primary_ip || conversion.custom_ipv6 || null,
        CIP: conversion.conversion_ip || conversion.custom_ipv4 || null, 
        IP: conversion.ip_address || conversion.ip || null,
        
        // Device signatures (likely missing in older conversions)
        dsig: conversion.device_signature || 
              (conversion.canvas_fingerprint?.slice(-20)) || 
              null,
        SVV: conversion.screen_hash || 
             (conversion.screen_resolution ? hashString(conversion.screen_resolution) : null),
        gsig: conversion.webgl_hash || 
              (conversion.webgl_fingerprint ? hashString(conversion.webgl_fingerprint) : null),
        
        // Standard fields
        landing_page: conversion.landing_page,
        utm_source: conversion.utm_source || conversion.source,
        utm_campaign: conversion.utm_campaign || conversion.campaign,
        utm_medium: conversion.utm_medium,
        utm_content: conversion.utm_content,
        utm_term: conversion.utm_term,
        
        // Metadata
        has_enhanced_params: !!(conversion.session_id || conversion.device_signature || conversion.primary_ip),
        conversion_age_days: conversionAge,
        requires_geo_correlation: !(conversion.session_id || conversion.device_signature),
        is_legacy_conversion: conversionAge > 30
    };
}

// Hash function for privacy-safe parameter values
function hashString(str) {
    if (!str) return null;
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
        const char = str.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash; // Convert to 32-bit integer
    }
    return Math.abs(hash).toString(36);
}

// Enhanced cached geographic data strategy
async function getCachedGeoData(ip, geoDataCache, cacheStats) {
    if (!ip || ip === 'unknown') return null;
    
    // Check in-memory cache first
    if (geoDataCache.has(ip)) {
        cacheStats.hits++;
        console.log(`   💾 Using in-memory cache for ${ip}`);
        return geoDataCache.get(ip);
    }
    
    try {
        // Check Redis cache (from store-attribution.js and track.js)
        const encodedIP = ip.replace(/:/g, '_');
        const cacheKey = `geo_cache:${encodedIP}`;
        const cachedResult = await redisRequest('get', cacheKey);
        
        if (cachedResult.result) {
            const geoData = JSON.parse(decodeURIComponent(cachedResult.result));
            geoDataCache.set(ip, geoData);
            cacheStats.redis_hits++;
            console.log(`   📦 Using Redis cache for ${ip}: ${geoData.city}, ${geoData.region}`);
            return geoData;
        }
        
        cacheStats.misses++;
        return null;
        
    } catch (error) {
        console.log(`   ⚠️ Cache lookup failed for ${ip}: ${error.message}`);
        cacheStats.misses++;
        return null;
    }
}

// Get or fetch geographic data
async function getOrFetchGeoData(ip, geoDataCache, cacheStats) {
    // Try cache first
    let geoData = await getCachedGeoData(ip, geoDataCache, cacheStats);
    if (geoData) return geoData;
    
    // Only make API call if within rate limits
    if (cacheStats.api_calls < 10) {
        try {
            const ipinfoToken = process.env.IPINFO_TOKEN;
            if (!ipinfoToken) return getFailedLookupData(ip);
            
            console.log(`   🌍 Making IPinfo API call for ${ip} (${cacheStats.api_calls + 1}/10)`);
            const response = await fetch(`https://ipinfo.io/${ip}?token=${ipinfoToken}`, {
                signal: AbortSignal.timeout(3000)
            });
            
            if (response.ok) {
                const data = await response.json();
                geoData = {
                    ip: data.ip,
                    city: data.city || 'Unknown',
                    region: data.region || 'Unknown', 
                    country: data.country || 'Unknown',
                    isp: extractBestISP(data),
                    coordinates: data.loc || '0,0',
                    timezone: data.timezone || 'Unknown',
                    lookup_timestamp: new Date().toISOString()
                };
                
                // Cache in both memory and Redis
                geoDataCache.set(ip, geoData);
                const cacheKey = `geo_cache:${ip.replace(/:/g, '_')}`;
                await redisRequest('setex', cacheKey, '86400', encodeURIComponent(JSON.stringify(geoData)));
                
                cacheStats.api_calls++;
                console.log(`   🌍 IPinfo result for ${ip}: ${geoData.city}, ${geoData.region}, ${geoData.isp}`);
                return geoData;
            }
        } catch (error) {
            console.log(`   ⚠️ IPinfo API failed for ${ip}: ${error.message}`);
            cacheStats.api_calls++;
        }
    }
    
    return getFailedLookupData(ip);
}

// Extract best ISP from IPinfo response
function extractBestISP(data) {
    if (data.company?.name) return data.company.name;
    if (data.asn?.name) return data.asn.name;
    if (data.org) return data.org;
    return 'Unknown';
}

// Failed lookup data
function getFailedLookupData(ip) {
    return {
        ip: ip,
        city: 'LOOKUP_FAILED',
        region: 'LOOKUP_FAILED',
        country: 'LOOKUP_FAILED',
        isp: 'LOOKUP_FAILED',
        coordinates: '0,0',
        timezone: 'Unknown',
        lookup_timestamp: new Date().toISOString()
    };
}

// Enhanced attribution analysis that can improve existing attributions
async function analyzeConversionForAttributionEnhanced(conversion, pageViews, conversionData, geoDataCache, cacheStats) {
    console.log(`   🔬 Analyzing attribution for ${conversion.email}...`);
    
    // Start with basic result structure
    let improvementResults = {
        shouldUpdate: false,
        improvementType: 'NO_IMPROVEMENT',
        attributionMethod: 'none',
        priorityLevel: 8,
        newAttribution: null,
        match: null,
        analysis: {
            processing_path: 'reprocessing_analysis',
            geo_correlation_attempted: false,
            cache_performance: `${cacheStats.hits}H/${cacheStats.misses}M`
        }
    };

    // Enhanced 8-tier attribution priority system
    const attributionPriorities = [
        { name: 'session_id_match', field: 'SSID', points: 300, priority: 1 },
        { name: 'primary_ip_match', field: 'PIP', points: 280, priority: 2 },
        { name: 'conversion_ip_match', field: 'CIP', points: 260, priority: 3 },
        { name: 'pageview_ip_match', field: 'IP', points: 240, priority: 4 },
        { name: 'device_signature_match', field: 'dsig', points: 220, priority: 5 },
        { name: 'screen_hash_match', field: 'SVV', points: 200, priority: 6 },
        { name: 'webgl_match', field: 'gsig', points: 180, priority: 7 },
        { name: 'geographic_match', field: 'geographic_correlation', points: 100, priority: 8 }
    ];

    // Determine current attribution priority (if any)
    let currentPriority = 9; // Lower than geographic
    if (conversion.attribution_found && conversion.attribution_improvement?.priority_level) {
        currentPriority = conversion.attribution_improvement.priority_level;
    }

    // Try each method in priority order
    for (const priority of attributionPriorities) {
        // Only consider methods with higher priority than current attribution
        if (priority.priority >= currentPriority) {
            console.log(`   ⏭️ Skipping ${priority.name} (priority ${priority.priority} >= current ${currentPriority})`);
            continue;
        }

        if (priority.name === 'geographic_match') {
            // Special handling for geographic matching
            console.log(`   🌍 Attempting geographic correlation...`);
            improvementResults.analysis.geo_correlation_attempted = true;
            
            const geoMatch = await attemptGeographicCorrelation(conversion, conversionData, geoDataCache, cacheStats);
            if (geoMatch) {
                console.log(`   ✅ Geographic correlation successful!`);
                improvementResults = {
                    shouldUpdate: true,
                    improvementType: conversion.attribution_found ? 'BETTER_ATTRIBUTION' : 'NEW_ATTRIBUTION',
                    attributionMethod: 'geographic_match',
                    priorityLevel: 8,
                    newAttribution: geoMatch.pageview?.landing_page || geoMatch.landing_page,
                    match: geoMatch,
                    analysis: {
                        ...improvementResults.analysis,
                        geographic_confidence: geoMatch.confidence,
                        geographic_score: geoMatch.score
                    }
                };
                break;
            }
        } else {
            // Standard attribution method lookup
            const fieldValue = conversionData[priority.field];
            if (!fieldValue) {
                console.log(`   ❌ ${priority.name}: No ${priority.field} available`);
                continue;
            }

            console.log(`   🔍 ${priority.name}: Checking ${priority.field}=${fieldValue}`);
            
            // Generate lookup key with correct format
            let lookupKey;
            if (priority.field === 'PIP' || priority.field === 'CIP' || priority.field === 'IP') {
                // IP-based lookup
                if (fieldValue.includes(':')) {
                    // IPv6
                    lookupKey = `attribution_ip_${fieldValue.replace(/:/g, '_')}`;
                } else {
                    // IPv4
                    lookupKey = `attribution_ip_${fieldValue}`;
                }
            } else if (priority.field === 'SSID') {
                lookupKey = `attribution_session_${fieldValue}`;
            } else if (priority.field === 'dsig') {
                lookupKey = `attribution_fp_${fieldValue}`;
            } else if (priority.field === 'SVV') {
                lookupKey = `attribution_screen_${fieldValue}`;
            } else if (priority.field === 'gsig') {
                lookupKey = `attribution_webgl_${fieldValue}`;
            }

            try {
                const attributionResult = await redisRequest('get', lookupKey);
                if (attributionResult.result) {
                    console.log(`   ✅ ${priority.name}: Found attribution data!`);
                    const attributionData = JSON.parse(attributionResult.result);
                    
                    improvementResults = {
                        shouldUpdate: true,
                        improvementType: conversion.attribution_found ? 'BETTER_ATTRIBUTION' : 'NEW_ATTRIBUTION',
                        attributionMethod: priority.name,
                        priorityLevel: priority.priority,
                        newAttribution: attributionData.landing_page || attributionData.url,
                        match: attributionData,
                        analysis: {
                            ...improvementResults.analysis,
                            attribution_score: priority.points,
                            lookup_key: lookupKey
                        }
                    };
                    break; // Found match, stop here (highest priority wins)
                } else {
                    console.log(`   ❌ ${priority.name}: No attribution data found`);
                }
            } catch (error) {
                console.log(`   ⚠️ ${priority.name}: Error during lookup - ${error.message}`);
            }
        }
    }

    console.log(`   📊 Analysis result: ${improvementResults.improvementType} via ${improvementResults.attributionMethod}`);
    return improvementResults;
}

// Geographic correlation attempt (same as before)
async function attemptGeographicCorrelation(conversion, conversionData, geoDataCache, cacheStats) {
    // Get geo data for available IPs
    const ipsToCheck = [conversionData.PIP, conversionData.CIP, conversionData.IP].filter(ip => ip);
    
    if (ipsToCheck.length === 0) {
        console.log(`   ❌ No IPs available for geographic correlation`);
        return null;
    }

    console.log(`   🗺️ Checking ${ipsToCheck.length} IPs for geographic correlation`);
    
    for (const ip of ipsToCheck) {
        const geoData = await getOrFetchGeoData(ip, geoDataCache, cacheStats);
        
        if (geoData && geoData.city !== 'LOOKUP_FAILED') {
            console.log(`   📍 Geo data for ${ip}: ${geoData.city}, ${geoData.region}, ${geoData.isp}`);
            
            // Try to find attribution data using geographic correlation
            const cleanCity = cleanForRedisKey(geoData.city);
            const cleanISP = cleanForRedisKey(geoData.isp?.substring(0, 20) || 'unknown');
            const cleanRegion = cleanForRedisKey(geoData.region);
            
            if (cleanCity.length > 2 && cleanISP.length > 2) {
                // Try city + ISP correlation
                const geoPattern = `attribution_geo_${cleanCity}_${cleanISP}_*`;
                console.log(`   🔍 Trying geographic pattern: ${geoPattern}`);
                
                try {
                    // Scan for matching geographic keys
                    const scanResult = await redisRequest('scan', '0', 'match', geoPattern, 'count', '100');
                    if (scanResult.result && scanResult.result[1] && scanResult.result[1].length > 0) {
                        const geoKeys = scanResult.result[1];
                        console.log(`   ✅ Found ${geoKeys.length} geographic matches`);
                        
                        // Get the first matching attribution data
                        const firstKey = geoKeys[0];
                        const attrResult = await redisRequest('get', firstKey);
                        if (attrResult.result) {
                            const attrData = JSON.parse(attrResult.result);
                            return {
                                pageview: attrData,
                                landing_page: attrData.landing_page || attrData.url,
                                score: 100,
                                confidence: 'medium',
                                method: 'geographic_city_isp',
                                geo_data: geoData,
                                lookup_pattern: geoPattern
                            };
                        }
                    }
                } catch (error) {
                    console.log(`   ⚠️ Geographic scan failed: ${error.message}`);
                }
                
                // Try region + ISP correlation if city failed
                if (cleanRegion.length > 2) {
                    const regionPattern = `attribution_region_${cleanRegion}_${cleanISP}_*`;
                    console.log(`   🔍 Trying regional pattern: ${regionPattern}`);
                    
                    try {
                        const regionScanResult = await redisRequest('scan', '0', 'match', regionPattern, 'count', '100');
                        if (regionScanResult.result && regionScanResult.result[1] && regionScanResult.result[1].length > 0) {
                            const regionKeys = regionScanResult.result[1];
                            console.log(`   ✅ Found ${regionKeys.length} regional matches`);
                            
                            const firstKey = regionKeys[0];
                            const attrResult = await redisRequest('get', firstKey);
                            if (attrResult.result) {
                                const attrData = JSON.parse(attrResult.result);
                                return {
                                    pageview: attrData,
                                    landing_page: attrData.landing_page || attrData.url,
                                    score: 80,
                                    confidence: 'low',
                                    method: 'geographic_region_isp',
                                    geo_data: geoData,
                                    lookup_pattern: regionPattern
                                };
                            }
                        }
                    } catch (error) {
                        console.log(`   ⚠️ Regional scan failed: ${error.message}`);
                    }
                }
            }
        } else {
            console.log(`   ❌ Could not get geo data for ${ip}`);
        }
    }
    
    console.log(`   ❌ No geographic correlation found`);
    return null;
}

// Clean string for Redis key usage
function cleanForRedisKey(str) {
    if (!str) return '';
    return str.replace(/[^a-zA-Z0-9]/g, '').toLowerCase();
}

// Mark conversion as reprocessed (instead of process4)
async function markConversionAsReprocessed(conversion, attributionMethod) {
    try {
        const reprocessKey = `reprocessed:${conversion.email}:${conversion.timestamp}`;
        const reprocessData = {
            email: conversion.email,
            timestamp: conversion.timestamp,
            reprocessed_at: new Date().toISOString(),
            system: 'attribution_reprocessing_all_72h',
            version: '1.0',
            attribution_method: attributionMethod,
            processing_type: 'complete_reprocessing'
        };
        
        await redisRequest('setex', reprocessKey, '604800', JSON.stringify(reprocessData)); // 7 days
    } catch (error) {
        console.log(`   ⚠️ Could not mark ${conversion.email} as reprocessed: ${error.message}`);
    }
}

// Enhanced attribution update (same as before but allows overwriting existing attribution)
async function updateConversionAttributionEnhanced(conversion, improvementResults) {
    try {
        // Find the conversion record in Redis
        const conversionKey = await findConversionKey(conversion);
        
        if (conversionKey) {
            // Get existing data
            const existingResult = await redisRequest('get', conversionKey);
            let conversionData = existingResult.result;
            if (typeof conversionData === 'string') {
                conversionData = JSON.parse(conversionData);
            }
            
            // Update with enhanced attribution
            const updatedConversion = {
                ...conversionData,
                attribution_found: true,
                landing_page: improvementResults.newAttribution,
                source: improvementResults.match?.source || 'reprocessed_v1_0',
                utm_campaign: improvementResults.match?.utm_campaign || conversionData.utm_campaign,
                utm_medium: improvementResults.match?.utm_medium || conversionData.utm_medium,
                referrer_url: improvementResults.match?.referrer_url || conversionData.referrer_url,
                
                // Attribution improvement metadata
                attribution_improvement: {
                    method: improvementResults.attributionMethod,
                    improvement_type: improvementResults.improvementType,
                    priority_level: improvementResults.priorityLevel,
                    confidence: improvementResults.match?.confidence || 'medium',
                    score: improvementResults.match?.score || 0,
                    improved_at: new Date().toISOString(),
                    system_version: '1.0',
                    processing_type: 'complete_reprocessing'
                }
            };
            
            // Save back to Redis
            await redisRequest('set', conversionKey, JSON.stringify(updatedConversion));
            
            return {
                success: true,
                updated_attribution: improvementResults.newAttribution,
                improvement_type: improvementResults.improvementType,
                attribution_method: improvementResults.attributionMethod,
                priority_level: improvementResults.priorityLevel
            };
            
        } else {
            return { success: false, error: 'Conversion not found in Redis' };
        }
        
    } catch (error) {
        return { success: false, error: error.message };
    }
}

// Find conversion key in Redis (same as before)
async function findConversionKey(conversion) {
    // Try to find the conversion key by scanning
    let cursor = '0';
    
    do {
        const result = await redisRequest('scan', cursor, 'match', 'conversions:*', 'count', '1000');
        if (result.result && result.result[1]) {
            cursor = result.result[0];
            const keys = result.result[1];
            
            for (const key of keys) {
                try {
                    const convResult = await redisRequest('get', key);
                    if (convResult.result) {
                        const convData = typeof convResult.result === 'string' ? 
                            JSON.parse(convResult.result) : convResult.result;
                        
                        if (convData.email === conversion.email && 
                            convData.timestamp === conversion.timestamp) {
                            return key;
                        }
                    }
                } catch (error) {
                    continue;
                }
            }
        } else {
            break;
        }
    } while (cursor !== '0');
    
    return null;
}

module.exports = { handler };
