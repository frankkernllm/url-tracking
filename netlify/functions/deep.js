// DEEP.JS v4.0: Complete Implementation with Full Geographic Correlation
// Processes conversions without "deep4" marker using robust 8-tier + 24-hour geo correlation

// ================================================================
// 1. ENHANCED REDIS REQUEST WITH COMPREHENSIVE ERROR HANDLING
// ================================================================

async function redisRequest(command, ...args) {
    const url = process.env.UPSTASH_REDIS_REST_URL;
    const token = process.env.UPSTASH_REDIS_REST_TOKEN;
    
    if (!url || !token) {
        throw new Error('Missing Redis configuration');
    }
    
    const maxRetries = 3;
    const baseDelay = 1000; // 1 second
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        const controller = new AbortController();
        const timeoutMs = attempt === 1 ? 5000 : 8000;
        const timeoutId = setTimeout(() => {
            controller.abort();
            console.log(`⏰ Redis request timeout after ${timeoutMs}ms (attempt ${attempt})`);
        }, timeoutMs);
        
        try {
            let response;
            
            if ((command.toLowerCase() === 'set' || command.toLowerCase() === 'setex') && args.length >= 2) {
                response = await fetch(url, {
                    method: 'POST',
                    headers: {
                        'Authorization': `Bearer ${token}`,
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify([command, ...args]),
                    signal: controller.signal
                });
            } else {
                const encodedArgs = args.map(arg => encodeURIComponent(arg));
                const requestUrl = `${url}/${command}/${encodedArgs.join('/')}`;
                
                response = await fetch(requestUrl, {
                    headers: {
                        'Authorization': `Bearer ${token}`,
                        'Content-Type': 'application/json'
                    },
                    signal: controller.signal
                });
            }
            
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
            
            if (error.name === 'AbortError') {
                console.log(`   ⚠️ Redis request timed out for command: ${command} (attempt ${attempt})`);
                if (attempt === maxRetries) {
                    throw new Error(`Redis timeout: ${command} operation exceeded ${timeoutMs}ms after ${maxRetries} attempts`);
                }
            } else if (error.message.includes('Failed to fetch') || error.message.includes('fetch')) {
                console.log(`   ⚠️ Network error for command: ${command} (attempt ${attempt}): ${error.message}`);
                if (attempt === maxRetries) {
                    throw new Error(`Network error: ${command} failed after ${maxRetries} attempts - ${error.message}`);
                }
            } else {
                console.log(`   ⚠️ Redis request failed for command: ${command} (attempt ${attempt}): ${error.message}`);
                if (attempt === maxRetries) {
                    throw error;
                }
            }
            
            if (attempt < maxRetries) {
                const delay = baseDelay * Math.pow(2, attempt - 1);
                console.log(`   🔄 Retrying in ${delay}ms...`);
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }
    }
}

// ================================================================
// 2. DEEP5 FILTERING AND MARKING FUNCTIONS
// ================================================================

async function filterNonDeep5Conversions(unattributedConversions) {
    const unprocessedConversions = [];
    let alreadyProcessedDeep5Count = 0;
    const maxProcessingTime = 30000;
    const startTime = Date.now();
    
    console.log(`🔍 Checking ${unattributedConversions.length} conversions for deep5 processing status...`);
    
    for (const conversion of unattributedConversions) {
        if (Date.now() - startTime > maxProcessingTime) {
            console.log(`⏰ Filtering timeout reached after 30 seconds`);
            break;
        }
        
        // ONLY check for deep5 marker (ignore deep2, deep3, deep4, process4, etc.)
        const keyDeep5 = `deep5:${conversion.email}:${conversion.timestamp}`;
        
        try {
            const processedData = await redisRequest('get', keyDeep5);
            
            if (processedData) {
                alreadyProcessedDeep5Count++;
                console.log(`   ⏭️ Skipping [PRIVACY PROTECTED] - already processed with deep5 system`);
            } else {
                // No deep5 marker found - process this conversion
                // (This includes conversions with deep2, deep3, deep4 markers that failed or need retesting)
                unprocessedConversions.push(conversion);
            }
        } catch (error) {
            console.log(`   ⚠️ Failed to check deep5 status: ${error.message}`);
            unprocessedConversions.push(conversion);
        }
    }
    
    console.log(`📊 Filtered out ${alreadyProcessedDeep5Count} already processed with deep5 system`);
    console.log(`🔄 Will reprocess ${unprocessedConversions.length} conversions (includes deep2/deep3/deep4 failures)`);
    return unprocessedConversions;
}

async function markConversionAsDeep5(conversion, attributionMethod, retryCount = 0) {
    const maxRetries = 3;
    
    try {
        const deep5Key = `deep5:${conversion.email}:${conversion.timestamp}`;
        const deep5Data = {
            email: conversion.email,
            timestamp: conversion.timestamp,
            processed_at: new Date().toISOString(),
            system: 'deep5_8tier_complete_geocorr',
            version: '5.0',
            attribution_method: attributionMethod,
            processing_type: 'deep_dive_analysis',
            retry_count: retryCount
        };
        
        await redisRequest('setex', deep5Key, 2592000, JSON.stringify(deep5Data));
        console.log(`   ✅ Marked conversion as deep5 processed (attempt ${retryCount + 1})`);
        
    } catch (error) {
        console.log(`   ⚠️ Could not mark conversion as deep5: ${error.message}`);
        
        if (retryCount < maxRetries) {
            console.log(`   🔄 Retrying mark operation (${retryCount + 1}/${maxRetries})...`);
            await new Promise(resolve => setTimeout(resolve, 1000 * (retryCount + 1)));
            return markConversionAsDeep5(conversion, attributionMethod, retryCount + 1);
        } else {
            console.log(`   ❌ Failed to mark conversion after ${maxRetries} attempts`);
        }
    }
}

// ================================================================
// 3. ANALYTICS DATA FETCHING
// ================================================================

async function fetchAnalyticsDataPast7Days() {
    console.log('📊 Fetching analytics data for past 7 days...');
    
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(endDate.getDate() - 7);
    
    const earliestDate = new Date('2025-06-11');
    if (startDate < earliestDate) {
        startDate.setTime(earliestDate.getTime());
    }
    
    const startDateStr = startDate.toISOString().split('T')[0];
    const endDateStr = endDate.toISOString().split('T')[0];
    
    console.log(`📅 Safe date range: ${startDateStr} to ${endDateStr}`);
    
    const params = new URLSearchParams();
    params.append('start_date', startDateStr);
    params.append('end_date', endDateStr);
    
    const apiUrl = `https://trackingojoy.netlify.app/.netlify/functions/analytics?${params}`;
    
    const response = await fetch(apiUrl, {
        headers: {
            'X-API-Key': process.env.OJOY_API_KEY
        }
    });
    
    if (!response.ok) {
        throw new Error(`Analytics API failed: ${response.status} ${response.statusText}`);
    }
    
    const data = await response.json();
    console.log(`✅ Analytics data loaded: ${data.conversions?.length || 0} conversions, ${data.page_views?.length || 0} pageviews`);
    
    return data;
}

// ================================================================
// 4. CONVERSION PROCESSING FUNCTIONS
// ================================================================

function getAllConversions(conversions) {
    if (!conversions || conversions.length === 0) {
        console.log('❌ No conversions found in analytics data');
        return [];
    }
    
    const sortedConversions = conversions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    console.log(`📊 Found ${sortedConversions.length} total conversions`);
    
    return sortedConversions;
}

function getUnattributedConversions(allConversions) {
    const unattributed = allConversions.filter(conv => {
        const hasNoAttribution = !conv.landing_page || 
                                conv.landing_page === '' || 
                                conv.landing_page === 'NO ATTRIBUTION' ||
                                conv.landing_page === null ||
                                conv.landing_page === undefined;
        return hasNoAttribution;
    });
    
    console.log(`🔍 Found ${unattributed.length} unattributed conversions out of ${allConversions.length} total`);
    
    if (unattributed.length > 0) {
        console.log('📋 Unattributed conversions:');
        unattributed.slice(0, 5).forEach((conv, index) => {
            console.log(`   ${index + 1}. [PRIVACY PROTECTED] | ${conv.timestamp}`);
        });
        if (unattributed.length > 5) {
            console.log(`   ... and ${unattributed.length - 5} more`);
        }
    }
    
    return unattributed;
}

// ================================================================
// 5. CONVERSION DATA PREPARATION
// ================================================================

function prepareConversionDataForDeep5(conversion) {
    const conversionAge = Math.floor((Date.now() - new Date(conversion.timestamp)) / (1000 * 60 * 60 * 24));
    
    return {
        email: conversion.email,
        timestamp: conversion.timestamp,
        
        // Enhanced parameters for 8-tier system
        SSID: conversion.session_id,
        PIP: conversion.primary_ip,
        CIP: conversion.conversion_ip || conversion.ip_address,
        IP: conversion.ip_address,
        
        // Device signatures
        dsig: conversion.device_signature,
        SVV: conversion.screen_resolution ? hashString(conversion.screen_resolution) : null,
        gsig: conversion.webgl_hash || (conversion.webgl_fingerprint ? hashString(conversion.webgl_fingerprint) : null),
        
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
        is_legacy_conversion: conversionAge > 30,
        processing_type: 'deep_dive_24h'
    };
}

function hashString(str) {
    if (!str) return null;
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
        const char = str.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash;
    }
    return Math.abs(hash).toString(36);
}

// ================================================================
// 6. ENHANCED ATTRIBUTION ANALYSIS (8-TIER SYSTEM)
// ================================================================

async function analyzeConversionForAttributionDeep5(conversion, pageviews, conversionData, geoDataCache, cacheStats) {
    console.log('   🔬 Using enhanced 8-tier attribution system with 24-hour window...');
    
    const results = {
        conversionEmail: conversion.email,
        originalAttribution: null,
        matchFound: false,
        newAttribution: null,
        shouldUpdate: false,
        improvementType: 'NO_CHANGE',
        attributionMethod: 'none',
        priorityLevel: 9,
        match: null,
        analysis: {
            conversion_type: conversionData.has_enhanced_params ? 'enhanced' : 'legacy',
            conversion_age_days: conversionData.conversion_age_days,
            processing_path: 'unknown',
            window_size: '24_hours'
        }
    };
    
    // Try enhanced 8-tier system first (Priorities 1-7: direct Redis lookups)
    // Always attempt all available priorities regardless of enhanced param status
    console.log('   🚀 Trying enhanced 8-tier attribution system (Priorities 1-7)...');
    results.analysis.processing_path = '8_tier_direct_lookups';
    
    const enhancedResult = await findEnhancedAttribution(conversionData);
    
    if (enhancedResult) {
        results.matchFound = true;
        results.newAttribution = enhancedResult.landing_page || enhancedResult.url;
        results.match = enhancedResult;
        results.attributionMethod = enhancedResult.method;
        results.priorityLevel = getPriorityLevel(enhancedResult.method);
        results.shouldUpdate = true;
        results.improvementType = 'NEW_ATTRIBUTION_DIRECT';
        
        console.log(`   ✅ Direct match found: ${enhancedResult.method} (Priority ${results.priorityLevel})`);
        return results;
    }
    
    // Priority 8: Geographic correlation with 24-hour window
    console.log('   🌍 Priority 8: Geographic correlation with 24-hour window...');
    results.analysis.processing_path = 'geographic_correlation_24h';
    
    const geoResult = await performGeographicCorrelation24Hour(conversion, pageviews, conversionData, geoDataCache, cacheStats);
    
    if (geoResult) {
        results.matchFound = true;
        results.newAttribution = geoResult.newAttribution;
        results.match = geoResult.match;
        results.attributionMethod = geoResult.method || 'geo_correlation_24h';
        results.priorityLevel = 8;
        results.shouldUpdate = true;
        results.improvementType = 'NEW_ATTRIBUTION_24H';
        
        console.log(`   ✅ 24-hour geographic correlation successful: ${results.attributionMethod}`);
    } else {
        console.log('   ❌ No attribution found via any method (including 24-hour window)');
        results.improvementType = 'NO_MATCH_FOUND_24H';
    }
    
    return results;
}

// 8-tier enhanced attribution system
async function findEnhancedAttribution(conversionData) {
    console.log('      🎯 Starting 8-tier attribution search...');
    
    // Priority 1: Session ID Match (300 points)
    if (conversionData.SSID) {
        console.log('      🔍 Priority 1: Trying SSID match:', conversionData.SSID);
        const sessionKey = `attribution_session_${conversionData.SSID}`;
        const sessionResult = await redisRequest('get', sessionKey);
        
        if (sessionResult) {
            const attributionResult = await redisRequest('get', sessionResult);
            if (attributionResult) {
                const attrData = JSON.parse(attributionResult);
                console.log('      ✅ Priority 1: SSID match found - highest confidence');
                return {
                    method: 'ssid_direct_match',
                    score: 300,
                    landing_page: attrData.landing_page,
                    ...attrData
                };
            }
        }
        console.log('      ⚠️ Priority 1: SSID lookup failed');
    } else {
        console.log('      ⚠️ Priority 1: No SSID available for matching');
    }
    
    // Priority 2: Primary IP Match (280 points)
    if (conversionData.PIP) {
        console.log('      🔍 Priority 2: Trying Primary IP match:', conversionData.PIP);
        const pipKey = `attribution_ip_${encodeIPForKey(conversionData.PIP)}`;
        const pipResult = await redisRequest('get', pipKey);
        
        if (pipResult) {
            const attributionResult = await redisRequest('get', pipResult);
            if (attributionResult) {
                const attrData = JSON.parse(attributionResult);
                console.log('      ✅ Priority 2: Primary IP match found');
                return {
                    method: 'primary_ip_match',
                    score: 280,
                    landing_page: attrData.landing_page,
                    ...attrData
                };
            }
        }
        console.log('      ⚠️ Priority 2: Primary IP lookup failed');
    } else {
        console.log('      ⚠️ Priority 2: No Primary IP available for matching');
    }
    
    // Priority 3: Conversion IP Match (260 points)
    if (conversionData.CIP) {
        console.log('      🔍 Priority 3: Trying Conversion IP match:', conversionData.CIP);
        const cipKey = `attribution_ip_${encodeIPForKey(conversionData.CIP)}`;
        const cipResult = await redisRequest('get', cipKey);
        
        if (cipResult) {
            const attributionResult = await redisRequest('get', cipResult);
            if (attributionResult) {
                const attrData = JSON.parse(attributionResult);
                console.log('      ✅ Priority 3: Conversion IP match found');
                return {
                    method: 'conversion_ip_match',
                    score: 260,
                    landing_page: attrData.landing_page,
                    ...attrData
                };
            }
        }
        console.log('      ⚠️ Priority 3: Conversion IP lookup failed');
    } else {
        console.log('      ⚠️ Priority 3: No Conversion IP available for matching');
    }
    
    // Priority 4: Pageview IP Match (240 points)
    if (conversionData.IP) {
        console.log('      🔍 Priority 4: Trying Pageview IP match:', conversionData.IP);
        const ipKey = `attribution_ip_${encodeIPForKey(conversionData.IP)}`;
        const ipResult = await redisRequest('get', ipKey);
        
        if (ipResult) {
            const attributionResult = await redisRequest('get', ipResult);
            if (attributionResult) {
                const attrData = JSON.parse(attributionResult);
                console.log('      ✅ Priority 4: Pageview IP match found');
                return {
                    method: 'pageview_ip_match',
                    score: 240,
                    landing_page: attrData.landing_page,
                    ...attrData
                };
            }
        }
        console.log('      ⚠️ Priority 4: Pageview IP lookup failed');
    } else {
        console.log('      ⚠️ Priority 4: No Pageview IP available for matching');
    }
    
    // Priority 5: Device Signature Match (220 points)
    if (conversionData.dsig) {
        console.log('      🔍 Priority 5: Trying Device signature match:', conversionData.dsig);
        const deviceKey = `attribution_fp_${conversionData.dsig}`;
        const deviceResult = await redisRequest('get', deviceKey);
        
        if (deviceResult) {
            const attributionResult = await redisRequest('get', deviceResult);
            if (attributionResult) {
                const attrData = JSON.parse(attributionResult);
                console.log('      ✅ Priority 5: Device signature match found');
                return {
                    method: 'device_signature_match',
                    score: 220,
                    landing_page: attrData.landing_page,
                    ...attrData
                };
            }
        }
        console.log('      ⚠️ Priority 5: Device signature lookup failed');
    } else {
        console.log('      ⚠️ Priority 5: No Device signature available for matching');
    }
    
    // Priority 6: Screen Hash Match (200 points)
    if (conversionData.SVV) {
        console.log('      🔍 Priority 6: Trying Screen hash match:', conversionData.SVV);
        const screenKey = `attribution_screen_${conversionData.SVV}`;
        const screenResult = await redisRequest('get', screenKey);
        
        if (screenResult) {
            const attributionResult = await redisRequest('get', screenResult);
            if (attributionResult) {
                const attrData = JSON.parse(attributionResult);
                console.log('      ✅ Priority 6: Screen hash match found');
                return {
                    method: 'screen_hash_match',
                    score: 200,
                    landing_page: attrData.landing_page,
                    ...attrData
                };
            }
        }
        console.log('      ⚠️ Priority 6: Screen hash lookup failed');
    } else {
        console.log('      ⚠️ Priority 6: No Screen hash available for matching');
    }
    
    // Priority 7: WebGL Signature Match (180 points)
    if (conversionData.gsig) {
        console.log('      🔍 Priority 7: Trying WebGL signature match:', conversionData.gsig);
        const webglKey = `attribution_webgl_${conversionData.gsig}`;
        const webglResult = await redisRequest('get', webglKey);
        
        if (webglResult) {
            const attributionResult = await redisRequest('get', webglResult);
            if (attributionResult) {
                const attrData = JSON.parse(attributionResult);
                console.log('      ✅ Priority 7: WebGL signature match found');
                return {
                    method: 'webgl_signature_match',
                    score: 180,
                    landing_page: attrData.landing_page,
                    ...attrData
                };
            }
        }
        console.log('      ⚠️ Priority 7: WebGL signature lookup failed');
    } else {
        console.log('      ⚠️ Priority 7: No WebGL signature available for matching');
    }
    
    console.log('      ❌ All 7 direct attribution methods exhausted - no matches found');
    return null;
}

function getPriorityLevel(method) {
    const priorities = {
        'ssid_direct_match': 1,
        'primary_ip_match': 2,
        'conversion_ip_match': 3,
        'pageview_ip_match': 4,
        'device_signature_match': 5,
        'screen_hash_match': 6,
        'webgl_signature_match': 7,
        'geo_correlation': 8,
        'geo_high_confidence': 8,
        'geo_medium_confidence': 8
    };
    return priorities[method] || 9;
}

function encodeIPForKey(ip) {
    return ip ? ip.replace(/:/g, '_') : '';
}

// ================================================================
// 7. COMPLETE GEOGRAPHIC CORRELATION IMPLEMENTATION (24-HOUR)
// ================================================================

async function performGeographicCorrelation24Hour(conversion, pageviews, conversionData, geoDataCache, cacheStats) {
    console.log('      🌍 Starting 24-hour cached geographic correlation...');
    
    // Get the best IP for correlation
    const testIPs = [conversionData.PIP, conversionData.CIP, conversionData.IP].filter(Boolean);
    
    if (testIPs.length === 0) {
        console.log('      ❌ No IPs available for geographic correlation');
        return null;
    }
    
    // Find pageviews in 24-hour window before conversion
    const candidatePageviews = findPageviewsIn24HourWindow(conversion, pageviews);
    
    if (candidatePageviews.length === 0) {
        console.log('      ❌ No pageviews found in 24-hour window before conversion');
        return null;
    }
    
    console.log(`      📱 Found ${candidatePageviews.length} pageviews in 24-hour window`);
    
    // Test each customer IP for geographic correlation
    for (const customerIP of testIPs) {
        console.log(`      🌍 Testing geographic correlation for: ${customerIP}`);
        
        // Get geographic data for conversion IP (using cache)
        const conversionGeo = await getOrFetchGeoData(customerIP, geoDataCache, cacheStats);
        
        console.log(`      🌍 Conversion geo: ${conversionGeo.city}, ${conversionGeo.region} (${conversionGeo.isp})`);

        if (conversionGeo.city === 'LOOKUP_FAILED') {
            console.log('      ❌ Geographic lookup failed, trying next IP');
            continue;
        }

        // Find the best temporal match with geographic correlation (24-hour window)
        const bestMatch = await findBestTemporalMatch24Hour(conversion, candidatePageviews, conversionGeo, geoDataCache, cacheStats);
        
        if (bestMatch) {
            console.log(`      ✅ 24-hour geographic correlation successful: ${bestMatch.confidence} (score: ${Math.round(bestMatch.score)})`);
            
            return {
                newAttribution: bestMatch.pageview.landing_page || bestMatch.pageview.url,
                match: bestMatch,
                method: bestMatch.confidence === 'HIGH_CONFIDENCE' ? 'geo_high_confidence_24h' : 
                       bestMatch.confidence === 'MEDIUM_CONFIDENCE' ? 'geo_medium_confidence_24h' : 'geo_correlation_24h'
            };
        }
    }
    
    console.log('      ❌ 24-hour geographic correlation failed for all IPs');
    return null;
}

// Find pageviews in 24-hour window before conversion
function findPageviewsIn24HourWindow(conversion, pageviews) {
    const conversionTime = new Date(conversion.timestamp);
    const windowStart = new Date(conversionTime.getTime() - 24 * 60 * 60 * 1000); // 24 hours before
    
    console.log(`      🔍 DEBUGGING 24-hour window filter:`);
    console.log(`      📅 Conversion time: ${conversion.timestamp}`);
    console.log(`      📅 Window start: ${windowStart.toISOString()}`);
    console.log(`      📅 Window end: ${conversionTime.toISOString()}`);
    console.log(`      📊 Total pageviews to check: ${pageviews.length}`);
    
    // Debug: Check how many pageviews have IP addresses
    const pageviewsWithIP = pageviews.filter(pv => pv.ip_address);
    const pageviewsWithoutIP = pageviews.filter(pv => !pv.ip_address);
    console.log(`      📊 Pageviews with IP: ${pageviewsWithIP.length}, without IP: ${pageviewsWithoutIP.length}`);
    
    // Debug: Check how many are in time window (regardless of IP)
    const pageviewsInTimeWindow = pageviews.filter(pv => {
        const pvTime = new Date(pv.timestamp);
        return pvTime >= windowStart && pvTime <= conversionTime;
    });
    console.log(`      📊 Pageviews in time window (all): ${pageviewsInTimeWindow.length}`);
    
    // Debug: Check how many have IP AND are in time window
    const candidatePageviews = pageviews.filter(pv => {
        const pvTime = new Date(pv.timestamp);
        const inTimeWindow = pvTime >= windowStart && pvTime <= conversionTime;
        const hasIP = pv.ip_address;
        
        // Log first few that fail each filter
        if (!inTimeWindow && pageviewsInTimeWindow.length < 50) {
            console.log(`      ⏰ Outside time window: ${pv.timestamp} (${pvTime.toISOString()})`);
        }
        if (inTimeWindow && !hasIP && pageviewsWithoutIP.length < 10) {
            console.log(`      🚫 In time window but no IP: ${pv.timestamp}`);
        }
        
        return inTimeWindow && hasIP;
    });
    
    console.log(`      ✅ Final candidates (in window + has IP): ${candidatePageviews.length}`);
    
    // Sort by timestamp DESCENDING (newest first = closest to conversion)
    candidatePageviews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    
    return candidatePageviews;
}

// Find best temporal match with cached geographic data (24-hour version)
async function findBestTemporalMatch24Hour(conversion, candidatePageviews, conversionGeoData, geoDataCache, cacheStats) {
    for (let i = 0; i < candidatePageviews.length; i++) {
        const pageview = candidatePageviews[i];
        const timeDiff = Math.abs(new Date(conversion.timestamp) - new Date(pageview.timestamp)) / 1000 / 60;
        
        // Get pageview geographic data (using cache)
        const pageviewGeoData = await getOrFetchGeoData(pageview.ip_address, geoDataCache, cacheStats);
        
        // Compare geographic data with enhanced scoring
        const geoMatch = compareGeographicDataEnhanced(conversionGeoData, pageviewGeoData);
        
        if (geoMatch.isMatch) {
            console.log(`      🏆 TEMPORAL MATCH: ${timeDiff.toFixed(1)}min before (${geoMatch.confidence})`);
            
            return {
                pageview: pageview,
                score: geoMatch.score,
                timeDiff: timeDiff,
                confidence: geoMatch.confidence,
                conversionGeo: conversionGeoData,
                pageviewGeo: pageviewGeoData,
                candidateNumber: i + 1
            };
        }
        
        // Progress logging for large datasets
        if ((i + 1) % 25 === 0) {
            console.log(`      📊 Checked ${i + 1}/${candidatePageviews.length} pageviews`);
        }
    }
    
    return null;
}

// Enhanced geographic data fetching with caching
async function getCachedGeoData(ip, geoDataCache, cacheStats) {
    if (!ip || ip === 'unknown') return null;
    
    // Check in-memory cache first (for current batch run)
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
        
        if (cachedResult) {
            const geoData = JSON.parse(decodeURIComponent(cachedResult));
            geoDataCache.set(ip, geoData); // Store in memory for this run
            cacheStats.redis_hits++;
            console.log(`   📦 Using Redis cache for ${ip}: ${geoData.city}, ${geoData.region}`);
            return geoData;
        }
        
        // Check if we already have geo data in existing attribution records
        const ipKey = `attribution_ip_${encodedIP}`;
        const attrKeyResult = await redisRequest('get', ipKey);
        
        if (attrKeyResult) {
            const attrResult = await redisRequest('get', attrKeyResult);
            if (attrResult) {
                const attrData = JSON.parse(attrResult);
                if (attrData.geographic_data) {
                    geoDataCache.set(ip, attrData.geographic_data);
                    cacheStats.redis_hits++;
                    console.log(`   🔗 Using geo data from attribution record for ${ip}`);
                    return attrData.geographic_data;
                }
            }
        }
        
        cacheStats.misses++;
        return null;
        
    } catch (error) {
        console.log(`   ⚠️ Cache lookup failed for ${ip}: ${error.message}`);
        cacheStats.misses++;
        return null;
    }
}

async function getOrFetchGeoData(ip, geoDataCache, cacheStats) {
    // Try cache first
    let geoData = await getCachedGeoData(ip, geoDataCache, cacheStats);
    if (geoData) return geoData;
    
    // Only make API call if absolutely necessary and within rate limits
    if (cacheStats.api_calls < 50) { // Increased limit for deep dive analysis
        try {
            const ipinfoToken = process.env.IPINFO_TOKEN;
            if (!ipinfoToken) return getFailedLookupData(ip);
            
            console.log(`   🌍 Making IPinfo API call for ${ip} (${cacheStats.api_calls + 1}/50)`);
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
                await redisRequest('setex', cacheKey, 86400, encodeURIComponent(JSON.stringify(geoData)));
                
                cacheStats.api_calls++;
                console.log(`   ✅ Cached geo data for ${ip}: ${geoData.city}, ${geoData.region} (${geoData.isp})`);
                return geoData;
            }
        } catch (error) {
            console.log(`   ❌ IPinfo API call failed for ${ip}: ${error.message}`);
        }
    } else {
        console.log(`   ⚠️ Skipping API call for ${ip} - rate limit reached (${cacheStats.api_calls}/50)`);
    }
    
    return getFailedLookupData(ip);
}

function getFailedLookupData(ip) {
    return {
        ip: ip || 'unknown',
        city: 'LOOKUP_FAILED',
        region: 'LOOKUP_FAILED', 
        country: 'LOOKUP_FAILED',
        isp: 'LOOKUP_FAILED',
        coordinates: '0,0',
        timezone: 'Unknown'
    };
}

function extractBestISP(data) {
    if (data.company?.name) return data.company.name;
    if (data.asn?.name) return data.asn.name;
    if (data.org) return data.org;
    if (data.carrier?.name) return data.carrier.name;
    return 'Unknown';
}

// Enhanced geographic scoring
function compareGeographicDataEnhanced(conversionGeo, pageviewGeo) {
    if (conversionGeo.city === 'LOOKUP_FAILED' || pageviewGeo.city === 'LOOKUP_FAILED') {
        return { isMatch: false, confidence: 'LOOKUP_FAILED', score: 0 };
    }

    const cityMatch = conversionGeo.city === pageviewGeo.city;
    const regionMatch = conversionGeo.region === pageviewGeo.region;
    const countryMatch = conversionGeo.country === pageviewGeo.country;
    const ispMatch = compareISPs(conversionGeo.isp, pageviewGeo.isp);

    let score = 0;
    
    // ISP + Location combinations (primary correlation method for dual-stack)
    if (conversionGeo.isp !== 'Unknown' && pageviewGeo.isp !== 'Unknown') {
        if (normalizeISP(conversionGeo.isp) === normalizeISP(pageviewGeo.isp)) {
            if (cityMatch) {
                score += 60; // High confidence: same city + ISP
            } else if (regionMatch) {
                score += 40; // Medium confidence: same region + ISP  
            } else if (countryMatch) {
                score += 20; // Low confidence: same country + ISP
            }
        }
    }

    // Geographic-only fallbacks (lower confidence)
    if (cityMatch && conversionGeo.city !== 'Unknown') {
        score += 20; // Same city bonus
    }
    if (regionMatch && conversionGeo.region !== 'Unknown') {
        score += 10; // Same region bonus
    }

    let confidence = 'NO_MATCH';
    let isMatch = false;

    // Enhanced thresholds for geographic correlation
    if (score >= 80) {
        confidence = 'HIGH_CONFIDENCE';
        isMatch = true;
    } else if (score >= 60) {
        confidence = 'MEDIUM_CONFIDENCE';
        isMatch = true;
    } else if (score >= 40) {
        confidence = 'LOW_CONFIDENCE';
        isMatch = true; // Accept lower threshold since it's final fallback
    }

    return {
        isMatch,
        confidence,
        score,
        cityMatch,
        regionMatch,
        countryMatch,
        ispMatch
    };
}

// Compare ISPs with normalization
function compareISPs(isp1, isp2) {
    if (!isp1 || !isp2 || isp1 === 'Unknown' || isp2 === 'Unknown') return false;
    return normalizeISP(isp1) === normalizeISP(isp2);
}

// Normalize ISP names for better matching
function normalizeISP(isp) {
    if (!isp || isp === 'Unknown') return '';
    
    const normalized = isp.toLowerCase().replace(/[^a-z0-9]/g, '');
    
    // Handle common ISP name variations
    if (normalized.includes('twc') || normalized.includes('timewarner') || normalized.includes('spectruminternet')) {
        return 'timewarner';
    }
    if (normalized.includes('comcast') || normalized.includes('xfinity')) {
        return 'comcast';
    }
    if (normalized.includes('verizon') || normalized.includes('vzw')) {
        return 'verizon';
    }
    
    return normalized;
}

// ================================================================
// 8. CONVERSION UPDATE FUNCTIONS
// ================================================================

async function updateConversionAttributionDeep5(conversion, improvementResults) {
    try {
        const conversionKey = await findConversionKey(conversion);
        
        if (conversionKey) {
            const existingData = await redisRequest('get', conversionKey);
            const conversionData = existingData ? (typeof existingData === 'string' ? JSON.parse(existingData) : existingData) : conversion;
            
            const updatedConversion = {
                ...conversionData,
                attribution_found: true,
                landing_page: improvementResults.newAttribution,
                source: improvementResults.match.pageview?.source || improvementResults.match.source || 'deep5_enhanced',
                utm_campaign: improvementResults.match.pageview?.utm_campaign || improvementResults.match.utm_campaign || conversionData.utm_campaign,
                utm_medium: improvementResults.match.pageview?.utm_medium || improvementResults.match.utm_medium || conversionData.utm_medium,
                referrer_url: improvementResults.match.pageview?.referrer_url || improvementResults.match.referrer_url || conversionData.referrer_url,
                
                attribution_improvement: {
                    method: improvementResults.attributionMethod,
                    improvement_type: improvementResults.improvementType,
                    priority_level: improvementResults.priorityLevel,
                    confidence: improvementResults.match.confidence || 'medium',
                    score: improvementResults.match.score || 0,
                    time_difference_minutes: improvementResults.match.timeDiff || 0,
                    improved_at: new Date().toISOString(),
                    system_version: '5.0',
                    processing_path: improvementResults.analysis?.processing_path || 'deep5_enhanced',
                    window_size: '24_hours',
                    
                    pageview_ip: improvementResults.match.pageview?.ip_address || improvementResults.match.ip_address,
                    pageview_timestamp: improvementResults.match.pageview?.timestamp || improvementResults.match.timestamp,
                    matched_ip_type: improvementResults.match.matched_ip || 'unknown'
                },
                
                previous_attribution: conversionData.landing_page || null,
                attributed_pageview_timestamp: improvementResults.match.pageview?.timestamp || improvementResults.match.timestamp || new Date().toISOString()
            };
            
            await redisRequest('set', conversionKey, JSON.stringify(updatedConversion));
            
            return {
                success: true,
                updated_attribution: improvementResults.newAttribution,
                previous_attribution: conversionData.landing_page || null,
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

async function findConversionKey(conversion) {
    const timeoutMs = 10000;
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
            if (Date.now() - startTime > timeoutMs) {
                console.log(`   ⏰ Key search timeout after ${timeoutMs}ms`);
                break;
            }
            
            try {
                const keys = await redisRequest('keys', pattern);
                if (keys && keys.length > 0) {
                    for (const key of keys) {
                        if (Date.now() - startTime > timeoutMs) break;
                        
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
                continue;
            }
        }
        
        const newKey = `conversion_${conversion.email}_${Date.now()}`;
        await redisRequest('set', newKey, JSON.stringify(conversion));
        return newKey;
        
    } catch (error) {
        console.error(`❌ Error finding conversion key:`, error);
        return null;
    }
}

// ================================================================
// 9. MAIN HANDLER FUNCTION
// ================================================================

exports.handler = async (event, context) => {
    const headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Content-Type': 'application/json'
    };

    if (event.httpMethod === 'OPTIONS') {
        return { statusCode: 200, headers, body: '' };
    }

    try {
        console.log('🔍 Starting DEEP DIVE ATTRIBUTION SYSTEM v5.0 - Complete Priority Coverage');
        console.log('⚡ Enhanced: Complete 8-tier priority system + 24-hour geo correlation + comprehensive error handling');
        
        const startTime = Date.now();
        
        // Validate environment variables
        const requiredEnvVars = ['UPSTASH_REDIS_REST_URL', 'UPSTASH_REDIS_REST_TOKEN', 'OJOY_API_KEY'];
        const missingEnvVars = requiredEnvVars.filter(envVar => !process.env[envVar]);
        
        if (missingEnvVars.length > 0) {
            console.error('❌ Missing environment variables:', missingEnvVars);
            return {
                statusCode: 500,
                headers,
                body: JSON.stringify({
                    success: false,
                    error: 'Configuration error',
                    missing_env_vars: missingEnvVars
                })
            };
        }
        
        // Initialize caching
        let geoDataCache = new Map();
        let cacheStats = { hits: 0, misses: 0, api_calls: 0, redis_hits: 0 };
        
        // Step 1: Fetch analytics data
        const analyticsData = await fetchAnalyticsDataPast7Days();
        
        // Step 2: Get conversions and filter
        const allConversions = getAllConversions(analyticsData.conversions);
        const unattributedConversions = getUnattributedConversions(allConversions);
        
        // Step 3: Filter out already processed deep5 conversions
        const unprocessedDeep5Conversions = await filterNonDeep5Conversions(unattributedConversions);
        
        if (unprocessedDeep5Conversions.length === 0) {
            const message = unattributedConversions.length > 0 
                ? `🎯 All ${unattributedConversions.length} unattributed conversions have been processed with deep5 system`
                : '🎉 All conversions have attribution! No unattributed conversions found';
                
            return {
                statusCode: 200,
                headers,
                body: JSON.stringify({
                    success: true,
                    message,
                    results: { 
                        total_conversions: allConversions.length,
                        unattributed: unattributedConversions.length,
                        unprocessed_deep5: 0,
                        status: 'ALL_DEEP5_PROCESSED'
                    }
                })
            };
        }

        console.log(`🎯 Found ${unprocessedDeep5Conversions.length} not yet processed with deep5 system`);
        
        // Step 4: Process first conversion
        const conversionToProcess = unprocessedDeep5Conversions[0];
        console.log(`🔍 Processing conversion: [PRIVACY PROTECTED] from ${conversionToProcess.timestamp}`);
        console.log(`   🔍 Using enhanced 8-tier system with 24-hour window (deep5 version)`);
        
        // Step 5: Prepare conversion data
        const conversionData = prepareConversionDataForDeep5(conversionToProcess);
        
        console.log(`   📊 Conversion: ${conversionData.has_enhanced_params ? 'ENHANCED' : 'LEGACY'} (age: ${conversionData.conversion_age_days} days)`);
        console.log(`   📍 IPs: PIP=${conversionData.PIP || 'none'}, CIP=${conversionData.CIP || 'none'}, IP=${conversionData.IP || 'none'}`);
        console.log(`   🔐 Signatures: SSID=${!!conversionData.SSID}, dsig=${!!conversionData.dsig}, SVV=${!!conversionData.SVV}`);
        
        // Step 6: Analyze for attribution
        const improvementResults = await analyzeConversionForAttributionDeep5(
            conversionToProcess, 
            analyticsData.page_views, 
            conversionData,
            geoDataCache,
            cacheStats
        );
        
        // Step 7: Update if needed
        let updateResult = { success: false };
        if (improvementResults.matchFound && improvementResults.shouldUpdate) {
            console.log(`📝 Updating attribution for conversion...`);
            try {
                updateResult = await updateConversionAttributionDeep5(conversionToProcess, improvementResults);
                console.log(`   ✅ Attribution updated: ${improvementResults.attributionMethod}`);
            } catch (error) {
                console.error(`   ❌ Failed to update conversion: ${error.message}`);
                updateResult = { success: false, error: error.message };
            }
        }
        
        // Step 8: Mark as processed with deep5
        await markConversionAsDeep5(conversionToProcess, improvementResults.attributionMethod || 'none');
        
        // Step 9: Generate response
        const remainingUnprocessedDeep5 = unprocessedDeep5Conversions.length - 1;
        const processingTime = Date.now() - startTime;
        
        let summaryMessage;
        if (improvementResults.matchFound) {
            summaryMessage = `✅ Found attribution using ${improvementResults.attributionMethod} (Priority ${improvementResults.priorityLevel})! ${remainingUnprocessedDeep5} conversions remaining.`;
        } else {
            summaryMessage = `❌ No attribution found even with complete 8-tier system + 24-hour geo correlation. ${remainingUnprocessedDeep5} conversions remaining.`;
        }
        
        console.log(`\n🏁 DEEP DIVE v5.0 COMPLETE:`);
        console.log(`   📧 Processed: [PRIVACY PROTECTED]`);
        console.log(`   ✅ Success: ${improvementResults.matchFound ? 'YES' : 'NO'}`);
        console.log(`   ⚡ Time: ${processingTime}ms`);
        console.log(`   📊 Cache: ${cacheStats.hits} hits, ${cacheStats.misses} misses, ${cacheStats.api_calls} API calls`);
        
        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({
                success: true,
                message: summaryMessage,
                results: {
                    total_conversions: allConversions.length,
                    unattributed: unattributedConversions.length,
                    unprocessed_deep5: unprocessedDeep5Conversions.length,
                    processed_this_run: 1,
                    remaining_unprocessed_deep5: remainingUnprocessedDeep5,
                    
                    match_found: improvementResults.matchFound,
                    attribution_method: improvementResults.attributionMethod,
                    improvement_type: improvementResults.improvementType,
                    priority_level: improvementResults.priorityLevel,
                    
                    attribution_updated: updateResult.success,
                    update_error: updateResult.error || null,
                    
                    processing_time_ms: processingTime,
                    cache_stats: cacheStats,
                    
                    status: remainingUnprocessedDeep5 > 0 ? 'MORE_TO_PROCESS' : 'ALL_DEEP5_PROCESSED',
                    next_action: remainingUnprocessedDeep5 > 0 ? 'Press button again to process next conversion' : 'All unattributed conversions have been processed with deep5 system',
                    timeout_protection: 'enabled',
                    retry_logic: 'enabled',
                    geographic_correlation: 'enabled_24h',
                    complete_priority_coverage: 'enabled'
                }
            })
        };
        
    } catch (error) {
        console.error('❌ Deep5 system error:', error);
        
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({
                success: false,
                error: 'Deep5 system error',
                details: error.message,
                stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
            })
        };
    }
};
