exports.handler = async (event, context) => {
    // TEST VERSION: Single Conversion Attribution Recovery - 6 Hour Window
    // Takes the most recent unattributed conversion and checks 6 hours of pageview data
    // Ignores processed status for testing purposes
    
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
        console.log('🧪 Starting TEST: Single Conversion Attribution Recovery (6-Hour Window)');
        console.log('🎯 Testing most recent unattributed conversion with comprehensive pageview analysis');
        
        // Step 1: Fetch analytics data from June 12-18
        const analyticsData = await fetchAnalyticsDataJune1218();
        
        // Step 2: Find unattributed conversions
        const allUnattributedConversions = findUnattributedConversions(analyticsData.conversions);
        
        if (allUnattributedConversions.length === 0) {
            return {
                statusCode: 200,
                headers,
                body: JSON.stringify({
                    success: true,
                    message: 'No unattributed conversions found in June 12-18 period',
                    results: { total: 0, recovered: 0, test_mode: true }
                })
            };
        }
        
        // Step 3: Take the FIRST (most recent) unattributed conversion for testing
        // Sort by timestamp descending to get the most recent first
        allUnattributedConversions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
        const testConversion = allUnattributedConversions[0];
        
        console.log(`🔬 TEST SUBJECT: ${testConversion.email}`);
        console.log(`   📍 Conversion IP: ${testConversion.ip_address}`);
        console.log(`   ⏰ Conversion Time: ${testConversion.timestamp}`);
        console.log(`   📊 Testing 1 of ${allUnattributedConversions.length} unattributed conversions`);
        
        // Step 4: Analyze this single conversion with 6-hour comprehensive window
        const recoveryResults = await analyzeSingleConversionComprehensive(testConversion, analyticsData.page_views);
        
        // Step 5: If match found, optionally update Redis (keeping test mode flag)
        if (recoveryResults.matches.length > 0) {
            console.log(`📝 Match found! Updating Redis with test recovery...`);
            try {
                await updateRecoveredAttributions(recoveryResults.matches, true); // true = test mode
            } catch (redisError) {
                console.error('❌ Redis update failed but recovery succeeded:', redisError);
            }
        }
        
        // Step 6: Return comprehensive test results
        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({
                success: true,
                test_mode: true,
                test_subject: {
                    email: testConversion.email,
                    ip_address: testConversion.ip_address,
                    timestamp: testConversion.timestamp
                },
                results: recoveryResults,
                message: recoveryResults.matches.length > 0 ? 
                    `TEST SUCCESS: Found attribution match for ${testConversion.email} in 6-hour window` :
                    `TEST COMPLETE: No attribution match found for ${testConversion.email} in 6-hour comprehensive search`,
                total_unattributed: allUnattributedConversions.length,
                date_range: 'June 12-18, 2025',
                search_window: '6 hours comprehensive'
            })
        };

    } catch (error) {
        console.error('❌ Test attribution recovery error:', error);
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({
                error: 'Test attribution recovery failed',
                details: error.message,
                test_mode: true
            })
        };
    }
};

// Global counters for cache statistics
let cacheStats = {
    hits: 0,
    misses: 0,
    errors: 0
};

// Analyze single conversion with comprehensive 6-hour window
async function analyzeSingleConversionComprehensive(conversion, pageviews) {
    console.log('🔬 COMPREHENSIVE TEST: Analyzing single conversion with 6-hour pageview window...');
    
    // Reset cache statistics for this test
    cacheStats = { hits: 0, misses: 0, errors: 0 };
    
    const results = {
        total: 1,
        recovered: 0,
        matches: [],
        test_details: {
            conversion_analyzed: conversion.email,
            search_window_minutes: 360, // 6 hours
            total_pageviews_checked: 0,
            ipv6_pageviews_in_window: 0,
            geographic_lookups_performed: 0,
            cache_performance: {}
        }
    };
    
    console.log(`🔍 ANALYZING TEST CONVERSION: ${conversion.email}`);
    console.log(`   📍 Conversion IP: ${conversion.ip_address}`);
    console.log(`   ⏰ Conversion Time: ${conversion.timestamp}`);
    
    // Find ALL pageviews in 6-hour window (360 minutes)
    const candidatePageviews = findAllPageviewsInWindow(conversion, pageviews, 0, 360);
    
    if (candidatePageviews.length === 0) {
        console.log(`   ❌ No pageviews found in 6-hour window`);
        results.test_details.ipv6_pageviews_in_window = 0;
        return results;
    }
    
    console.log(`   📱 Found ${candidatePageviews.length} pageviews in 6-hour window`);
    results.test_details.ipv6_pageviews_in_window = candidatePageviews.length;
    
    // Get geographic data for conversion IP
    console.log(`   🌍 Looking up conversion location...`);
    const conversionGeoData = await getIPLocationData(conversion.ip_address);
    console.log(`   📍 Conversion Location: ${conversionGeoData.city}, ${conversionGeoData.region}, ${conversionGeoData.country} (${conversionGeoData.isp})`);
    
    // Check EVERY pageview for geographic match (comprehensive test)
    console.log(`   🔍 Starting comprehensive geographic matching...`);
    const match = await checkAllPageviewCandidates(conversion, candidatePageviews, conversionGeoData);
    
    if (match) {
        console.log(`   ✅ COMPREHENSIVE TEST: MATCH FOUND!`);
        
        results.matches.push({
            conversion: conversion,
            match: match,
            phase: 'COMPREHENSIVE_6H',
            confidence: match.confidence
        });
        
        results.recovered = 1;
    } else {
        console.log('   ❌ COMPREHENSIVE TEST: No matches found in entire 6-hour window');
    }
    
    // Final cache statistics
    const totalCacheLookups = cacheStats.hits + cacheStats.misses + cacheStats.errors;
    const cacheHitRate = totalCacheLookups > 0 ? ((cacheStats.hits / totalCacheLookups) * 100).toFixed(1) : 0;
    
    results.test_details.cache_performance = {
        cache_hits: cacheStats.hits,
        cache_misses: cacheStats.misses,
        cache_errors: cacheStats.errors,
        cache_hit_rate_percent: cacheHitRate,
        fresh_api_calls_needed: cacheStats.misses
    };
    
    console.log(`📈 COMPREHENSIVE TEST CACHE STATISTICS:`);
    console.log(`   💾 Cache hits: ${cacheStats.hits}`);
    console.log(`   📭 Cache misses: ${cacheStats.misses}`);
    console.log(`   ❌ Cache errors: ${cacheStats.errors}`);
    console.log(`   📊 Cache hit rate: ${cacheHitRate}%`);
    console.log(`   🌍 Fresh API calls needed: ${cacheStats.misses}`);
    
    console.log(`🏁 Comprehensive Test Complete: ${results.recovered}/1 conversion recovered`);
    return results;
}

// Find ALL pageviews (both IPv4 and IPv6) within time window for comprehensive testing
function findAllPageviewsInWindow(conversion, pageviews, startMinutes, endMinutes) {
    const conversionTime = new Date(conversion.timestamp);
    const windowStart = new Date(conversionTime.getTime() - endMinutes * 60 * 1000);
    const windowEnd = new Date(conversionTime.getTime() - startMinutes * 60 * 1000);
    
    console.log(`   🕐 COMPREHENSIVE Search window: ${windowStart.toISOString()} to ${windowEnd.toISOString()}`);
    console.log(`   ⏱️  Window size: ${endMinutes} minutes (${endMinutes/60} hours)`);
    
    // Filter pageviews in time window - including BOTH IPv4 and IPv6 for comprehensive test
    const candidatePageviews = pageviews.filter(pv => {
        const pvTime = new Date(pv.timestamp);
        return pvTime >= windowStart && 
               pvTime <= conversionTime && 
               pv.ip_address; // Any IP address (both IPv4 and IPv6)
    });
    
    // SORT BY TIMESTAMP - NEWEST FIRST (most recent pageviews checked first)
    candidatePageviews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    
    // Count IPv4 vs IPv6 for analysis
    const ipv4Count = candidatePageviews.filter(pv => !pv.ip_address.includes(':')).length;
    const ipv6Count = candidatePageviews.filter(pv => pv.ip_address.includes(':')).length;
    
    console.log(`   📊 Found ${candidatePageviews.length} total pageviews in time window out of ${pageviews.length} total pageviews`);
    console.log(`   🌐 IP breakdown: ${ipv4Count} IPv4, ${ipv6Count} IPv6`);
    console.log(`   🕐 Sorted by timestamp (newest first) - will check most recent matches first`);
    console.log(`   🔬 COMPREHENSIVE TEST: No limits - checking ALL pageviews in 6-hour window`);
    
    return candidatePageviews;
}

// Check ALL pageview candidates for comprehensive testing
async function checkAllPageviewCandidates(conversion, candidatePageviews, conversionGeoData) {
    let freshApiCallCount = 0;
    let totalChecked = 0;
    let bestMatch = null;
    let bestScore = 0;
    
    console.log(`   🔍 COMPREHENSIVE CHECK: Analyzing ${candidatePageviews.length} pageviews...`);
    
    for (let i = 0; i < candidatePageviews.length; i++) {
        const pageview = candidatePageviews[i];
        const timeDiff = Math.abs(new Date(conversion.timestamp) - new Date(pageview.timestamp)) / 1000 / 60;
        const ipType = pageview.ip_address.includes(':') ? 'IPv6' : 'IPv4';
        
        totalChecked++;
        
        console.log(`   🌐 Candidate ${i + 1}/${candidatePageviews.length}: ${pageview.ip_address} (${ipType})`);
        console.log(`      ⏰ Pageview Time: ${pageview.timestamp} (${timeDiff.toFixed(1)} min before)`);
        console.log(`      📄 Landing Page: ${pageview.landing_page || pageview.url || 'Unknown'}`);
        
        // Use unified cache/fresh lookup
        const startTime = Date.now();
        const pageviewGeoData = await getIPLocationData(pageview.ip_address);
        const lookupTime = Date.now() - startTime;
        
        // Count fresh API calls for statistics
        if (lookupTime > 100) { // Likely a fresh API call if it took >100ms
            freshApiCallCount++;
        }
        
        console.log(`      📍 ${ipType} Location: ${pageviewGeoData.city}, ${pageviewGeoData.region}, ${pageviewGeoData.country} (${pageviewGeoData.isp})`);
        
        // Compare geographic data
        const match = compareGeographicData(conversionGeoData, pageviewGeoData);
        
        if (match.isMatch) {
            console.log(`      ✅ GEOGRAPHIC MATCH FOUND! (${match.confidence}, Score: ${match.score})`);
            console.log(`         🎯 City: ${match.cityMatch ? '✓' : '✗'} | Region: ${match.regionMatch ? '✓' : '✗'} | Country: ${match.countryMatch ? '✓' : '✗'} | ISP: ${match.ispMatch ? '✓' : '✗'}`);
            
            // In comprehensive mode, keep checking to find the BEST match
            if (match.score > bestScore) {
                bestScore = match.score;
                bestMatch = {
                    pageview: pageview,
                    score: match.score,
                    timeDiff: timeDiff,
                    confidence: match.confidence,
                    conversionGeo: conversionGeoData,
                    pageviewGeo: pageviewGeoData,
                    ipType: ipType,
                    candidateNumber: i + 1
                };
                console.log(`      🏆 NEW BEST MATCH! Score: ${match.score}`);
            } else {
                console.log(`      ✅ Good match but lower score than current best (${match.score} vs ${bestScore})`);
            }
        } else {
            console.log(`      ❌ No geographic match (${match.confidence}, Score: ${match.score})`);
        }
        
        // Log progress every 10 checks for long lists
        if ((i + 1) % 10 === 0) {
            console.log(`   📊 Progress: ${i + 1}/${candidatePageviews.length} checked, ${freshApiCallCount} API calls so far`);
        }
    }
    
    console.log(`   📊 COMPREHENSIVE CHECK COMPLETE:`);
    console.log(`      🔍 Total pageviews analyzed: ${totalChecked}`);
    console.log(`      🌍 Estimated fresh API calls: ${freshApiCallCount}`);
    console.log(`      🏆 Best match score: ${bestScore}`);
    
    if (bestMatch) {
        console.log(`   🎯 RETURNING BEST MATCH: Candidate #${bestMatch.candidateNumber} (${bestMatch.ipType}) with score ${bestMatch.score}`);
    }
    
    return bestMatch;
}

// NEW: Get cached geographic data using SAME cache structure as track.js
async function getCachedGeoData(ip) {
    try {
        // Use SAME cache key format as track.js IPinfoService
        const cacheKey = `geo_cache:${ip.replace(/:/g, '_')}`;
        
        console.log(`   🔍 Checking track.js style cache for key: ${cacheKey}`);
        const cachedResult = await redisRequest('get', cacheKey);
        
        if (cachedResult === null || cachedResult === undefined) {
            console.log(`   📭 Cache miss: No geo_cache key found for ${ip}`);
            cacheStats.misses++;
            return null;
        }
        
        if (cachedResult) {
            try {
                // Decode the cached data (track.js uses encodeURIComponent)
                const cachedData = JSON.parse(decodeURIComponent(cachedResult));
                console.log(`   💾 Cache HIT for ${ip}: ${cachedData.city}, ${cachedData.region} (${cachedData.isp})`);
                cacheStats.hits++;
                return cachedData;
            } catch (parseError) {
                console.log(`   ❌ Cache parse error for ${ip}: ${parseError.message}`);
                cacheStats.errors++;
                return null;
            }
        }
        
        console.log(`   📭 Cache miss: No usable geo data for ${ip}`);
        cacheStats.misses++;
        return null;
        
    } catch (error) {
        console.log(`   ❌ Cache lookup ERROR for ${ip}: ${error.message}`);
        cacheStats.errors++;
        return null;
    }
}

// MODIFIED: Fetch analytics data for June 12-18 specifically
async function fetchAnalyticsDataJune1218() {
    console.log('📊 Fetching analytics data for June 12-18, 2025...');
    
    // FIXED DATE RANGE for June 12-18 batch
    const startDate = '2025-06-12';
    const endDate = '2025-06-18';
    
    console.log(`📅 Date range: ${startDate} to ${endDate} (June 12-18 test)`);
    
    const params = new URLSearchParams();
    params.append('start_date', startDate);
    params.append('end_date', endDate);
    
    const apiUrl = `https://trackingojoy.netlify.app/.netlify/functions/analytics?${params}`;
    
    console.log(`📡 API Request URL: ${apiUrl}`);
    
    const response = await fetch(apiUrl, {
        headers: {
            'X-API-Key': process.env.OJOY_API_KEY
        }
    });
    
    if (!response.ok) {
        throw new Error(`Analytics API failed: ${response.status} ${response.statusText}`);
    }
    
    const data = await response.json();
    console.log(`✅ Analytics data loaded for ${startDate} to ${endDate}:`);
    console.log(`   📊 Total conversions: ${data.conversions?.length || 0}`);
    console.log(`   📊 Total pageviews: ${data.page_views?.length || 0}`);
    
    if (data.page_views && data.page_views.length > 0) {
        const ipv4Count = data.page_views.filter(pv => pv.ip_address && !pv.ip_address.includes(':')).length;
        const ipv6Count = data.page_views.filter(pv => pv.ip_address && pv.ip_address.includes(':')).length;
        console.log(`🌐 IP Address breakdown - IPv4: ${ipv4Count}, IPv6: ${ipv6Count}`);
    }
    
    return data;
}

// Step 2: Find unattributed conversions (UNCHANGED - identical to original)
function findUnattributedConversions(conversions) {
    if (!conversions || conversions.length === 0) {
        console.log('❌ No conversions found in analytics data');
        return [];
    }
    
    const unattributed = conversions.filter(conv => 
        conv.attribution_found === false || 
        !conv.landing_page || 
        conv.landing_page === ''
    );
    
    console.log(`🚨 Found ${unattributed.length} unattributed conversions out of ${conversions.length} total`);
    
    if (unattributed.length > 0) {
        console.log('📋 Unattributed conversions:');
        unattributed.forEach((conv, index) => {
            console.log(`   ${index + 1}. ${conv.email} | ${conv.ip_address} | ${conv.timestamp}`);
        });
    }
    
    return unattributed;
}

// OPTIMIZED: Get location/ISP data using SAME cache structure as track.js
async function getIPLocationData(ip) {
    // Check cache first using SAME logic as track.js
    const cached = await getCachedGeoData(ip);
    if (cached) return cached;
    
    // Make fresh API call and cache using SAME format as track.js
    const token = process.env.IPINFO_TOKEN || 'dd31c7ae01d4e4';
    const url = `https://ipinfo.io/${ip}?token=${token}`;
    
    try {
        const response = await fetch(url, {
            method: 'GET',
            headers: { 'Accept': 'application/json' },
            signal: AbortSignal.timeout(2000) // 2 second timeout like track.js
        });
        
        if (response.ok) {
            const data = await response.json();
            
            const geoData = {
                ip: data.ip,
                city: data.city || 'Unknown',
                region: data.region || 'Unknown',
                country: data.country || 'Unknown',
                isp: extractBestISP(data),
                coordinates: data.loc || '0,0',
                timezone: data.timezone || 'Unknown',
                lookup_timestamp: new Date().toISOString()
            };
            
            // Cache using SAME format as track.js (24 hours = 86400 seconds)
            try {
                const cacheKey = `geo_cache:${ip.replace(/:/g, '_')}`;
                const encodedData = encodeURIComponent(JSON.stringify(geoData));
                await redisRequest('setex', cacheKey, 86400, encodedData);
                console.log(`   🌍 Fresh lookup + cached for ${ip}: ${geoData.city}, ${geoData.region} (${geoData.isp})`);
            } catch (cacheError) {
                console.log(`   ⚠️ Failed to cache geo data for ${ip}: ${cacheError.message}`);
            }
            
            return geoData;
        } else {
            throw new Error(`HTTP ${response.status}`);
        }
    } catch (error) {
        console.log(`   ⚠️  Failed to lookup ${ip}: ${error.message}`);
        return {
            ip: ip,
            city: 'LOOKUP_FAILED',
            region: 'LOOKUP_FAILED',
            country: 'LOOKUP_FAILED',
            isp: 'LOOKUP_FAILED'
        };
    }
}

// Extract best ISP info using SAME logic as track.js
function extractBestISP(data) {
    // Priority hierarchy for ISP identification (same as track.js)
    if (data.company?.name) return data.company.name;
    if (data.asn?.name) return data.asn.name;
    if (data.org) return data.org;
    if (data.carrier?.name) return data.carrier.name;
    return 'Unknown';
}

// Compare geographic data between conversion and pageview (UNCHANGED - identical to original)
function compareGeographicData(conversionGeo, pageviewGeo) {
    if (conversionGeo.city === 'LOOKUP_FAILED' || pageviewGeo.city === 'LOOKUP_FAILED') {
        return { isMatch: false, confidence: 'LOOKUP_FAILED', score: 0 };
    }

    const cityMatch = conversionGeo.city === pageviewGeo.city;
    const regionMatch = conversionGeo.region === pageviewGeo.region;
    const countryMatch = conversionGeo.country === pageviewGeo.country;
    const ispMatch = compareISPs(conversionGeo.isp, pageviewGeo.isp);

    // Scoring system
    let score = 0;
    if (cityMatch) score += 3;
    if (regionMatch) score += 2;
    if (countryMatch) score += 1;
    if (ispMatch) score += 2;

    // Determine confidence level
    let confidence = 'NO_MATCH';
    let isMatch = false;

    if (score >= 5) {
        confidence = 'DEFINITE';
        isMatch = true;
    } else if (score >= 4) {
        confidence = 'STRONG';
        isMatch = true;
    } else if (score >= 3) {
        confidence = 'POSSIBLE';
        isMatch = true;
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

// Compare ISP names (UNCHANGED - identical to original)
function compareISPs(isp1, isp2) {
    if (!isp1 || !isp2 || isp1 === 'Unknown' || isp2 === 'Unknown') return false;
    
    const normalize = (str) => str.toLowerCase().replace(/[^a-z0-9]/g, '');
    const norm1 = normalize(isp1);
    const norm2 = normalize(isp2);
    
    // Exact match
    if (norm1 === norm2) return true;
    
    // Contains match
    if (norm1.includes(norm2) || norm2.includes(norm1)) return true;
    
    // ASN match
    const asn1 = isp1.match(/AS(\d+)/);
    const asn2 = isp2.match(/AS(\d+)/);
    if (asn1 && asn2 && asn1[1] === asn2[1]) return true;
    
    return false;
}

// Helper function to make Redis HTTP requests (FIXED - proper error handling for missing keys)
async function redisRequest(command, ...args) {
    const url = process.env.UPSTASH_REDIS_REST_URL;
    const token = process.env.UPSTASH_REDIS_REST_TOKEN;
    
    if (!url || !token) {
        throw new Error('Missing Redis configuration');
    }
    
    let response;
    
    try {
        // For complex commands like SET with JSON, use POST with body
        if ((command.toLowerCase() === 'set' || command.toLowerCase() === 'setex') && args.length >= 2) {
            response = await fetch(url, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify([command, ...args])
            });
        } 
        // For simple commands like GET, KEYS, DEL, use URL path
        else {
            // Properly encode Redis key for URL
            const encodedArgs = args.map(arg => encodeURIComponent(arg));
            const requestUrl = `${url}/${command}/${encodedArgs.join('/')}`;
            
            response = await fetch(requestUrl, {
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'Content-Type': 'application/json'
                }
            });
        }
        
        if (!response.ok) {
            // Don't throw for 404-like responses - Redis returns these for missing keys
            if (response.status === 404) {
                return null;
            }
            throw new Error(`Redis request failed: ${response.status} ${response.statusText}`);
        }
        
        const data = await response.json();
        return data.result;
        
    } catch (error) {
        // Log the actual request details for debugging
        console.log(`   🔧 Redis request debug: ${command} ${args.join(' ')} -> Error: ${error.message}`);
        throw error;
    }
}

// Update recovered attributions in Redis with test mode flag
async function updateRecoveredAttributions(matches, testMode = false) {
    console.log(`📝 Updating ${matches.length} recovered attributions in Redis${testMode ? ' (TEST MODE)' : ''}...`);
    
    for (const match of matches) {
        const conversion = match.conversion;
        const pageview = match.match.pageview;
        
        try {
            console.log(`🔄 Updating ${conversion.email}...`);
            
            // Try to find the conversion record in Redis by searching different key patterns
            const conversionKey = await findConversionKey(conversion);
            
            if (conversionKey) {
                // Get the existing conversion data
                const existingData = await redisRequest('get', conversionKey);
                let conversionData = typeof existingData === 'string' ? JSON.parse(existingData) : existingData;
                
                // Update with recovered attribution
                const updatedConversion = {
                    ...conversionData,
                    attribution_found: true,
                    landing_page: pageview.landing_page || pageview.url,
                    source: pageview.source || 'recovered',
                    utm_campaign: pageview.utm_campaign || conversionData.utm_campaign,
                    utm_medium: pageview.utm_medium || conversionData.utm_medium,
                    referrer_url: pageview.referrer_url || conversionData.referrer_url,
                    recovery_method: testMode ? 'TEST_comprehensive_6h' : 'comprehensive_6h',
                    recovery_phase: match.phase,
                    recovery_confidence: match.confidence,
                    recovery_score: match.match.score,
                    recovery_timestamp: new Date().toISOString(),
                    recovery_match_ip: pageview.ip_address,
                    recovery_ip_type: match.match.ipType,
                    test_mode: testMode
                };
                
                // Save back to Redis (using fixed POST method)
                await redisRequest('set', conversionKey, JSON.stringify(updatedConversion));
                
                console.log(`✅ Updated ${conversion.email}: ${pageview.landing_page} (${match.phase}${testMode ? ' - TEST' : ''})`);
            } else {
                console.log(`⚠️ Could not find Redis key for ${conversion.email}`);
            }
            
        } catch (error) {
            console.log(`❌ Failed to update ${conversion.email}: ${error.message}`);
        }
    }
    
    console.log(`📝 Redis update complete for ${matches.length} attributions${testMode ? ' (TEST MODE)' : ''}`);
}

// Find the Redis key for a specific conversion (UNCHANGED - identical to original)
async function findConversionKey(conversion) {
    try {
        // Try different possible key patterns (including the actual format seen in logs)
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
            try {
                const keys = await redisRequest('keys', pattern);
                if (keys && keys.length > 0) {
                    // If multiple keys, try to find the exact match
                    for (const key of keys) {
                        const data = await redisRequest('get', key);
                        if (data) {
                            const parsed = typeof data === 'string' ? JSON.parse(data) : data;
                            if (parsed.email === conversion.email && 
                                Math.abs(new Date(parsed.timestamp) - new Date(conversion.timestamp)) < 60000) {
                                console.log(`🔍 Found conversion key: ${key}`);
                                return key;
                            }
                        }
                    }
                }
            } catch (error) {
                // Continue trying other patterns
                console.log(`   ⚠️ Pattern ${pattern} failed: ${error.message}`);
            }
        }
        
        // If no existing key found, create a new one
        const newKey = `conversion_${conversion.email}_${Date.now()}`;
        console.log(`🆕 Creating new conversion key: ${newKey}`);
        
        // Store the conversion data first
        await redisRequest('set', newKey, JSON.stringify(conversion));
        return newKey;
        
    } catch (error) {
        console.error(`❌ Error finding conversion key for ${conversion.email}:`, error);
        return null;
    }
}
