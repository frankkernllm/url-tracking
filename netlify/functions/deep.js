exports.handler = async (event, context) => {
    // ATTRIBUTION DEEP DIVE SYSTEM: 24-Hour Single Conversion Processing
    // Processes ONE unattributed conversion per run with 24-hour window
    
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
        console.log('🔍 Starting ATTRIBUTION DEEP DIVE SYSTEM - 24-Hour Single Conversion');
        console.log('⚡ Will process ONE unattributed conversion with maximum time window');
        
        // Step 1: Fetch analytics data from past 7 days
        const analyticsData = await fetchAnalyticsDataPast7Days();
        
        // Step 2: Find all conversions and filter for unattributed only
        const allConversions = getAllConversions(analyticsData.conversions);
        const unattributedConversions = getUnattributedConversions(allConversions);
        
        // Step 3: Filter out conversions already processed with 24-hour window
        const alreadydeep = await alreadydeep(unattributedConversions);
        
        if (alreadydeep.length === 0) {
            if (unattributedConversions.length > 0) {
                return {
                    statusCode: 200,
                    headers,
                    body: JSON.stringify({
                        success: true,
                        message: `🎯 All ${unattributedConversions.length} unattributed conversions have been processed with 24-hour window. No more to process.`,
                        results: { 
                            total_conversions: allConversions.length,
                            unattributed: unattributedConversions.length,
                            unprocessed_24h: 0,
                            processed_this_run: 0,
                            status: 'ALL_24H_PROCESSED'
                        }
                    })
                };
            } else {
                return {
                    statusCode: 200,
                    headers,
                    body: JSON.stringify({
                        success: true,
                        message: '🎉 All conversions have attribution! No unattributed conversions found.',
                        results: { 
                            total_conversions: allConversions.length,
                            unattributed: 0,
                            unprocessed_24h: 0,
                            processed_this_run: 0,
                            status: 'ALL_ATTRIBUTED'
                        }
                    })
                };
            }
        }
        
        console.log(`📋 Found ${unattributedConversions.length} unattributed conversions`);
        console.log(`🎯 Found ${alreadydeep.length} not yet processed with 24-hour window`);
        console.log(`🔍 Processing the first unprocessed conversion...`);
        
        // Step 4: Process the FIRST unprocessed conversion
        const conversionToProcess = alreadydeep[0];
        
        console.log(`\n🔬 DEEP DIVE ANALYSIS: [PRIVACY PROTECTED]`);
        console.log(`   📍 IP: ${conversionToProcess.ip_address}`);
        console.log(`   ⏰ Time: ${conversionToProcess.timestamp}`);
        console.log(`   📊 Current: NO ATTRIBUTION`);
        console.log(`   🔍 Using 24-hour window for maximum coverage`);
        
        // Step 5: Analyze this conversion with 24-hour window
        const improvementResults = await analyzeConversionForAttribution24Hour(conversionToProcess, analyticsData.page_views);
        
        // Step 6: Update Redis if needed
        let updateResult = null;
        if (improvementResults.shouldUpdate) {
            console.log(`📝 Updating attribution for conversion...`);
            try {
                updateResult = await updateConversionAttribution(conversionToProcess, improvementResults);
            } catch (redisError) {
                console.error('❌ Redis update failed:', redisError);
            }
        }
        
        // Step 7: Mark as processed with alreadydeep flag
        await markConversionAsAlreadyDeep(conversionToProcess);
        
        // Step 8: Generate response
        const remainingUnattributed = unattributedConversions.length - 1;
        const remainingUnprocessed24H = alreadydeep.length - 1;
        const wasSuccessful = improvementResults.matchFound;
        
        let summaryMessage;
        if (wasSuccessful) {
            summaryMessage = `✅ Found attribution for conversion! ${remainingUnprocessed24H} unattributed conversions remaining to process with 24-hour window.`;
        } else {
            summaryMessage = `❌ No attribution found even with 24-hour window. ${remainingUnprocessed24H} unattributed conversions remaining to process.`;
        }
        
        console.log(`\n🏁 DEEP DIVE COMPLETE:`);
        console.log(`   📧 Processed: [PRIVACY PROTECTED]`);
        console.log(`   ✅ Success: ${wasSuccessful ? 'YES' : 'NO'}`);
        console.log(`   🔄 Remaining unprocessed (24h): ${remainingUnprocessed24H}`);
        
        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({
                success: true,
                processed_conversion: {
                    email: '[PRIVACY_PROTECTED]', // Don't expose email in response
                    timestamp: conversionToProcess.timestamp,
                    match_found: wasSuccessful,
                    improvement_type: improvementResults.improvementType,
                    new_attribution: improvementResults.newAttribution,
                    confidence: improvementResults.match?.confidence || null,
                    time_difference_minutes: improvementResults.match?.timeDiff || null,
                    update_result: updateResult
                },
                message: summaryMessage,
                progress: {
                    total_conversions: allConversions.length,
                    unattributed_total: unattributedConversions.length,
                    unattributed_remaining_24h: remainingUnprocessed24H,
                    processed_this_run: 1,
                    status: remainingUnprocessed24H > 0 ? 'MORE_TO_PROCESS' : 'ALL_24H_PROCESSED',
                    next_action: remainingUnprocessed24H > 0 ? 'Press button again to process next conversion' : 'All unattributed conversions have been processed with 24-hour window'
                },
                analysis: improvementResults.analysis,
                processing_method: '24hour_deep_dive'
            })
        };

    } catch (error) {
        console.error('❌ Attribution deep dive system error:', error);
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({
                error: 'Attribution deep dive system failed',
                details: error.message
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

// Fetch analytics data for past 7 days (dynamic date range)
async function fetchAnalyticsDataPast7Days() {
    console.log('📊 Fetching analytics data for past 7 days...');
    
    // Calculate dynamic date range - using 7 days to stay within available data
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(endDate.getDate() - 7);
    
    // Ensure we don't go before June 11, 2025 (when data starts)
    const earliestDate = new Date('2025-06-11');
    if (startDate < earliestDate) {
        startDate.setTime(earliestDate.getTime());
    }
    
    const startDateStr = startDate.toISOString().split('T')[0];
    const endDateStr = endDate.toISOString().split('T')[0];
    
    console.log(`📅 Date range: ${startDateStr} to ${endDateStr}`);
    
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

// Get ALL conversions
function getAllConversions(conversions) {
    if (!conversions || conversions.length === 0) {
        console.log('❌ No conversions found in analytics data');
        return [];
    }
    
    // Sort by timestamp DESCENDING (newest first)
    const sortedConversions = conversions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    console.log(`📊 Found ${sortedConversions.length} total conversions`);
    
    return sortedConversions;
}

// Get ONLY unattributed conversions (no landing_page or empty landing_page)
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

// Filter out conversions that have already been processed with 24-hour window
async function alreadydeep(unattributedConversions) {
    const unprocessedConversions = [];
    let alreadyProcessed24HCount = 0;
    
    for (const conversion of unattributedConversions) {
        const keyAlreadyDeep = `alreadydeep:${conversion.email}:${conversion.timestamp}`;
        
        try {
            const processedData = await redisRequest('get', keyAlreadyDeep);
            
            if (processedData) {
                alreadyProcessed24HCount++;
                console.log(`   ⏭️ Skipping [PRIVACY PROTECTED] - already processed with 24-hour window`);
            } else {
                unprocessedConversions.push(conversion);
            }
        } catch (error) {
            // If we can't check status, assume unprocessed
            unprocessedConversions.push(conversion);
        }
    }
    
    console.log(`📊 Filtered out ${alreadyProcessed24HCount} already processed with 24-hour window`);
    return unprocessedConversions;
}

// Analyze single conversion for attribution improvement with 24-hour window
async function analyzeConversionForAttribution24Hour(conversion, pageviews) {
    console.log('   🔬 Finding best temporal match with 24-hour window...');
    
    // Reset cache statistics
    cacheStats = { hits: 0, misses: 0, errors: 0 };
    
    const results = {
        conversionEmail: conversion.email,
        originalAttribution: null,
        matchFound: false,
        newAttribution: null,
        shouldUpdate: false,
        improvementType: 'NO_CHANGE',
        match: null,
        analysis: {
            pageviews_in_window: 0,
            cache_performance: {},
            window_size: '24 hours'
        }
    };
    
    // Find all pageviews in 24-hour window before conversion
    const candidatePageviews = findPageviewsIn24HourWindow(conversion, pageviews);
    
    if (candidatePageviews.length === 0) {
        console.log(`   ❌ No pageviews found in 24-hour window before conversion`);
        results.analysis.pageviews_in_window = 0;
        return results;
    }
    
    console.log(`   📱 Found ${candidatePageviews.length} pageviews in 24-hour window`);
    results.analysis.pageviews_in_window = candidatePageviews.length;
    
    // Get conversion geographic data
    const conversionGeoData = await getIPLocationData(conversion.ip_address);
    console.log(`   📍 Location: ${conversionGeoData.city}, ${conversionGeoData.region} (${conversionGeoData.isp})`);
    
    // Find the best temporal match
    const bestMatch = await findBestTemporalMatch(conversion, candidatePageviews, conversionGeoData);
    
    if (bestMatch) {
        console.log(`   ✅ MATCH FOUND! (${bestMatch.confidence})`);
        
        results.matchFound = true;
        results.newAttribution = bestMatch.pageview.landing_page || bestMatch.pageview.url;
        results.match = bestMatch;
        results.shouldUpdate = true;
        results.improvementType = 'NEW_ATTRIBUTION_24H';
        
        console.log(`   🎯 Update: YES (NEW_ATTRIBUTION_24H)`);
        
    } else {
        console.log('   ❌ No geographic matches found even with 24-hour window');
        results.improvementType = 'NO_MATCH_FOUND_24H';
    }
    
    // Cache performance stats
    const totalLookups = cacheStats.hits + cacheStats.misses + cacheStats.errors;
    const hitRate = totalLookups > 0 ? ((cacheStats.hits / totalLookups) * 100).toFixed(1) : 0;
    
    results.analysis.cache_performance = {
        cache_hits: cacheStats.hits,
        cache_misses: cacheStats.misses,
        cache_hit_rate_percent: hitRate
    };
    
    return results;
}

// Find pageviews in 24-hour window before conversion
function findPageviewsIn24HourWindow(conversion, pageviews) {
    const conversionTime = new Date(conversion.timestamp);
    const windowStart = new Date(conversionTime.getTime() - 24 * 60 * 60 * 1000); // 24 hours before
    
    const candidatePageviews = pageviews.filter(pv => {
        const pvTime = new Date(pv.timestamp);
        return pvTime >= windowStart && 
               pvTime <= conversionTime && 
               pv.ip_address; // Must have IP address
    });
    
    // Sort by timestamp DESCENDING (newest first = closest to conversion)
    candidatePageviews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    
    return candidatePageviews;
}

// Find best temporal match (closest to conversion time)
async function findBestTemporalMatch(conversion, candidatePageviews, conversionGeoData) {
    for (let i = 0; i < candidatePageviews.length; i++) {
        const pageview = candidatePageviews[i];
        const timeDiff = Math.abs(new Date(conversion.timestamp) - new Date(pageview.timestamp)) / 1000 / 60;
        const ipType = pageview.ip_address.includes(':') ? 'IPv6' : 'IPv4';
        
        // Get pageview geographic data
        const pageviewGeoData = await getIPLocationData(pageview.ip_address);
        
        // Compare geographic data
        const geoMatch = compareGeographicData(conversionGeoData, pageviewGeoData);
        
        if (geoMatch.isMatch) {
            console.log(`   🏆 TEMPORAL MATCH: ${timeDiff.toFixed(1)}min before (${geoMatch.confidence})`);
            
            return {
                pageview: pageview,
                score: geoMatch.score,
                timeDiff: timeDiff,
                confidence: geoMatch.confidence,
                conversionGeo: conversionGeoData,
                pageviewGeo: pageviewGeoData,
                ipType: ipType,
                candidateNumber: i + 1
            };
        }
        
        // Progress logging for large datasets
        if ((i + 1) % 50 === 0) {
            console.log(`   📊 Checked ${i + 1}/${candidatePageviews.length} pageviews`);
        }
    }
    
    return null;
}

// Mark conversion as alreadydeep
async function markConversionAsAlreadyDeep(conversion) {
    try {
        const keyAlreadyDeep = `alreadydeep:${conversion.email}:${conversion.timestamp}`;
        const processedData = {
            email: conversion.email,
            timestamp: conversion.timestamp,
            processed_at: new Date().toISOString(),
            system: 'attribution_24hour_deep_dive',
            window_size: '24 hours'
        };
        
        // Set with 30-day expiration
        await redisRequest('setex', keyAlreadyDeep, 2592000, JSON.stringify(processedData)); // 30 days
        console.log(`   ✅ Marked conversion as alreadydeep`);
    } catch (error) {
        console.log(`   ⚠️ Could not mark conversion as alreadydeep: ${error.message}`);
    }
}

// Update conversion attribution in Redis
async function updateConversionAttribution(conversion, improvementResults) {
    try {
        // Find the conversion record in Redis
        const conversionKey = await findConversionKey(conversion);
        
        if (conversionKey) {
            // Get existing data
            const existingData = await redisRequest('get', conversionKey);
            let conversionData = typeof existingData === 'string' ? JSON.parse(existingData) : existingData;
            
            // Update with improved attribution
            const updatedConversion = {
                ...conversionData,
                attribution_found: true,
                landing_page: improvementResults.newAttribution,
                source: improvementResults.match.pageview.source || '24h_improved',
                utm_campaign: improvementResults.match.pageview.utm_campaign || conversionData.utm_campaign,
                utm_medium: improvementResults.match.pageview.utm_medium || conversionData.utm_medium,
                referrer_url: improvementResults.match.pageview.referrer_url || conversionData.referrer_url,
                // Attribution improvement metadata
                attribution_improvement: {
                    method: '24hour_deep_dive',
                    improvement_type: improvementResults.improvementType,
                    confidence: improvementResults.match.confidence,
                    score: improvementResults.match.score,
                    time_difference_minutes: improvementResults.match.timeDiff,
                    improved_at: new Date().toISOString(),
                    pageview_ip: improvementResults.match.pageview.ip_address,
                    pageview_timestamp: improvementResults.match.pageview.timestamp,
                    window_size: '24_hours'
                },
                // Store previous attribution (should be null for unattributed)
                previous_attribution: conversionData.landing_page || null,
                attributed_pageview_timestamp: improvementResults.match.pageview.timestamp
            };
            
            // Save back to Redis
            await redisRequest('set', conversionKey, JSON.stringify(updatedConversion));
            
            return {
                success: true,
                updated_attribution: improvementResults.newAttribution,
                previous_attribution: conversionData.landing_page || null,
                improvement_type: improvementResults.improvementType
            };
            
        } else {
            return { success: false, error: 'Conversion not found in Redis' };
        }
        
    } catch (error) {
        return { success: false, error: error.message };
    }
}

// Get cached geographic data
async function getCachedGeoData(ip) {
    try {
        const cacheKey = `geo_cache:${ip.replace(/:/g, '_')}`;
        const cachedResult = await redisRequest('get', cacheKey);
        
        if (cachedResult === null || cachedResult === undefined) {
            cacheStats.misses++;
            return null;
        }
        
        if (cachedResult) {
            try {
                const cachedData = JSON.parse(decodeURIComponent(cachedResult));
                cacheStats.hits++;
                return cachedData;
            } catch (parseError) {
                cacheStats.errors++;
                return null;
            }
        }
        
        cacheStats.misses++;
        return null;
        
    } catch (error) {
        cacheStats.errors++;
        return null;
    }
}

// Get location/ISP data using cache
async function getIPLocationData(ip) {
    // Check cache first
    const cached = await getCachedGeoData(ip);
    if (cached) return cached;
    
    // Make fresh API call
    const token = process.env.IPINFO_TOKEN || 'dd31c7ae01d4e4';
    const url = `https://ipinfo.io/${ip}?token=${token}`;
    
    try {
        const response = await fetch(url, {
            method: 'GET',
            headers: { 'Accept': 'application/json' },
            signal: AbortSignal.timeout(2000)
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
            
            // Cache the result
            try {
                const cacheKey = `geo_cache:${ip.replace(/:/g, '_')}`;
                const encodedData = encodeURIComponent(JSON.stringify(geoData));
                await redisRequest('setex', cacheKey, 86400, encodedData);
            } catch (cacheError) {
                // Ignore cache errors
            }
            
            return geoData;
        } else {
            throw new Error(`HTTP ${response.status}`);
        }
    } catch (error) {
        return {
            ip: ip,
            city: 'LOOKUP_FAILED',
            region: 'LOOKUP_FAILED',
            country: 'LOOKUP_FAILED',
            isp: 'LOOKUP_FAILED'
        };
    }
}

// Extract best ISP info
function extractBestISP(data) {
    if (data.company?.name) return data.company.name;
    if (data.asn?.name) return data.asn.name;
    if (data.org) return data.org;
    if (data.carrier?.name) return data.carrier.name;
    return 'Unknown';
}

// Compare geographic data - STRICTER CRITERIA
function compareGeographicData(conversionGeo, pageviewGeo) {
    if (conversionGeo.city === 'LOOKUP_FAILED' || pageviewGeo.city === 'LOOKUP_FAILED') {
        return { isMatch: false, confidence: 'LOOKUP_FAILED', score: 0 };
    }

    const cityMatch = conversionGeo.city === pageviewGeo.city;
    const regionMatch = conversionGeo.region === pageviewGeo.region;
    const countryMatch = conversionGeo.country === pageviewGeo.country;
    const ispMatch = compareISPs(conversionGeo.isp, pageviewGeo.isp);

    let score = 0;
    if (cityMatch) score += 3;
    if (regionMatch) score += 2;
    if (countryMatch) score += 1;
    if (ispMatch) score += 2;

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
        isMatch = false; // ← STRICTER: Reject city-only matches
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

// Compare ISP names
function compareISPs(isp1, isp2) {
    if (!isp1 || !isp2 || isp1 === 'Unknown' || isp2 === 'Unknown') return false;
    
    const normalize = (str) => str.toLowerCase().replace(/[^a-z0-9]/g, '');
    const norm1 = normalize(isp1);
    const norm2 = normalize(isp2);
    
    if (norm1 === norm2) return true;
    if (norm1.includes(norm2) || norm2.includes(norm1)) return true;
    
    const asn1 = isp1.match(/AS(\d+)/);
    const asn2 = isp2.match(/AS(\d+)/);
    if (asn1 && asn2 && asn1[1] === asn2[1]) return true;
    
    return false;
}

// Redis request helper
async function redisRequest(command, ...args) {
    const url = process.env.UPSTASH_REDIS_REST_URL;
    const token = process.env.UPSTASH_REDIS_REST_TOKEN;
    
    if (!url || !token) {
        throw new Error('Missing Redis configuration');
    }
    
    let response;
    
    try {
        if ((command.toLowerCase() === 'set' || command.toLowerCase() === 'setex') && args.length >= 2) {
            response = await fetch(url, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify([command, ...args])
            });
        } else {
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
            if (response.status === 404) {
                return null;
            }
            throw new Error(`Redis request failed: ${response.status} ${response.statusText}`);
        }
        
        const data = await response.json();
        return data.result;
        
    } catch (error) {
        throw error;
    }
}

// Find the Redis key for a specific conversion
async function findConversionKey(conversion) {
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
            try {
                const keys = await redisRequest('keys', pattern);
                if (keys && keys.length > 0) {
                    for (const key of keys) {
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
                continue;
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
