exports.handler = async (event, context) => {
    // SPECIAL BATCH: June 12-18 Attribution Recovery - BYPASS PROCESSED CHECK
    // This version specifically targets older unattributed conversions from June 12-18
    // and bypasses the "processed" check to reprocess all unattributed conversions
    
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
        console.log('🎯 Starting JUNE 12-18 Batch Attribution Recovery (SPECIAL RUN)');
        console.log('⚠️  BYPASSING processed check - will reprocess ALL unattributed conversions');
        
        // SPECIAL CONFIGURATION for June 12-18 batch
        const BATCH_SIZE = 13; // Process all 13 at once
        
        // Step 1: Fetch analytics data from June 12-18 (FIXED DATE RANGE)
        const analyticsData = await fetchAnalyticsDataJune1218();
        
        // Step 2: Find unattributed conversions (unchanged)
        const allUnattributedConversions = findUnattributedConversions(analyticsData.conversions);
        
        if (allUnattributedConversions.length === 0) {
            return {
                statusCode: 200,
                headers,
                body: JSON.stringify({
                    success: true,
                    message: 'No unattributed conversions found in June 12-18 period',
                    results: { total: 0, recovered: 0, phases: {} },
                    batch_info: { processed: 0, remaining: 0, batch_size: BATCH_SIZE }
                })
            };
        }
        
        // Step 2.5: BYPASS PROCESSED CHECK - Process ALL unattributed conversions
        console.log('🔄 BYPASSING processed check - reprocessing ALL unattributed conversions from June 12-18');
        const unprocessedConversions = allUnattributedConversions; // Process all, ignore processed status
        
        // Clear any existing processed markers for these conversions
        await clearProcessedMarkers(unprocessedConversions);
        
        // Step 2.6: Take batch from unprocessed conversions
        const conversionsToProcess = unprocessedConversions.slice(0, BATCH_SIZE);
        const remainingAfterBatch = unprocessedConversions.length - conversionsToProcess.length;
        
        console.log(`📦 JUNE 12-18 BATCH PROCESSING: Processing ${conversionsToProcess.length} conversions (${remainingAfterBatch} remaining)`);
        console.log(`📊 Total Status: ${allUnattributedConversions.length} total unattributed from June 12-18 period`);
        
        // Log the conversions we're about to process
        console.log('📋 Conversions to process:');
        conversionsToProcess.forEach((conv, index) => {
            console.log(`   ${index + 1}. ${conv.email} | ${conv.ip_address} | ${conv.timestamp}`);
        });
        
        // Step 3: Analyze conversions in this batch (OPTIMIZED with caching)
        const recoveryResults = await analyzeUnattributedConversions(conversionsToProcess, analyticsData.page_views);
        
        // Step 4: Update Redis with recovered attributions (unchanged)
        if (recoveryResults.matches.length > 0) {
            console.log(`📝 Updating ${recoveryResults.matches.length} recovered attributions in Redis...`);
            try {
                await updateRecoveredAttributions(recoveryResults.matches);
            } catch (redisError) {
                console.error('❌ Redis update failed but recovery succeeded:', redisError);
            }
        }
        
        // Step 5: Mark processed conversions to prevent re-processing (re-enable for future)
        await markConversionsAsProcessed(conversionsToProcess);
        
        // Step 6: Return batch processing status
        const batchComplete = remainingAfterBatch === 0;
        const statusMessage = batchComplete ? 
            `June 12-18 batch processing COMPLETE: ${recoveryResults.recovered}/${recoveryResults.total} conversions recovered` :
            `June 12-18 batch ${conversionsToProcess.length}/${unprocessedConversions.length} complete: ${recoveryResults.recovered}/${recoveryResults.total} recovered. ${remainingAfterBatch} conversions remaining - run again to continue.`;
        
        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({
                success: true,
                results: recoveryResults,
                message: statusMessage,
                date_range: 'June 12-18, 2025',
                processed_check_bypassed: true,
                batch_info: {
                    processed_this_batch: conversionsToProcess.length,
                    recovered_this_batch: recoveryResults.recovered,
                    remaining_conversions: remainingAfterBatch,
                    batch_size: BATCH_SIZE,
                    status: batchComplete ? 'COMPLETE' : 'CONTINUE',
                    next_action: batchComplete ? 'June 12-18 recovery complete!' : 'Run the function again to process remaining conversions'
                }
            })
        };

    } catch (error) {
        console.error('❌ June 12-18 batch recovery error:', error);
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({
                error: 'June 12-18 batch recovery failed',
                details: error.message
            })
        };
    }
};

// NEW: Clear processed markers for conversions we want to reprocess
async function clearProcessedMarkers(conversions) {
    console.log('🧹 Clearing processed markers for June 12-18 conversions...');
    
    for (const conversion of conversions) {
        try {
            const processedKey = `processed_conversion:${conversion.email}:${conversion.timestamp}`;
            await redisRequest('del', processedKey);
            console.log(`🗑️  Cleared processed marker: ${conversion.email}`);
        } catch (error) {
            console.log(`⚠️ Could not clear processed marker for ${conversion.email}: ${error.message}`);
            // Continue anyway - we're bypassing the check
        }
    }
}

// NEW: Get cached geographic data from attribution records (major optimization)
async function getCachedGeoData(ip) {
    try {
        // Check if we already have geo data for this IP from pageview attribution
        const encodedIP = ip.replace(/:/g, '_');
        const ipKey = `attribution_ip_${encodedIP}`;
        const attrKeyResult = await redisRequest('get', ipKey);
        
        if (attrKeyResult) {
            // Get the main attribution record
            const attrResult = await redisRequest('get', attrKeyResult);
            if (attrResult) {
                const attrData = JSON.parse(attrResult);
                if (attrData.geographic_data) {
                    console.log(`   💾 Using cached geo data for ${ip}: ${attrData.geographic_data.city}, ${attrData.geographic_data.region} (${attrData.geographic_data.isp})`);
                    return attrData.geographic_data;
                }
            }
        }
        
        // Also try to find geo data in any recent attribution record with this IP
        const geoKeys = await redisRequest('keys', 'attribution_geo_*');
        if (geoKeys && geoKeys.length > 0) {
            for (const geoKey of geoKeys.slice(-20)) { // Check last 20 geo keys
                try {
                    const mainKeyResult = await redisRequest('get', geoKey);
                    if (mainKeyResult) {
                        const mainKey = mainKeyResult;
                        const attrResult = await redisRequest('get', mainKey);
                        if (attrResult) {
                            const attrData = JSON.parse(attrResult);
                            if (attrData.ip_address === ip && attrData.geographic_data) {
                                console.log(`   💾 Found cached geo data via geo key for ${ip}`);
                                return attrData.geographic_data;
                            }
                        }
                    }
                } catch (geoError) {
                    continue; // Skip invalid geo keys
                }
            }
        }
        
        return null;
    } catch (error) {
        console.log(`   ⚠️ Cached geo lookup failed for ${ip}: ${error.message}`);
        return null;
    }
}

// NEW: Mark conversions as processed to prevent re-processing
async function markConversionsAsProcessed(conversions) {
    console.log(`📝 Marking ${conversions.length} conversions as processed...`);
    
    for (const conversion of conversions) {
        try {
            const processedKey = `processed_conversion:${conversion.email}:${conversion.timestamp}`;
            const processedData = {
                email: conversion.email,
                timestamp: conversion.timestamp,
                processed_at: new Date().toISOString(),
                batch_id: Date.now(),
                special_run: 'june_12_18_recovery'
            };
            
            // Set with 7-day expiration to prevent Redis bloat
            await redisRequest('setex', processedKey, 604800, JSON.stringify(processedData)); // 7 days = 604800 seconds
            console.log(`✅ Re-marked as processed: ${conversion.email}`);
        } catch (error) {
            console.log(`⚠️ Could not mark ${conversion.email} as processed: ${error.message}`);
        }
    }
}

// MODIFIED: Fetch analytics data for June 12-18 specifically
async function fetchAnalyticsDataJune1218() {
    console.log('📊 Fetching analytics data for June 12-18, 2025...');
    
    // FIXED DATE RANGE for June 12-18 batch
    const startDate = '2025-06-12';
    const endDate = '2025-06-18';
    
    console.log(`📅 Date range: ${startDate} to ${endDate} (June 12-18 special batch)`);
    
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

// Step 3: Analyze unattributed conversions with OPTIMIZED phases
async function analyzeUnattributedConversions(unattributedConversions, pageviews) {
    console.log('🔬 Analyzing June 12-18 unattributed conversions with OPTIMIZED geographic correlation...');
    
    const results = {
        total: unattributedConversions.length,
        recovered: 0,
        matches: [],
        phases: {
            'Phase 1': { attempts: 0, matches: 0 },
            'Phase 2': { attempts: 0, matches: 0 },
            'Phase 3': { attempts: 0, matches: 0 }
        }
    };
    
    for (let i = 0; i < unattributedConversions.length; i++) {
        const conversion = unattributedConversions[i];
        console.log(`🔍 ANALYZING CONVERSION ${i + 1}/${unattributedConversions.length}: ${conversion.email}`);
        console.log(`   📍 Conversion IP: ${conversion.ip_address}`);
        console.log(`   ⏰ Conversion Time: ${conversion.timestamp}`);
        
        // OPTIMIZED phases - reduced from 4 to 3 phases since track.js now catches more in real-time
        const phases = [
            { name: 'Phase 1', start: 0, end: 30, confidence: 'HIGH' },        // 0-30 min
            { name: 'Phase 2', start: 30, end: 120, confidence: 'MEDIUM' },    // 30min-2h
            { name: 'Phase 3', start: 120, end: 240, confidence: 'EXTENDED' }  // 2-4 hours
        ];
        
        let matched = false;
        
        for (const phase of phases) {
            if (matched) break;
            
            results.phases[phase.name].attempts++;
            
            console.log(`   🕐 ${phase.name}: Searching ${phase.start}-${phase.end} minute window`);
            
            // Find IPv6 pageviews in window
            const candidatePageviews = findIPv6PageviewsInWindow(conversion, pageviews, phase.start, phase.end);
            
            if (candidatePageviews.length === 0) {
                console.log(`   ❌ No IPv6 pageviews found in ${phase.name} window`);
                continue;
            }
            
            console.log(`   📱 Found ${candidatePageviews.length} IPv6 pageviews in ${phase.name} window`);
            
            // Get geographic data for conversion IP (OPTIMIZED - check cache first)
            const conversionGeoData = await getIPLocationData(conversion.ip_address);
            console.log(`   📍 Conversion Location: ${conversionGeoData.city}, ${conversionGeoData.region}, ${conversionGeoData.country} (${conversionGeoData.isp})`);
            
            // Check each IPv6 pageview for geographic match
            const match = await checkIPv6Candidates(conversion, candidatePageviews, conversionGeoData);
            
            if (match) {
                console.log(`   ✅ ${phase.name} MATCH FOUND!`);
                
                results.matches.push({
                    conversion: conversion,
                    match: match,
                    phase: phase.name,
                    confidence: phase.confidence
                });
                
                results.phases[phase.name].matches++;
                results.recovered++;
                matched = true;
            }
        }
        
        if (!matched) {
            console.log('   ❌ No matches found in any phase');
        }
    }
    
    console.log(`🏁 June 12-18 Recovery complete: ${results.recovered}/${results.total} conversions recovered`);
    return results;
}

// Find IPv6 pageviews within time window - OPTIMIZED with timestamp sorting
function findIPv6PageviewsInWindow(conversion, pageviews, startMinutes, endMinutes) {
    const conversionTime = new Date(conversion.timestamp);
    const windowStart = new Date(conversionTime.getTime() - endMinutes * 60 * 1000);
    const windowEnd = new Date(conversionTime.getTime() - startMinutes * 60 * 1000);
    
    console.log(`   🕐 Search window: ${windowStart.toISOString()} to ${windowEnd.toISOString()}`);
    
    const ipv6Pageviews = pageviews.filter(pv => {
        const pvTime = new Date(pv.timestamp);
        return pvTime >= windowStart && 
               pvTime <= conversionTime && 
               pv.ip_address && pv.ip_address.includes(':'); // IPv6 addresses contain colons
    });
    
    // SORT BY TIMESTAMP - NEWEST FIRST (most recent pageviews checked first)
    ipv6Pageviews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    
    console.log(`   📊 Found ${ipv6Pageviews.length} IPv6 pageviews in time window out of ${pageviews.length} total pageviews`);
    console.log(`   🕐 Sorted by timestamp (newest first) - will check most recent matches first`);
    
    return ipv6Pageviews;
}

// OPTIMIZED: Get location/ISP data with caching support
async function getIPLocationData(ip) {
    // Check cache first (major optimization)
    const cached = await getCachedGeoData(ip);
    if (cached) return cached;
    
    // Only make API call if not cached
    const token = process.env.IPINFO_TOKEN || 'dd31c7ae01d4e4';
    const url = `https://ipinfo.io/${ip}?token=${token}`;
    
    try {
        const response = await fetch(url, {
            method: 'GET',
            headers: { 'Accept': 'application/json' }
        });
        
        if (response.ok) {
            const data = await response.json();
            
            const result = {
                ip: data.ip,
                city: data.city || 'Unknown',
                region: data.region || 'Unknown',
                country: data.country || 'Unknown',
                isp: extractBestISP(data),
                timezone: data.timezone || 'Unknown',
                location: data.loc || '0,0'
            };
            
            console.log(`   🌍 Fresh geo lookup for ${ip}: ${result.city}, ${result.region} (${result.isp})`);
            return result;
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

// Extract best ISP info (UNCHANGED - identical to original)
function extractBestISP(data) {
    if (data.company && data.company.name) {
        return data.company.name;
    } else if (data.asn && data.asn.name) {
        return data.asn.name;
    } else if (data.org) {
        return data.org;
    } else if (data.carrier && data.carrier.name) {
        return data.carrier.name;
    }
    return 'Unknown';
}

// Check IPv6 candidates against conversion for geographic matches (OPTIMIZED with caching)
async function checkIPv6Candidates(conversion, candidatePageviews, conversionGeoData) {
    for (let i = 0; i < candidatePageviews.length; i++) {
        const pageview = candidatePageviews[i];
        const timeDiff = Math.abs(new Date(conversion.timestamp) - new Date(pageview.timestamp)) / 1000 / 60;
        
        console.log(`   🌈 IPv6 Candidate ${i + 1}: ${pageview.ip_address}`);
        console.log(`      ⏰ Pageview Time: ${pageview.timestamp} (${timeDiff.toFixed(1)} min before)`);
        console.log(`      📄 Landing Page: ${pageview.landing_page || pageview.url || 'Unknown'}`);
        
        // OPTIMIZED: Get geographic data for IPv6 pageview (check cache first)
        const pageviewGeoData = await getIPLocationData(pageview.ip_address);
        console.log(`      📍 IPv6 Location: ${pageviewGeoData.city}, ${pageviewGeoData.region}, ${pageviewGeoData.country} (${pageviewGeoData.isp})`);
        
        // Compare geographic data
        const match = compareGeographicData(conversionGeoData, pageviewGeoData);
        
        if (match.isMatch) {
            console.log(`      ✅ GEOGRAPHIC MATCH FOUND! (${match.confidence})`);
            console.log(`         🎯 City: ${match.cityMatch ? '✓' : '✗'} | Region: ${match.regionMatch ? '✓' : '✗'} | Country: ${match.countryMatch ? '✓' : '✗'} | ISP: ${match.ispMatch ? '✓' : '✗'}`);
            
            return {
                pageview: pageview,
                score: match.score,
                timeDiff: timeDiff,
                confidence: match.confidence,
                conversionGeo: conversionGeoData,
                pageviewGeo: pageviewGeoData
            };
        } else {
            console.log(`      ❌ No geographic match (${match.confidence})`);
        }
    }
    
    return null;
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

// Helper function to make Redis HTTP requests (ENHANCED - added SETEX support for expiration)
async function redisRequest(command, ...args) {
    const url = process.env.UPSTASH_REDIS_REST_URL;
    const token = process.env.UPSTASH_REDIS_REST_TOKEN;
    
    if (!url || !token) {
        throw new Error('Missing Redis configuration');
    }
    
    // For complex commands like SET with JSON, use POST with body
    if ((command.toLowerCase() === 'set' || command.toLowerCase() === 'setex') && args.length >= 2) {
        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify([command, ...args])
        });
        
        if (!response.ok) {
            throw new Error(`Redis request failed: ${response.status} ${response.statusText}`);
        }
        
        const data = await response.json();
        return data.result;
    } 
    // For simple commands like GET, KEYS, DEL, use URL path
    else {
        const response = await fetch(`${url}/${command}/${args.join('/')}`, {
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json'
            }
        });
        
        if (!response.ok) {
            throw new Error(`Redis request failed: ${response.status} ${response.statusText}`);
        }
        
        const data = await response.json();
        return data.result;
    }
}

// Update recovered attributions in Redis (UNCHANGED - identical to original)
async function updateRecoveredAttributions(matches) {
    console.log(`📝 Updating ${matches.length} recovered attributions in Redis...`);
    
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
                    recovery_method: 'june_12_18_special',
                    recovery_phase: match.phase,
                    recovery_confidence: match.confidence,
                    recovery_score: match.match.score,
                    recovery_timestamp: new Date().toISOString(),
                    recovery_ipv6_match: pageview.ip_address
                };
                
                // Save back to Redis (using fixed POST method)
                await redisRequest('set', conversionKey, JSON.stringify(updatedConversion));
                
                console.log(`✅ Updated ${conversion.email}: ${pageview.landing_page} (${match.phase})`);
            } else {
                console.log(`⚠️ Could not find Redis key for ${conversion.email}`);
            }
            
        } catch (error) {
            console.log(`❌ Failed to update ${conversion.email}: ${error.message}`);
        }
    }
    
    console.log(`📝 Redis update complete for ${matches.length} attributions`);
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
