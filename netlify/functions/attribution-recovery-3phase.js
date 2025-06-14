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
        console.log('🎯 Starting Four-Phase Attribution Recovery');
        
        // Step 1: Fetch analytics data from June 11-14
        const analyticsData = await fetchAnalyticsData();
        
        // Step 2: Find unattributed conversions
        const unattributedConversions = findUnattributedConversions(analyticsData.conversions);
        
        if (unattributedConversions.length === 0) {
            return {
                statusCode: 200,
                headers,
                body: JSON.stringify({
                    success: true,
                    message: 'No unattributed conversions found',
                    results: { total: 0, recovered: 0, phases: {} }
                })
            };
        }
        
        // Step 3: Analyze unattributed conversions for IPv6 matches
        const recoveryResults = await analyzeUnattributedConversions(unattributedConversions, analyticsData.page_views);
        
        // Step 4: Update Redis with recovered attributions
        if (recoveryResults.matches.length > 0) {
            console.log(`📝 Updating ${recoveryResults.matches.length} recovered attributions in Redis...`);
            await updateRecoveredAttributions(recoveryResults.matches);
        }
        
        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({
                success: true,
                results: recoveryResults,
                message: `Recovery complete: ${recoveryResults.recovered}/${recoveryResults.total} conversions recovered`
            })
        };

    } catch (error) {
        console.error('❌ Recovery error:', error);
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({
                error: 'Recovery failed',
                details: error.message
            })
        };
    }
};

// Step 1: Fetch analytics data from June 11-14
async function fetchAnalyticsData() {
    console.log('📊 Fetching analytics data for June 11-14...');
    
    const startDate = '2025-06-11';
    const endDate = '2025-06-14';
    
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

// Step 2: Find unattributed conversions
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

// Step 3: Analyze unattributed conversions with FOUR phases
async function analyzeUnattributedConversions(unattributedConversions, pageviews) {
    console.log('🔬 Analyzing unattributed conversions for IPv6 pageview matches...');
    
    const results = {
        total: unattributedConversions.length,
        recovered: 0,
        matches: [],
        phases: {
            'Phase 1': { attempts: 0, matches: 0 },
            'Phase 2': { attempts: 0, matches: 0 },
            'Phase 3': { attempts: 0, matches: 0 },
            'Phase 4': { attempts: 0, matches: 0 }
        }
    };
    
    for (let i = 0; i < unattributedConversions.length; i++) {
        const conversion = unattributedConversions[i];
        console.log(`🔍 ANALYZING CONVERSION ${i + 1}/${unattributedConversions.length}: ${conversion.email}`);
        console.log(`   📍 Conversion IP: ${conversion.ip_address}`);
        console.log(`   ⏰ Conversion Time: ${conversion.timestamp}`);
        
        // Try FOUR phases in sequence (including post-conversion search)
        const phases = [
            { name: 'Phase 1', start: 0, end: 15, confidence: 'HIGH', direction: 'before' },
            { name: 'Phase 2', start: 15, end: 45, confidence: 'MEDIUM', direction: 'before' },
            { name: 'Phase 3', start: 45, end: 120, confidence: 'EXTENDED', direction: 'before' },
            { name: 'Phase 4', start: 120, end: 1440, confidence: 'POST_CONVERSION', direction: 'after' }
        ];
        
        let matched = false;
        
        for (const phase of phases) {
            if (matched) break;
            
            results.phases[phase.name].attempts++;
            
            if (phase.direction === 'after') {
                console.log(`   🕐 ${phase.name}: Searching ${phase.start}-${phase.end} minutes AFTER conversion (2-24 hours post-conversion)`);
            } else {
                console.log(`   🕐 ${phase.name}: Searching ${phase.start}-${phase.end} minutes BEFORE conversion`);
            }
            
            // Find IPv6 pageviews in window (now with direction support)
            const candidatePageviews = findIPv6PageviewsInWindow(conversion, pageviews, phase.start, phase.end, phase.direction);
            
            if (candidatePageviews.length === 0) {
                console.log(`   ❌ No IPv6 pageviews found in ${phase.name} window`);
                continue;
            }
            
            console.log(`   📱 Found ${candidatePageviews.length} IPv6 pageviews in ${phase.name} window`);
            
            // Get geographic data for conversion IP
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
    
    console.log(`🏁 Recovery complete: ${results.recovered}/${results.total} conversions recovered`);
    return results;
}

// Find IPv6 pageviews within time window (enhanced with direction support for Phase 4)
function findIPv6PageviewsInWindow(conversion, pageviews, startMinutes, endMinutes, direction = 'before') {
    const conversionTime = new Date(conversion.timestamp);
    let windowStart, windowEnd;
    
    if (direction === 'after') {
        // Search AFTER conversion (Phase 4: 2-24 hours post-conversion)
        windowStart = new Date(conversionTime.getTime() + startMinutes * 60 * 1000);
        windowEnd = new Date(conversionTime.getTime() + endMinutes * 60 * 1000);
        
        console.log(`   🕐 Search window (AFTER conversion): ${windowStart.toISOString()} to ${windowEnd.toISOString()}`);
        
        const ipv6Pageviews = pageviews.filter(pv => {
            const pvTime = new Date(pv.timestamp);
            return pvTime >= windowStart && 
                   pvTime <= windowEnd && 
                   pv.ip_address && pv.ip_address.includes(':'); // IPv6 addresses contain colons
        });
        
        console.log(`   📊 Found ${ipv6Pageviews.length} IPv6 pageviews AFTER conversion (out of ${pageviews.length} total pageviews)`);
        return ipv6Pageviews;
        
    } else {
        // Search BEFORE conversion (Phases 1-3: original logic)
        windowStart = new Date(conversionTime.getTime() - endMinutes * 60 * 1000);
        windowEnd = new Date(conversionTime.getTime() - startMinutes * 60 * 1000);
        
        console.log(`   🕐 Search window (BEFORE conversion): ${windowStart.toISOString()} to ${windowEnd.toISOString()}`);
        
        const ipv6Pageviews = pageviews.filter(pv => {
            const pvTime = new Date(pv.timestamp);
            return pvTime >= windowStart && 
                   pvTime <= conversionTime && 
                   pv.ip_address && pv.ip_address.includes(':'); // IPv6 addresses contain colons
        });
        
        console.log(`   📊 Found ${ipv6Pageviews.length} IPv6 pageviews BEFORE conversion (out of ${pageviews.length} total pageviews)`);
        return ipv6Pageviews;
    }
}

// Get location/ISP data for an IP using IPInfo.io
async function getIPLocationData(ip) {
    const token = process.env.IPINFO_TOKEN || 'dd31c7ae01d4e4';
    const url = `https://ipinfo.io/${ip}?token=${token}`;
    
    try {
        const response = await fetch(url, {
            method: 'GET',
            headers: { 'Accept': 'application/json' }
        });
        
        if (response.ok) {
            const data = await response.json();
            
            return {
                ip: data.ip,
                city: data.city || 'Unknown',
                region: data.region || 'Unknown',
                country: data.country || 'Unknown',
                isp: extractBestISP(data),
                timezone: data.timezone || 'Unknown',
                location: data.loc || '0,0'
            };
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

// Extract best ISP info
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

// Check IPv6 candidates against conversion for geographic matches
async function checkIPv6Candidates(conversion, candidatePageviews, conversionGeoData) {
    for (let i = 0; i < candidatePageviews.length; i++) {
        const pageview = candidatePageviews[i];
        const timeDiff = Math.abs(new Date(conversion.timestamp) - new Date(pageview.timestamp)) / 1000 / 60;
        
        console.log(`   🌈 IPv6 Candidate ${i + 1}: ${pageview.ip_address}`);
        console.log(`      ⏰ Pageview Time: ${pageview.timestamp} (${timeDiff.toFixed(1)} min difference)`);
        console.log(`      📄 Landing Page: ${pageview.landing_page || pageview.url || 'Unknown'}`);
        
        // Get geographic data for IPv6 pageview
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

// Compare geographic data between conversion and pageview
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

// Compare ISP names
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

// Helper function to make Redis HTTP requests
async function redisRequest(command, ...args) {
    const url = process.env.UPSTASH_REDIS_REST_URL;
    const token = process.env.UPSTASH_REDIS_REST_TOKEN;
    
    if (!url || !token) {
        throw new Error('Missing Redis configuration');
    }
    
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

// Update recovered attributions in Redis
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
                    recovery_method: 'four_phase_geographic',
                    recovery_phase: match.phase,
                    recovery_confidence: match.confidence,
                    recovery_score: match.match.score,
                    recovery_timestamp: new Date().toISOString(),
                    recovery_ipv6_match: pageview.ip_address
                };
                
                // Save back to Redis
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

// Find the Redis key for a specific conversion
async function findConversionKey(conversion) {
    try {
        // Try different possible key patterns
        const patterns = [
            `conversion_${conversion.email}_*`,
            `conv_${conversion.email}_*`,
            `track_${conversion.email}_*`,
            `*${conversion.email}*`,
            `conversion_*${conversion.timestamp}*`,
            `*conversion*`
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
