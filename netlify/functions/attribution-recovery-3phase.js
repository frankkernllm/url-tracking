// netlify/functions/attribution-recovery-3phase.js
// Complete three-phase attribution recovery system with environment variable API key

exports.handler = async (event, context) => {
    // CORS headers
    const headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Content-Type': 'application/json'
    };

    if (event.httpMethod === 'OPTIONS') {
        return { statusCode: 200, headers, body: '' };
    }

    // API Key validation using environment variable
    const apiKey = event.headers['x-api-key'];
    const expectedApiKey = process.env.OJOY_API_KEY;
    if (apiKey !== expectedApiKey) {
        return {
            statusCode: 401,
            headers,
            body: JSON.stringify({ error: 'Invalid API key' })
        };
    }

    try {
        console.log('🎯 Starting Three-Phase Attribution Recovery');
        
        // Step 1: Get unattributed conversions
        const unattributedConversions = await getUnattributedConversions();
        
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

        console.log(`📊 Found ${unattributedConversions.length} unattributed conversions`);

        // Step 2: Run three-phase recovery
        const recoveryResults = await runThreePhaseRecovery(unattributedConversions);

        // Step 3: Update Redis with recovered attributions
        await updateRecoveredAttributions(recoveryResults.matches);

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

async function getUnattributedConversions() {
    console.log('🔍 Searching for unattributed conversions in Redis...');
    
    // Get all conversion keys
    let conversionKeys;
    try {
        conversionKeys = await redisRequest('keys', 'conversion_*');
    } catch (error) {
        console.error('❌ Failed to get conversion keys from Redis:', error);
        return [];
    }
    
    const unattributed = [];
    
    if (!conversionKeys || conversionKeys.length === 0) {
        console.log('⚠️ No conversion keys found in Redis');
        return [];
    }
    
    console.log(`📊 Found ${conversionKeys.length} total conversion keys in Redis`);
    
    // Check each conversion for attribution status
    for (let i = 0; i < conversionKeys.length; i++) {
        const key = conversionKeys[i];
        
        try {
            const conversionData = await redisRequest('get', key);
            if (!conversionData) {
                console.log(`⚠️ No data for conversion key: ${key}`);
                continue;
            }
            
            let conversion;
            try {
                conversion = typeof conversionData === 'string' ? JSON.parse(conversionData) : conversionData;
            } catch (parseError) {
                console.log(`❌ Failed to parse conversion data for key ${key}:`, parseError.message);
                continue;
            }
            
            // Check if conversion lacks attribution
            const hasAttribution = conversion.attribution_found === true;
            const hasLandingPage = conversion.landing_page && conversion.landing_page !== '';
            const hasSource = conversion.source && conversion.source !== '' && conversion.source !== 'direct';
            
            console.log(`🔍 Conversion ${i + 1}/${conversionKeys.length}: ${conversion.email || 'Unknown'} | Attribution: ${hasAttribution} | Landing Page: ${hasLandingPage} | Source: ${conversion.source || 'None'}`);
            
            if (!hasAttribution || (!hasLandingPage && !hasSource)) {
                unattributed.push({
                    ...conversion,
                    key: key
                });
                console.log(`   ➕ Added to unattributed list (reason: ${!hasAttribution ? 'no attribution flag' : 'missing landing page/source'})`);
            } else {
                console.log(`   ✅ Has attribution - skipping`);
            }
            
        } catch (error) {
            console.error(`❌ Error processing conversion key ${key}:`, error.message);
            // Continue processing other conversions
        }
    }
    
    console.log(`🎯 Final result: ${unattributed.length} unattributed conversions found out of ${conversionKeys.length} total`);
    
    // Log details of unattributed conversions
    if (unattributed.length > 0) {
        console.log('📋 Unattributed conversions details:');
        unattributed.forEach((conv, index) => {
            console.log(`   ${index + 1}. ${conv.email} | ${conv.ip_address} | ${conv.timestamp} | Source: ${conv.source || 'None'}`);
        });
    }
    
    return unattributed;
}

async function runThreePhaseRecovery(conversions) {
    const phases = [
        { name: 'Phase 1', start: 0, end: 15, confidence: 'HIGH' },
        { name: 'Phase 2', start: 15, end: 45, confidence: 'MEDIUM' },
        { name: 'Phase 3', start: 45, end: 120, confidence: 'EXTENDED' }
    ];
    
    const results = {
        total: conversions.length,
        recovered: 0,
        matches: [],
        phases: {
            'Phase 1': { attempts: 0, matches: 0 },
            'Phase 2': { attempts: 0, matches: 0 },
            'Phase 3': { attempts: 0, matches: 0 }
        }
    };

    console.log(`🔄 Starting recovery for ${conversions.length} unattributed conversions`);

    // Process each conversion with individual error handling
    for (let i = 0; i < conversions.length; i++) {
        const conversion = conversions[i];
        
        try {
            console.log(`🔍 Processing conversion ${i + 1}/${conversions.length}: ${conversion.email} (${conversion.ip_address})`);
            
            let matched = false;
            
            // Try each phase in sequence
            for (const phase of phases) {
                if (matched) break;
                
                try {
                    results.phases[phase.name].attempts++;
                    console.log(`   ${phase.name}: Searching ${phase.start}-${phase.end} minutes...`);
                    
                    const match = await searchPhase(conversion, phase.start, phase.end);
                    
                    if (match && match.score >= 3) {
                        console.log(`   ✅ ${phase.name} match found! Score: ${match.score}`);
                        
                        results.matches.push({
                            conversion: conversion,
                            match: match,
                            phase: phase.name,
                            confidence: phase.confidence
                        });
                        
                        results.phases[phase.name].matches++;
                        results.recovered++;
                        matched = true;
                    } else {
                        console.log(`   ⚪ ${phase.name}: No match found (score: ${match?.score || 'N/A'})`);
                    }
                } catch (phaseError) {
                    console.error(`   ❌ Error in ${phase.name} for ${conversion.email}:`, phaseError.message);
                    // Continue to next phase despite error
                }
            }
            
            if (!matched) {
                console.log(`   ❌ No matches found in any phase for ${conversion.email}`);
            }
            
        } catch (conversionError) {
            console.error(`❌ Error processing conversion ${conversion.email}:`, conversionError.message);
            // Continue to next conversion despite error
        }
        
        // Add a small delay between conversions to avoid overwhelming APIs
        if (i < conversions.length - 1) {
            await new Promise(resolve => setTimeout(resolve, 100));
        }
    }
    
    console.log(`🏁 Recovery complete: ${results.recovered}/${results.total} conversions recovered`);
    return results;
}

async function searchPhase(conversion, startMinutes, endMinutes) {
    try {
        const conversionTime = new Date(conversion.timestamp);
        const windowStart = new Date(conversionTime.getTime() - endMinutes * 60 * 1000);
        const windowEnd = new Date(conversionTime.getTime() - startMinutes * 60 * 1000);
        
        console.log(`   🔍 Search window: ${windowStart.toISOString()} to ${windowEnd.toISOString()}`);
        
        // Get all attribution keys (pageviews)
        let attributionKeys;
        try {
            attributionKeys = await redisRequest('keys', 'attribution_*');
        } catch (redisError) {
            console.log(`   ⚠️ Redis keys request failed: ${redisError.message}`);
            return null;
        }
        
        const candidates = [];
        
        if (!attributionKeys || attributionKeys.length === 0) {
            console.log('   ⚠️ No attribution keys found in Redis');
            return null;
        }
        
        console.log(`   📊 Found ${attributionKeys.length} total attribution keys to check`);
        
        let processedKeys = 0;
        let validPageviews = 0;
        let ipv6Pageviews = 0;
        let timeWindowMatches = 0;
        
        for (const key of attributionKeys) {
            // Skip lookup keys - we want the main pageview records
            if (key.includes('_ip_') || key.includes('_session_') || key.includes('_geo_') || key.includes('_fp_')) {
                continue;
            }
            
            try {
                const pageviewData = await redisRequest('get', key);
                if (!pageviewData) continue;
                
                let pageview;
                try {
                    pageview = typeof pageviewData === 'string' ? JSON.parse(pageviewData) : pageviewData;
                } catch (parseError) {
                    console.log(`   ⚠️ Failed to parse pageview data for key ${key}`);
                    continue;
                }
                
                processedKeys++;
                
                if (!pageview.timestamp || !pageview.ip_address) {
                    continue;
                }
                
                validPageviews++;
                
                // Check if this pageview is in our time window
                const pageviewTime = new Date(pageview.timestamp);
                if (pageviewTime < windowStart || pageviewTime > windowEnd) {
                    continue;
                }
                
                timeWindowMatches++;
                
                // Check if this is an IPv6 pageview (contains colons)
                if (!pageview.ip_address.includes(':')) {
                    continue;
                }
                
                ipv6Pageviews++;
                console.log(`   📱 Found IPv6 pageview ${ipv6Pageviews}: ${pageview.ip_address} at ${pageview.timestamp}`);
                
                // Get geographic data for both IPs
                const conversionGeo = await getIPLocation(conversion.ip_address);
                const pageviewGeo = await getIPLocation(pageview.ip_address);
                
                const score = calculateGeoScore(conversionGeo, pageviewGeo);
                
                if (score >= 3) {
                    const timeDiff = Math.abs(conversionTime - pageviewTime) / 60000; // minutes
                    
                    candidates.push({
                        pageview: pageview,
                        score: score,
                        timeDiff: timeDiff,
                        conversionGeo: conversionGeo,
                        pageviewGeo: pageviewGeo
                    });
                    
                    console.log(`   ✅ Candidate found! Score: ${score}, Time diff: ${timeDiff.toFixed(1)} min`);
                }
                
            } catch (error) {
                console.log(`   ⚠️ Error processing key ${key}:`, error.message);
                // Continue processing other keys
            }
        }
        
        console.log(`   📊 Search summary:`);
        console.log(`      - Total keys checked: ${attributionKeys.length}`);
        console.log(`      - Valid pageviews: ${validPageviews}`);
        console.log(`      - Time window matches: ${timeWindowMatches}`);
        console.log(`      - IPv6 pageviews: ${ipv6Pageviews}`);
        console.log(`      - Final candidates: ${candidates.length}`);
        
        if (candidates.length === 0) return null;
        
        // Return best match (highest score, then closest time)
        const bestMatch = candidates.sort((a, b) => {
            if (b.score !== a.score) return b.score - a.score;
            return a.timeDiff - b.timeDiff;
        })[0];
        
        console.log(`   🎯 Best match: Score ${bestMatch.score}, ${bestMatch.timeDiff.toFixed(1)} min gap`);
        return bestMatch;
        
    } catch (error) {
        console.error(`   ❌ SearchPhase error: ${error.message}`);
        return null;
    }
}

async function getIPLocation(ip) {
    const token = process.env.IPINFO_TOKEN || 'dd31c7ae01d4e4';
    
    try {
        const response = await fetch(`https://ipinfo.io/${ip}?token=${token}`, {
            method: 'GET',
            headers: { 'Accept': 'application/json' }
        });
        
        if (response.ok) {
            const data = await response.json();
            
            return {
                city: data.city || 'Unknown',
                region: data.region || 'Unknown',
                country: data.country || 'Unknown',
                isp: extractBestISP(data),
                timezone: data.timezone || 'Unknown'
            };
        } else {
            throw new Error(`HTTP ${response.status}`);
        }
    } catch (error) {
        console.log(`   ⚠️ IP lookup failed for ${ip}: ${error.message}`);
        return {
            city: 'LOOKUP_FAILED',
            region: 'LOOKUP_FAILED',
            country: 'LOOKUP_FAILED',
            isp: 'LOOKUP_FAILED'
        };
    }
}

function extractBestISP(data) {
    if (data.company?.name) return data.company.name;
    if (data.asn?.name) return data.asn.name;
    if (data.org) return data.org;
    if (data.carrier?.name) return data.carrier.name;
    return 'Unknown';
}

function calculateGeoScore(conversionGeo, pageviewGeo) {
    let score = 0;
    
    if (conversionGeo.city === pageviewGeo.city) score += 3;
    if (conversionGeo.region === pageviewGeo.region) score += 2;
    if (conversionGeo.country === pageviewGeo.country) score += 1;
    if (compareISPs(conversionGeo.isp, pageviewGeo.isp)) score += 2;
    
    console.log(`   📊 Geographic score: ${score} (City: ${conversionGeo.city === pageviewGeo.city ? '✓' : '✗'}, Region: ${conversionGeo.region === pageviewGeo.region ? '✓' : '✗'}, Country: ${conversionGeo.country === pageviewGeo.country ? '✓' : '✗'}, ISP: ${compareISPs(conversionGeo.isp, pageviewGeo.isp) ? '✓' : '✗'})`);
    
    return score;
}

function compareISPs(isp1, isp2) {
    if (!isp1 || !isp2 || isp1 === 'Unknown' || isp2 === 'Unknown') return false;
    
    const normalize = str => str.toLowerCase().replace(/[^a-z0-9]/g, '');
    const norm1 = normalize(isp1);
    const norm2 = normalize(isp2);
    
    // Exact match
    if (norm1 === norm2) return true;
    
    // Contains match
    if (norm1.includes(norm2) || norm2.includes(norm1)) return true;
    
    // ASN matching
    const asn1 = isp1.match(/AS(\d+)/);
    const asn2 = isp2.match(/AS(\d+)/);
    if (asn1 && asn2 && asn1[1] === asn2[1]) return true;
    
    return false;
}

async function updateRecoveredAttributions(matches) {
    console.log(`📝 Updating ${matches.length} recovered attributions in Redis`);
    
    for (const match of matches) {
        const conversion = match.conversion;
        const pageview = match.match.pageview;
        
        try {
            // Update conversion record with attribution
            const updatedConversion = {
                ...conversion,
                attribution_found: true,
                landing_page: pageview.landing_page || pageview.url,
                source: pageview.source || 'recovered',
                recovery_method: 'three_phase_geographic',
                recovery_phase: match.phase,
                recovery_confidence: match.confidence,
                recovery_score: match.match.score,
                recovery_timestamp: new Date().toISOString()
            };
            
            // Update the conversion in Redis
            await redisRequest('set', conversion.key, JSON.stringify(updatedConversion));
            
            console.log(`✅ Updated ${conversion.email}: ${pageview.landing_page} (${match.phase})`);
        } catch (error) {
            console.log(`❌ Failed to update ${conversion.email}: ${error.message}`);
        }
    }
}
