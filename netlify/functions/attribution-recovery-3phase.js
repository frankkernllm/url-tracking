// netlify/functions/attribution-recovery-3phase.js
const redis = require('@upstash/redis');

const redisClient = redis.Redis.fromEnv();

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

    // API Key validation
    const apiKey = event.headers['x-api-key'];
    if (apiKey !== 'ojoy_track_2025_secure_key_v1') {
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

async function getUnattributedConversions() {
    // Get today and yesterday's analytics data
    const today = new Date();
    const yesterday = new Date(today.getTime() - 24 * 60 * 60 * 1000);
    
    const conversionKeys = await redisClient.keys('conversion_*');
    const unattributed = [];
    
    for (const key of conversionKeys) {
        const conversion = await redisClient.get(key);
        if (conversion && (!conversion.attribution_found || conversion.attribution_found === false)) {
            unattributed.push({
                ...conversion,
                key: key
            });
        }
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

    for (const conversion of conversions) {
        console.log(`🔍 Processing: ${conversion.email} (${conversion.ip_address})`);
        
        let matched = false;
        
        // Try each phase in sequence
        for (const phase of phases) {
            if (matched) break;
            
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
            }
        }
        
        if (!matched) {
            console.log(`   ❌ No matches found in any phase`);
        }
    }
    
    return results;
}

async function searchPhase(conversion, startMinutes, endMinutes) {
    const conversionTime = new Date(conversion.timestamp);
    const windowStart = new Date(conversionTime.getTime() - endMinutes * 60 * 1000);
    const windowEnd = new Date(conversionTime.getTime() - startMinutes * 60 * 1000);
    
    // Get pageviews in time window
    const pageviewKeys = await redisClient.keys('attribution_*');
    const candidates = [];
    
    for (const key of pageviewKeys) {
        if (key.includes('_ip_') || key.includes('_session_')) continue; // Skip lookup keys
        
        const pageview = await redisClient.get(key);
        if (!pageview) continue;
        
        const pageviewTime = new Date(pageview.timestamp);
        
        // Check if in time window and IPv6
        if (pageviewTime >= windowStart && 
            pageviewTime <= windowEnd && 
            pageview.ip_address && 
            pageview.ip_address.includes(':')) {
            
            // Get geographic data for both IPs
            const conversionGeo = await getIPLocation(conversion.ip_address);
            const pageviewGeo = await getIPLocation(pageview.ip_address);
            
            const score = calculateGeoScore(conversionGeo, pageviewGeo);
            
            if (score >= 3) {
                candidates.push({
                    pageview: pageview,
                    score: score,
                    timeDiff: Math.abs(conversionTime - pageviewTime) / 60000,
                    conversionGeo: conversionGeo,
                    pageviewGeo: pageviewGeo
                });
            }
        }
    }
    
    if (candidates.length === 0) return null;
    
    // Return best match (highest score, then closest time)
    return candidates.sort((a, b) => {
        if (b.score !== a.score) return b.score - a.score;
        return a.timeDiff - b.timeDiff;
    })[0];
}

async function getIPLocation(ip) {
    try {
        const response = await fetch(`https://ipinfo.io/${ip}?token=dd31c7ae01d4e4`);
        const data = await response.json();
        
        return {
            city: data.city || 'Unknown',
            region: data.region || 'Unknown',
            country: data.country || 'Unknown',
            isp: extractBestISP(data),
            timezone: data.timezone || 'Unknown'
        };
    } catch (error) {
        console.log(`⚠️ IP lookup failed for ${ip}: ${error.message}`);
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
        
        await redisClient.set(conversion.key, updatedConversion);
        
        console.log(`✅ Updated ${conversion.email}: ${pageview.landing_page} (${match.phase})`);
    }
}
