// attribution-debugger.js - Deep diagnostic tool for unattributed conversions
// Path: netlify/functions/attribution-debugger.js

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key, Authorization',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Access-Control-Max-Age': '86400',
  'Content-Type': 'application/json'
};

function createCorsResponse(statusCode, body) {
  return {
    statusCode,
    headers: CORS_HEADERS,
    body: typeof body === 'string' ? body : JSON.stringify(body)
  };
}

exports.handler = async (event, context) => {
  if (event.httpMethod === 'OPTIONS') {
    return createCorsResponse(200, { message: 'CORS preflight successful' });
  }

  // Validate API key
  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  if (apiKey !== process.env.OJOY_API_KEY) {
    return createCorsResponse(401, { error: 'Invalid API key' });
  }

  const redis = (path) => {
    const url = `${process.env.UPSTASH_REDIS_REST_URL}/${path}`;
    return fetch(url, {
      headers: { 'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
    }).then(r => r.json());
  };

  try {
    const { email, timestamp } = JSON.parse(event.body || '{}');
    
    if (!email) {
      return createCorsResponse(400, { 
        error: 'Email required for debugging',
        usage: 'POST with {"email": "customer@example.com", "timestamp": "optional"}'
      });
    }

    console.log(`🔍 ATTRIBUTION DEBUGGER: Analyzing ${email}`);
    
    // Step 1: Find the conversion record
    const conversionAnalysis = await findConversionRecord(redis, email, timestamp);
    
    if (!conversionAnalysis.found) {
      return createCorsResponse(404, {
        success: false,
        message: `No conversion found for ${email}`,
        searched_patterns: conversionAnalysis.search_details
      });
    }

    const conversion = conversionAnalysis.conversion;
    console.log(`✅ Found conversion for ${email}: ${conversion.timestamp}`);

    // Step 2: Extract all attribution signals from conversion
    const attributionSignals = extractAttributionSignals(conversion);
    
    // Step 3: Manual attribution search with detailed logging
    const debugResults = await performDetailedAttributionSearch(redis, attributionSignals, conversion.timestamp);
    
    // Step 4: Analyze why attribution failed
    const failureAnalysis = analyzeAttributionFailure(attributionSignals, debugResults);
    
    // Step 5: Generate recommendations
    const recommendations = generateRecommendations(attributionSignals, debugResults, failureAnalysis);

    return createCorsResponse(200, {
      success: true,
      email: email,
      conversion_analysis: {
        found: true,
        timestamp: conversion.timestamp,
        order_id: conversion.order_id,
        current_attribution: {
          found: conversion.attribution_found || false,
          method: conversion.attribution_method || 'none',
          score: conversion.attribution_score || 0,
          landing_page: conversion.landing_page || null,
          source: conversion.source || null
        }
      },
      attribution_signals: attributionSignals,
      debug_results: debugResults,
      failure_analysis: failureAnalysis,
      recommendations: recommendations,
      diagnostic_summary: {
        total_signals_available: Object.values(attributionSignals).filter(Boolean).length,
        redis_keys_checked: debugResults.total_keys_checked,
        potential_matches_found: debugResults.potential_matches.length,
        time_window_issues: debugResults.time_window_analysis.outside_window_count,
        data_quality_issues: failureAnalysis.data_quality_issues.length
      }
    });

  } catch (error) {
    console.error('❌ Attribution debugger error:', error);
    return createCorsResponse(500, {
      success: false,
      error: 'Attribution debugging failed',
      message: error.message
    });
  }
};

// Find the conversion record in Redis
async function findConversionRecord(redis, email, timestamp) {
  console.log('🔍 Searching for conversion record...');
  
  const searchDetails = {
    patterns_tried: [],
    keys_scanned: 0
  };

  try {
    // Search through conversion keys
    let cursor = '0';
    do {
      const scanResult = await redis(`scan/${cursor}/match/conversions:*/count/1000`);
      if (scanResult.result) {
        cursor = scanResult.result[0];
        const keys = scanResult.result[1] || [];
        searchDetails.keys_scanned += keys.length;
        
        // Check each conversion
        for (const key of keys) {
          try {
            const conversionData = await redis(`get/${key}`);
            if (conversionData.result) {
              const conversion = JSON.parse(decodeURIComponent(conversionData.result));
              
              // Match by email and optionally timestamp
              const emailMatch = conversion.email === email;
              const timestampMatch = !timestamp || conversion.timestamp === timestamp;
              
              if (emailMatch && timestampMatch) {
                conversion._redis_key = key;
                return {
                  found: true,
                  conversion: conversion,
                  search_details: searchDetails
                };
              }
            }
          } catch (parseError) {
            // Skip invalid records
          }
        }
      }
    } while (cursor !== '0' && searchDetails.keys_scanned < 5000);
    
    return {
      found: false,
      search_details: searchDetails
    };
    
  } catch (error) {
    console.error('❌ Conversion search failed:', error);
    return {
      found: false,
      error: error.message,
      search_details: searchDetails
    };
  }
}

// Extract all possible attribution signals from conversion
function extractAttributionSignals(conversion) {
  console.log('🔍 Extracting attribution signals from conversion...');
  
  const signals = {
    // Session tracking
    session_id: conversion.session_id || conversion.SSID || conversion.ssid || null,
    
    // Device fingerprinting
    device_signature: conversion.device_signature || conversion.dsig || null,
    canvas_fingerprint: conversion.canvas_fingerprint || null,
    
    // Screen/Hardware signatures  
    screen_value: conversion.screen_value || conversion.SVV || conversion.SVVV || null,
    gpu_signature: conversion.gpu_signature || conversion.gsig || null,
    
    // IP addresses (multiple possible fields)
    primary_ip: conversion.primary_ip || conversion.PIP || null,
    conversion_ip: conversion.conversion_ip || conversion.CIP || null,
    pageview_ip: conversion.pageview_ip || conversion.IP || null,
    ip_address: conversion.ip_address || null,
    
    // Additional IPs
    unique_ips: conversion.unique_ips || [],
    
    // Timing
    conversion_timestamp: conversion.timestamp,
    
    // Current attribution status
    current_attribution_found: conversion.attribution_found || false,
    current_attribution_method: conversion.attribution_method || null,
    current_landing_page: conversion.landing_page || null
  };
  
  // Consolidate all IP addresses
  const allIPs = [
    signals.primary_ip,
    signals.conversion_ip, 
    signals.pageview_ip,
    signals.ip_address,
    ...(signals.unique_ips || [])
  ].filter(ip => ip && ip !== 'unknown');
  
  signals.all_ips = [...new Set(allIPs)]; // Remove duplicates
  
  console.log('📊 Attribution signals extracted:', {
    session_id: !!signals.session_id,
    device_signature: !!signals.device_signature,
    screen_value: !!signals.screen_value,
    gpu_signature: !!signals.gpu_signature,
    total_ips: signals.all_ips.length,
    ips: signals.all_ips
  });
  
  return signals;
}

// Perform detailed attribution search with comprehensive logging
async function performDetailedAttributionSearch(redis, signals, conversionTimestamp) {
  console.log('🔍 Starting detailed attribution search...');
  
  const results = {
    total_keys_checked: 0,
    searches_performed: [],
    potential_matches: [],
    time_window_analysis: {
      conversion_time: new Date(conversionTimestamp).getTime(),
      window_24h_start: new Date(conversionTimestamp).getTime() - (24 * 60 * 60 * 1000),
      window_7d_start: new Date(conversionTimestamp).getTime() - (7 * 24 * 60 * 60 * 1000),
      pageviews_in_24h: 0,
      pageviews_in_7d: 0,
      outside_window_count: 0
    }
  };
  
  // Search 1: Session ID
  if (signals.session_id) {
    console.log(`🎯 Searching by session ID: ${signals.session_id}`);
    const sessionSearch = await searchBySessionID(redis, signals.session_id);
    results.searches_performed.push({
      method: 'session_id',
      signal: signals.session_id,
      found: sessionSearch.found,
      matches: sessionSearch.matches,
      keys_checked: sessionSearch.keys_checked
    });
    results.total_keys_checked += sessionSearch.keys_checked;
    results.potential_matches.push(...sessionSearch.matches);
  }
  
  // Search 2: Device Signature
  if (signals.device_signature) {
    console.log(`🔐 Searching by device signature: ${signals.device_signature.substring(0, 20)}...`);
    const deviceSearch = await searchByDeviceSignature(redis, signals.device_signature);
    results.searches_performed.push({
      method: 'device_signature',
      signal: signals.device_signature.substring(0, 20) + '...',
      found: deviceSearch.found,
      matches: deviceSearch.matches,
      keys_checked: deviceSearch.keys_checked
    });
    results.total_keys_checked += deviceSearch.keys_checked;
    results.potential_matches.push(...deviceSearch.matches);
  }
  
  // Search 3: All IP Addresses
  for (let i = 0; i < signals.all_ips.length; i++) {
    const ip = signals.all_ips[i];
    console.log(`📍 Searching by IP ${i + 1}/${signals.all_ips.length}: ${ip}`);
    const ipSearch = await searchByIP(redis, ip);
    results.searches_performed.push({
      method: `ip_address_${i + 1}`,
      signal: ip,
      found: ipSearch.found,
      matches: ipSearch.matches,
      keys_checked: ipSearch.keys_checked
    });
    results.total_keys_checked += ipSearch.keys_checked;
    results.potential_matches.push(...ipSearch.matches);
  }
  
  // Search 4: Screen Signature
  if (signals.screen_value) {
    console.log(`📺 Searching by screen signature: ${signals.screen_value}`);
    const screenSearch = await searchByScreenSignature(redis, signals.screen_value);
    results.searches_performed.push({
      method: 'screen_signature',
      signal: signals.screen_value,
      found: screenSearch.found,
      matches: screenSearch.matches,
      keys_checked: screenSearch.keys_checked
    });
    results.total_keys_checked += screenSearch.keys_checked;
    results.potential_matches.push(...screenSearch.matches);
  }
  
  // Search 5: GPU Signature
  if (signals.gpu_signature) {
    console.log(`🎮 Searching by GPU signature: ${signals.gpu_signature}`);
    const gpuSearch = await searchByGPUSignature(redis, signals.gpu_signature);
    results.searches_performed.push({
      method: 'gpu_signature',
      signal: signals.gpu_signature,
      found: gpuSearch.found,
      matches: gpuSearch.matches,
      keys_checked: gpuSearch.keys_checked
    });
    results.total_keys_checked += gpuSearch.keys_checked;
    results.potential_matches.push(...gpuSearch.matches);
  }
  
  // Analyze time windows for all potential matches
  results.potential_matches.forEach(match => {
    const matchTime = new Date(match.timestamp).getTime();
    const hoursDiff = (results.time_window_analysis.conversion_time - matchTime) / (1000 * 60 * 60);
    
    match.time_analysis = {
      hours_before_conversion: Math.round(hoursDiff * 10) / 10,
      within_24h: hoursDiff >= 0 && hoursDiff <= 24,
      within_7d: hoursDiff >= 0 && hoursDiff <= (7 * 24)
    };
    
    if (match.time_analysis.within_24h) {
      results.time_window_analysis.pageviews_in_24h++;
    }
    if (match.time_analysis.within_7d) {
      results.time_window_analysis.pageviews_in_7d++;
    }
    if (!match.time_analysis.within_7d) {
      results.time_window_analysis.outside_window_count++;
    }
  });
  
  console.log(`📊 Search complete: ${results.potential_matches.length} potential matches found`);
  return results;
}

// Individual search functions
async function searchBySessionID(redis, sessionId) {
  try {
    const sessionKey = `attribution_session_${sessionId}`;
    const sessionResult = await redis(`get/${sessionKey}`);
    
    const result = {
      found: false,
      matches: [],
      keys_checked: 1
    };
    
    if (sessionResult.result) {
      const attributionResult = await redis(`get/${sessionResult.result}`);
      result.keys_checked++;
      
      if (attributionResult.result) {
        const pageview = JSON.parse(decodeURIComponent(attributionResult.result));
        result.found = true;
        result.matches.push({
          ...pageview,
          match_method: 'session_id_direct_lookup',
          confidence: 300,
          redis_key: sessionResult.result
        });
      }
    }
    
    return result;
  } catch (error) {
    return { found: false, matches: [], keys_checked: 1, error: error.message };
  }
}

async function searchByDeviceSignature(redis, deviceSig) {
  try {
    const deviceKey = `attribution_fp_${deviceSig.slice(-20)}`;
    const deviceResult = await redis(`get/${deviceKey}`);
    
    const result = {
      found: false,
      matches: [],
      keys_checked: 1
    };
    
    if (deviceResult.result) {
      const attributionResult = await redis(`get/${deviceResult.result}`);
      result.keys_checked++;
      
      if (attributionResult.result) {
        const pageview = JSON.parse(decodeURIComponent(attributionResult.result));
        result.found = true;
        result.matches.push({
          ...pageview,
          match_method: 'device_signature_direct_lookup',
          confidence: 220,
          redis_key: deviceResult.result
        });
      }
    }
    
    return result;
  } catch (error) {
    return { found: false, matches: [], keys_checked: 1, error: error.message };
  }
}

async function searchByIP(redis, ip) {
  try {
    const encodedIP = ip.replace(/:/g, '_');
    const ipKey = `attribution_ip_${encodedIP}`;
    const ipResult = await redis(`get/${ipKey}`);
    
    const result = {
      found: false,
      matches: [],
      keys_checked: 1
    };
    
    if (ipResult.result) {
      const attributionResult = await redis(`get/${ipResult.result}`);
      result.keys_checked++;
      
      if (attributionResult.result) {
        const pageview = JSON.parse(decodeURIComponent(attributionResult.result));
        result.found = true;
        result.matches.push({
          ...pageview,
          match_method: 'ip_address_direct_lookup',
          confidence: 260,
          redis_key: ipResult.result
        });
      }
    }
    
    return result;
  } catch (error) {
    return { found: false, matches: [], keys_checked: 1, error: error.message };
  }
}

async function searchByScreenSignature(redis, screenValue) {
  try {
    const screenKey = `attribution_screen_${screenValue}`;
    const screenResult = await redis(`get/${screenKey}`);
    
    const result = {
      found: false,
      matches: [],
      keys_checked: 1
    };
    
    if (screenResult.result) {
      const attributionResult = await redis(`get/${screenResult.result}`);
      result.keys_checked++;
      
      if (attributionResult.result) {
        const pageview = JSON.parse(decodeURIComponent(attributionResult.result));
        result.found = true;
        result.matches.push({
          ...pageview,
          match_method: 'screen_signature_direct_lookup',
          confidence: 200,
          redis_key: screenResult.result
        });
      }
    }
    
    return result;
  } catch (error) {
    return { found: false, matches: [], keys_checked: 1, error: error.message };
  }
}

async function searchByGPUSignature(redis, gpuSig) {
  try {
    const gpuKey = `attribution_webgl_${gpuSig}`;
    const gpuResult = await redis(`get/${gpuKey}`);
    
    const result = {
      found: false,
      matches: [],
      keys_checked: 1
    };
    
    if (gpuResult.result) {
      const attributionResult = await redis(`get/${gpuResult.result}`);
      result.keys_checked++;
      
      if (attributionResult.result) {
        const pageview = JSON.parse(decodeURIComponent(attributionResult.result));
        result.found = true;
        result.matches.push({
          ...pageview,
          match_method: 'gpu_signature_direct_lookup',
          confidence: 180,
          redis_key: gpuResult.result
        });
      }
    }
    
    return result;
  } catch (error) {
    return { found: false, matches: [], keys_checked: 1, error: error.message };
  }
}

// Analyze why attribution failed
function analyzeAttributionFailure(signals, debugResults) {
  const analysis = {
    primary_failure_reasons: [],
    data_quality_issues: [],
    timing_issues: [],
    potential_solutions: []
  };
  
  // Check for missing attribution signals
  if (!signals.session_id && !signals.device_signature && signals.all_ips.length === 0) {
    analysis.primary_failure_reasons.push('NO_ATTRIBUTION_SIGNALS');
    analysis.data_quality_issues.push('Conversion has no session ID, device signature, or IP addresses');
  }
  
  // Check if signals exist but no matches found
  const signalsWithData = Object.entries(signals).filter(([key, value]) => 
    value && key !== 'conversion_timestamp' && key !== 'all_ips' && key !== 'unique_ips'
  ).length;
  
  if (signalsWithData > 0 && debugResults.potential_matches.length === 0) {
    analysis.primary_failure_reasons.push('SIGNALS_EXIST_BUT_NO_PAGEVIEWS_FOUND');
    analysis.data_quality_issues.push('Attribution signals exist but no matching pageviews found in Redis');
  }
  
  // Check for timing issues
  if (debugResults.potential_matches.length > 0) {
    const matchesInWindow = debugResults.potential_matches.filter(match => 
      match.time_analysis && match.time_analysis.within_24h
    ).length;
    
    if (matchesInWindow === 0) {
      analysis.timing_issues.push('Pageviews found but all outside 24-hour attribution window');
      analysis.primary_failure_reasons.push('TIMING_WINDOW_MISMATCH');
    }
  }
  
  // Check for Redis lookup key issues
  const directLookupsMissing = debugResults.searches_performed.filter(search => 
    search.keys_checked === 1 && !search.found
  ).length;
  
  if (directLookupsMissing > 0) {
    analysis.data_quality_issues.push('Direct Redis lookup keys missing (attribution_session_*, attribution_ip_*, etc.)');
  }
  
  return analysis;
}

// Generate actionable recommendations
function generateRecommendations(signals, debugResults, failureAnalysis) {
  const recommendations = [];
  
  if (failureAnalysis.primary_failure_reasons.includes('NO_ATTRIBUTION_SIGNALS')) {
    recommendations.push({
      priority: 'HIGH',
      category: 'Data Collection',
      issue: 'Missing attribution signals in conversion webhook',
      action: 'Check if session ID, device fingerprint, and IP addresses are being passed from landing page to checkout',
      technical_fix: 'Verify landing page attribution collection script and webhook data extraction'
    });
  }
  
  if (failureAnalysis.primary_failure_reasons.includes('SIGNALS_EXIST_BUT_NO_PAGEVIEWS_FOUND')) {
    recommendations.push({
      priority: 'HIGH',
      category: 'Data Storage',
      issue: 'Attribution signals exist but no pageviews found',
      action: 'Check if pageview data is being stored correctly in Redis',
      technical_fix: 'Verify store-attribution.js function and Redis key creation'
    });
  }
  
  if (failureAnalysis.primary_failure_reasons.includes('TIMING_WINDOW_MISMATCH')) {
    recommendations.push({
      priority: 'MEDIUM',
      category: 'Attribution Logic',
      issue: 'Pageviews exist but outside attribution window',
      action: 'Consider extending attribution window from 24 hours to 7 days',
      technical_fix: 'Update attribution window in customer journey and recovery functions'
    });
  }
  
  if (debugResults.potential_matches.length > 0) {
    const bestMatch = debugResults.potential_matches.reduce((best, current) => 
      (current.confidence || 0) > (best.confidence || 0) ? current : best
    );
    
    recommendations.push({
      priority: 'MEDIUM',
      category: 'Manual Recovery',
      issue: 'Potential attribution match found',
      action: `Consider manual attribution to ${bestMatch.landing_page} via ${bestMatch.match_method}`,
      technical_fix: `Use attribution recovery function with ${bestMatch.match_method} method`
    });
  }
  
  // IP-specific recommendations
  if (signals.all_ips.length > 1) {
    recommendations.push({
      priority: 'LOW',
      category: 'Data Analysis',
      issue: 'Multiple IP addresses detected',
      action: 'Investigate if customer is using VPN or mobile/wifi switching',
      technical_fix: 'Consider geographic correlation for IP-based attribution'
    });
  }
  
  return recommendations;
}
