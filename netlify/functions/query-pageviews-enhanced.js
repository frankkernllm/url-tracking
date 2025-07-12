// query-pageviews-enhanced.js - Enhanced pageview search with IP index fallback
// Fixed version with proper exports and IP index fallback

const headers = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Content-Type': 'application/json'
};

exports.handler = async (event, context) => {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers };
  }

  // Validate API key
  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  if (apiKey !== process.env.OJOY_API_KEY) {
    return {
      statusCode: 401,
      headers,
      body: JSON.stringify({ error: 'Invalid API key' })
    };
  }

  // Redis helper function
  const redis = (path) => {
    const url = `${process.env.UPSTASH_REDIS_REST_URL}/${path}`;
    return fetch(url, {
      headers: { 'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
    }).then(r => r.json());
  };

  try {
    const startTime = Date.now();
    const {
      conversion_timestamp,
      ips_to_check = [],
      session_id,
      device_signature,
      screen_value,
      gpu_signature,
      window_hours = 24
    } = JSON.parse(event.body || '{}');

    console.log('🔍 ENHANCED QUERY with IP Index Fallback: Multi-signal attribution search');
    console.log(`   🕐 Window: ${window_hours}h before ${conversion_timestamp}`);
    console.log(`   📍 IPs: ${ips_to_check.length}`);
    console.log(`   🔐 Session: ${!!session_id}`);
    console.log(`   📱 Device sig: ${!!device_signature}`);

    const conversionTime = new Date(conversion_timestamp).getTime();
    const windowStart = conversionTime - (window_hours * 60 * 60 * 1000);
    
    let allMatches = [];
    const queryMethods = [];

    // PRIORITY 1: Session ID Match (Highest confidence - 300 points)
    if (session_id) {
      console.log(`🎯 Searching by session ID: ${session_id}`);
      const sessionMatches = await searchBySessionId(redis, session_id, windowStart, conversionTime);
      if (sessionMatches.length > 0) {
        sessionMatches.forEach(match => {
          match.attribution_method = 'session_id_match';
          match.confidence = 300;
        });
        allMatches = allMatches.concat(sessionMatches);
        queryMethods.push('session_id_lookup');
        console.log(`✅ Session ID: ${sessionMatches.length} matches found`);
      }
    }

    // PRIORITY 2: Device Signature Match (High confidence - 260 points)
    if (device_signature && allMatches.length === 0) {
      console.log(`🔐 Searching by device signature: ${device_signature}`);
      const deviceMatches = await searchByDeviceSignature(redis, device_signature, windowStart, conversionTime);
      if (deviceMatches.length > 0) {
        deviceMatches.forEach(match => {
          match.attribution_method = 'device_signature_match';
          match.confidence = 260;
        });
        allMatches = allMatches.concat(deviceMatches);
        queryMethods.push('device_signature_lookup');
        console.log(`✅ Device signature: ${deviceMatches.length} matches found`);
      }
    }

    // PRIORITY 3: IP Address Matches (Medium-high confidence - 280-240 points)
    if (ips_to_check.length > 0 && allMatches.length === 0) {
      console.log(`📍 Searching by IP addresses: ${ips_to_check.join(', ')}`);
      
      for (let i = 0; i < ips_to_check.length; i++) {
        const ip = ips_to_check[i];
        if (!ip || ip === 'unknown') continue;
        
        const ipMatches = await searchByIpAddress(redis, ip, windowStart, conversionTime);
        if (ipMatches.length > 0) {
          const confidence = i === 0 ? 280 : i === 1 ? 260 : 240;
          const ipType = i === 0 ? 'primary_ip' : i === 1 ? 'conversion_ip' : 'fallback_ip';
          
          ipMatches.forEach(match => {
            match.attribution_method = `${ipType}_match`;
            match.confidence = confidence;
          });
          
          allMatches = allMatches.concat(ipMatches);
          queryMethods.push('ip_lookup');
          console.log(`✅ IP ${ip}: ${ipMatches.length} matches found`);
          break;
        }
      }
    }

    // 🚀 NEW: FALLBACK TO IP INDEXES FOR MULTI-SIGNAL ATTRIBUTION
    if (allMatches.length === 0) {
      console.log('🔄 FALLBACK: Searching IP indexes for multi-signal attribution...');
      
      const indexMatches = await searchIPIndexesForMultiSignal(redis, {
        ips_to_check,
        session_id,
        device_signature,
        screen_value,
        gpu_signature
      }, windowStart, conversionTime);
      
      if (indexMatches.length > 0) {
        allMatches = allMatches.concat(indexMatches);
        queryMethods.push('ip_index_multi_signal_fallback');
        console.log(`✅ IP Index Fallback: ${indexMatches.length} multi-signal matches found`);
      }
    }

    // Remove duplicates and sort by confidence
    const uniqueMatches = removeDuplicateMatches(allMatches);
    uniqueMatches.sort((a, b) => (b.confidence || 0) - (a.confidence || 0));

    const processingTime = Date.now() - startTime;
    console.log(`🏁 Enhanced query complete: ${uniqueMatches.length} unique matches in ${processingTime}ms`);

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        matches_found: uniqueMatches,
        processing_time_ms: processingTime,
        query_methods_used: queryMethods,
        search_signals: {
          session_id: !!session_id,
          device_signature: !!device_signature,
          ip_addresses: ips_to_check.length,
          screen_value: !!screen_value,
          gpu_signature: !!gpu_signature
        },
        window_info: {
          conversion_timestamp,
          window_hours,
          window_start: new Date(windowStart).toISOString()
        }
      })
    };

  } catch (error) {
    console.error('❌ Enhanced query error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: error.message })
    };
  }
};

// Search by session ID (try lookup keys first)
async function searchBySessionId(redis, sessionId, windowStart, conversionTime) {
  try {
    const sessionKey = `attribution_session_${sessionId}`;
    const sessionResult = await redis(`get/${sessionKey}`);
    
    if (sessionResult.result) {
      const attributionResult = await redis(`get/${sessionResult.result}`);
      if (attributionResult.result) {
        const pageview = JSON.parse(decodeURIComponent(attributionResult.result));
        const pageviewTime = new Date(pageview.timestamp).getTime();
        
        if (pageviewTime >= windowStart && pageviewTime <= conversionTime) {
          return [pageview];
        }
      }
    }
    
    return [];
  } catch (error) {
    console.warn('⚠️ Session ID search failed:', error);
    return [];
  }
}

// Search by device signature (try lookup keys first)
async function searchByDeviceSignature(redis, deviceSig, windowStart, conversionTime) {
  try {
    const deviceKey = `attribution_fp_${deviceSig}`;
    const deviceResult = await redis(`get/${deviceKey}`);
    
    if (deviceResult.result) {
      const attributionResult = await redis(`get/${deviceResult.result}`);
      if (attributionResult.result) {
        const pageview = JSON.parse(decodeURIComponent(attributionResult.result));
        const pageviewTime = new Date(pageview.timestamp).getTime();
        
        if (pageviewTime >= windowStart && pageviewTime <= conversionTime) {
          return [pageview];
        }
      }
    }
    
    return [];
  } catch (error) {
    console.warn('⚠️ Device signature search failed:', error);
    return [];
  }
}

// Search by IP address (try lookup keys first)
async function searchByIpAddress(redis, ip, windowStart, conversionTime) {
  try {
    const encodedIp = ip.replace(/:/g, '_');
    const ipKey = `attribution_ip_${encodedIp}`;
    const ipResult = await redis(`get/${ipKey}`);
    
    if (ipResult.result) {
      const attributionKeys = Array.isArray(ipResult.result) ? ipResult.result : [ipResult.result];
      const matches = [];
      
      for (const key of attributionKeys) {
        try {
          const attributionResult = await redis(`get/${key}`);
          if (attributionResult.result) {
            const pageview = JSON.parse(decodeURIComponent(attributionResult.result));
            const pageviewTime = new Date(pageview.timestamp).getTime();
            
            if (pageviewTime >= windowStart && pageviewTime <= conversionTime) {
              matches.push(pageview);
            }
          }
        } catch (e) {
          console.warn(`⚠️ Failed to parse pageview for key ${key}`);
        }
      }
      
      return matches;
    }
    
    return [];
  } catch (error) {
    console.warn('⚠️ IP address search failed:', error);
    return [];
  }
}

// 🚀 NEW: Search IP indexes for multi-signal attribution
async function searchIPIndexesForMultiSignal(redis, queryParams, windowStart, conversionTime) {
  const { ips_to_check, session_id, device_signature, screen_value, gpu_signature } = queryParams;
  const matches = [];
  
  console.log('🔍 Searching IP indexes for multi-signal attribution...');
  
  // If no IPs provided, can't search IP indexes
  if (!ips_to_check || ips_to_check.length === 0) {
    console.log('⚠️ No IPs provided for IP index search');
    return matches;
  }
  
  for (const ip of ips_to_check) {
    if (!ip || ip === 'unknown') continue;
    
    const encodedIP = ip.replace(/:/g, '_');
    const ipIndexKey = `pageview_index_ip:${encodedIP}`;
    
    try {
      console.log(`🔍 Checking IP index: ${ipIndexKey}`);
      const indexData = await redis(`get/${ipIndexKey}`);
      
      if (indexData?.result) {
        const parsed = JSON.parse(decodeURIComponent(indexData.result));
        console.log(`📊 Found IP index with ${parsed.pageview_count || 0} pageviews`);
        
        // Search through pageviews in this IP index for multi-signal matches
        for (const pageview of parsed.pageviews || []) {
          const pvTime = new Date(pageview.timestamp);
          
          // Check time window
          if (pvTime >= new Date(windowStart) && pvTime <= new Date(conversionTime)) {
            
            // PRIORITY 1: Session ID match in IP index data (highest confidence)
            if (session_id && pageview.session_id === session_id) {
              matches.push({
                ...pageview,
                attribution_method: 'session_id_match_ip_index',
                confidence: 295, // Slightly lower than lookup key method
                matched_ip: ip,
                match_method: 'ip_index_session_match'
              });
              console.log(`✅ Found session match in IP index: ${session_id}`);
              continue; // Don't check other signals for this pageview
            }
            
            // PRIORITY 2: Device signature match in IP index data
            if (device_signature && pageview.canvas_fingerprint && 
                pageview.canvas_fingerprint.includes(device_signature)) {
              matches.push({
                ...pageview,
                attribution_method: 'device_signature_match_ip_index',
                confidence: 255, // Slightly lower than lookup key method
                matched_ip: ip,
                match_method: 'ip_index_device_match'
              });
              console.log(`✅ Found device match in IP index: ${device_signature.substring(0, 10)}...`);
              continue;
            }
            
            // PRIORITY 3: Screen signature match
            if (screen_value && pageview.screen_resolution) {
              const screenHash = hashString(pageview.screen_resolution);
              if (screenHash === screen_value) {
                matches.push({
                  ...pageview,
                  attribution_method: 'screen_signature_match_ip_index',
                  confidence: 195,
                  matched_ip: ip,
                  match_method: 'ip_index_screen_match'
                });
                console.log(`✅ Found screen match in IP index`);
                continue;
              }
            }
            
            // PRIORITY 4: WebGL/GPU signature match
            if (gpu_signature && pageview.webgl_fingerprint && 
                pageview.webgl_fingerprint !== 'unavailable') {
              const webglHash = hashString(pageview.webgl_fingerprint);
              if (webglHash === gpu_signature) {
                matches.push({
                  ...pageview,
                  attribution_method: 'webgl_signature_match_ip_index',
                  confidence: 175,
                  matched_ip: ip,
                  match_method: 'ip_index_webgl_match'
                });
                console.log(`✅ Found WebGL match in IP index`);
                continue;
              }
            }
            
            // PRIORITY 5: IP-only match (if no other signals provided)
            if (!session_id && !device_signature && !screen_value && !gpu_signature) {
              matches.push({
                ...pageview,
                attribution_method: 'ip_only_match_ip_index',
                confidence: 235,
                matched_ip: ip,
                match_method: 'ip_index_ip_match'
              });
            }
          }
        }
      } else {
        console.log(`⚠️ No IP index found for ${ip}`);
      }
      
    } catch (error) {
      console.warn(`⚠️ IP index search failed for ${ip}: ${error.message}`);
    }
  }
  
  // Sort by confidence (highest first)
  matches.sort((a, b) => (b.confidence || 0) - (a.confidence || 0));
  
  console.log(`🔍 IP Index multi-signal search complete: ${matches.length} matches found`);
  return matches;
}

// Hash function for matching (same as store-attribution.js)
function hashString(str) {
  if (!str) return '';
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash;
  }
  return Math.abs(hash).toString(36);
}

// Remove duplicate matches (same pageview found via multiple methods)
function removeDuplicateMatches(matches) {
  const seen = new Set();
  return matches.filter(match => {
    const key = `${match.timestamp}_${match.session_id || match.ip_address}`;
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}
