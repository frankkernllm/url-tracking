// query-pageviews-enhanced.js - Enhanced pageview search with OPTIMIZED multi-signal attribution
// UPDATED: Uses enhanced IP indexes with complete attribution data

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

    console.log('🔍 ENHANCED QUERY: Multi-signal attribution search with optimized IP indexes');
    console.log(`   🕐 Window: ${window_hours}h before ${conversion_timestamp}`);
    console.log(`   📍 IPs: ${ips_to_check.length}`);
    console.log(`   🔐 Session: ${!!session_id}`);
    console.log(`   📱 Device sig: ${!!device_signature}`);

    const conversionTime = new Date(conversion_timestamp).getTime();
    const windowStart = conversionTime - (window_hours * 60 * 60 * 1000);
    
    let allMatches = [];
    const queryMethods = [];

    // PRIORITY 1: Enhanced IP Index Multi-Signal Search (FASTEST - uses optimized indexes)
    if (ips_to_check.length > 0) {
      console.log(`🚀 PRIMARY: Enhanced IP index multi-signal search`);
      const ipMatches = await searchByEnhancedIPIndexes(redis, ips_to_check, session_id, device_signature, screen_value, gpu_signature, windowStart, conversionTime);
      if (ipMatches.length > 0) {
        allMatches = allMatches.concat(ipMatches);
        queryMethods.push('enhanced_ip_index_multi_signal');
        console.log(`✅ Enhanced IP indexes: ${ipMatches.length} matches found`);
        
        // If we found high-confidence matches, return immediately for performance
        const highConfidenceMatches = ipMatches.filter(match => match.confidence >= 250);
        if (highConfidenceMatches.length > 0) {
          console.log(`🎯 High confidence matches found, returning immediately`);
          allMatches = highConfidenceMatches;
        }
      }
    }

    // FALLBACK METHODS: Only if no matches found in enhanced IP indexes
    if (allMatches.length === 0) {
      
      // PRIORITY 2: Session ID Match (Original lookup keys - for very recent data)
      if (session_id) {
        console.log(`🎯 FALLBACK: Direct session ID lookup for recent data`);
        const sessionMatches = await searchBySessionId(redis, session_id, windowStart, conversionTime);
        if (sessionMatches.length > 0) {
          sessionMatches.forEach(match => {
            match.attribution_method = 'session_id_match_direct';
            match.confidence = 300;
          });
          allMatches = allMatches.concat(sessionMatches);
          queryMethods.push('session_id_direct_lookup');
          console.log(`✅ Direct session ID: ${sessionMatches.length} matches found`);
        }
      }

      // PRIORITY 3: Device Signature Match (Original lookup keys - for very recent data)
      if (device_signature && allMatches.length === 0) {
        console.log(`🔐 FALLBACK: Direct device signature lookup`);
        const deviceMatches = await searchByDeviceSignature(redis, device_signature, windowStart, conversionTime);
        if (deviceMatches.length > 0) {
          deviceMatches.forEach(match => {
            match.attribution_method = 'device_signature_match_direct';
            match.confidence = 260;
          });
          allMatches = allMatches.concat(deviceMatches);
          queryMethods.push('device_signature_direct_lookup');
          console.log(`✅ Direct device signature: ${deviceMatches.length} matches found`);
        }
      }

      // PRIORITY 4: Basic IP Address Matches (Original lookup keys)
      if (ips_to_check.length > 0 && allMatches.length === 0) {
        console.log(`📍 FALLBACK: Basic IP address lookup`);
        
        for (let i = 0; i < ips_to_check.length; i++) {
          const ip = ips_to_check[i];
          if (!ip || ip === 'unknown') continue;
          
          const ipMatches = await searchByIpAddress(redis, ip, windowStart, conversionTime);
          if (ipMatches.length > 0) {
            // Assign confidence based on IP priority (PIP > CIP > IP)
            const confidence = i === 0 ? 280 : i === 1 ? 260 : 240;
            const ipType = i === 0 ? 'primary_ip' : i === 1 ? 'conversion_ip' : 'fallback_ip';
            
            ipMatches.forEach(match => {
              match.attribution_method = `${ipType}_match_direct`;
              match.confidence = confidence;
            });
            
            allMatches = allMatches.concat(ipMatches);
            queryMethods.push('ip_direct_lookup');
            console.log(`✅ Direct IP ${ip}: ${ipMatches.length} matches found`);
            break; // Stop at first IP match to avoid duplicates
          }
        }
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
        enhanced_capabilities_used: queryMethods.includes('enhanced_ip_index_multi_signal'),
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

// 🚀 NEW: Enhanced IP index search with complete multi-signal attribution
async function searchByEnhancedIPIndexes(redis, ipsToCheck, sessionId, deviceSignature, screenValue, gpuSignature, windowStart, windowEnd) {
  const matches = [];
  
  console.log(`🚀 Searching enhanced IP indexes for multi-signal attribution...`);
  
  for (const ip of ipsToCheck) {
    const encodedIP = ip.replace(/:/g, '_');
    const ipIndexKey = `pageview_index_ip:${encodedIP}`;
    
    try {
      const indexData = await redis(`get/${ipIndexKey}`);
      
      if (indexData?.result) {
        const parsed = JSON.parse(decodeURIComponent(indexData.result));
        
        // Verify this is an enhanced index with multi-signal data
        if (!parsed.multi_signal_ready) {
          console.log(`⚠️ IP index ${ip} not enhanced yet, skipping`);
          continue;
        }
        
        console.log(`📊 Enhanced IP index found for ${ip}: ${parsed.pageview_count} pageviews with multi-signal data`);
        
        // Filter pageviews within time window
        const windowPageviews = parsed.pageviews.filter(pv => {
          const pvTime = new Date(pv.timestamp);
          return pvTime >= windowStart && pvTime <= windowEnd;
        });
        
        // 🎯 ENHANCED: Multi-signal matching within IP index
        for (const pv of windowPageviews) {
          let confidence = 240; // Base IP match confidence
          let attributionMethod = 'ip_index_match';
          
          // 🏆 PRIORITY 1: Session ID match (highest confidence)
          if (sessionId && pv.session_id === sessionId) {
            confidence = 295; // Slightly lower than direct session lookup (300)
            attributionMethod = 'session_id_match_ip_index';
            console.log(`🎯 Session ID match found in IP index: ${sessionId}`);
          }
          // 🥈 PRIORITY 2: Device signature match
          else if (deviceSignature && pv.canvas_fingerprint === deviceSignature) {
            confidence = 255; // Slightly lower than direct device lookup (260)
            attributionMethod = 'device_signature_match_ip_index';
            console.log(`🔐 Device signature match found in IP index`);
          }
          // 🥉 PRIORITY 3: Screen signature match
          else if (screenValue && pv.screen_resolution && hashString(pv.screen_resolution) === screenValue) {
            confidence = 195; // Slightly lower than direct screen lookup (200)
            attributionMethod = 'screen_signature_match_ip_index';
            console.log(`📺 Screen signature match found in IP index`);
          }
          // 🎮 PRIORITY 4: GPU signature match
          else if (gpuSignature && pv.webgl_fingerprint && hashString(pv.webgl_fingerprint) === gpuSignature) {
            confidence = 175; // Slightly lower than direct GPU lookup (180)
            attributionMethod = 'webgl_signature_match_ip_index';
            console.log(`🎮 WebGL signature match found in IP index`);
          }
          
          matches.push({
            ...pv,
            matched_ip: ip,
            match_method: 'enhanced_ip_index_multi_signal',
            attribution_method: attributionMethod,
            confidence: confidence,
            index_source: 'enhanced_ip_index'
          });
        }
      } else {
        console.log(`⚠️ No enhanced IP index found for ${ip}`);
      }
      
    } catch (error) {
      console.warn(`⚠️ Enhanced IP index search failed for ${ip}:`, error.message);
    }
  }
  
  console.log(`✅ Enhanced IP index search complete: ${matches.length} matches found`);
  return matches;
}

// Helper function for hashing (should match store-attribution.js)
function hashString(str) {
  if (!str) return '';
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32-bit integer
  }
  return Math.abs(hash).toString(36);
}

// FALLBACK METHODS: Original search functions for recent data not yet in enhanced indexes

// Search by session ID (fallback for very recent data)
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

// Search by device signature (fallback for very recent data)
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

// Search by IP address (fallback for very recent data)
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

// Remove duplicate matches (same pageview found via multiple signals)
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
