// query-pageviews-enhanced.js - Enhanced pageview search with session ID support
// Replaces query-pageviews.js with multi-signal attribution matching

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

    console.log('🔍 ENHANCED QUERY: Multi-signal attribution search');
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

    // PRIORITY 2: Device Signature Match (High confidence - 220 points)
    if (device_signature && allMatches.length === 0) {
      console.log(`🔐 Searching by device signature: ${device_signature}`);
      const deviceMatches = await searchByDeviceSignature(redis, device_signature, windowStart, conversionTime);
      if (deviceMatches.length > 0) {
        deviceMatches.forEach(match => {
          match.attribution_method = 'device_signature_match';
          match.confidence = 220;
        });
        allMatches = allMatches.concat(deviceMatches);
        queryMethods.push('device_signature_lookup');
        console.log(`✅ Device signature: ${deviceMatches.length} matches found`);
      }
    }

    // PRIORITY 3: IP Address Matches (Medium confidence - 280/260/240 points based on IP type)
    if (ips_to_check.length > 0 && allMatches.length === 0) {
      console.log(`📍 Searching by IP addresses: ${ips_to_check.join(', ')}`);
      
      for (let i = 0; i < ips_to_check.length; i++) {
        const ip = ips_to_check[i];
        if (!ip || ip === 'unknown') continue;
        
        const ipMatches = await searchByIpAddress(redis, ip, windowStart, conversionTime);
        if (ipMatches.length > 0) {
          // Assign confidence based on IP priority (PIP > CIP > IP)
          const confidence = i === 0 ? 280 : i === 1 ? 260 : 240;
          const ipType = i === 0 ? 'primary_ip' : i === 1 ? 'conversion_ip' : 'fallback_ip';
          
          ipMatches.forEach(match => {
            match.attribution_method = `${ipType}_match`;
            match.confidence = confidence;
          });
          
          allMatches = allMatches.concat(ipMatches);
          queryMethods.push('ip_index_lookup');
          console.log(`✅ IP ${ip}: ${ipMatches.length} matches found`);
          break; // Stop at first IP match to avoid duplicates
        }
      }
    }

    // PRIORITY 4: Screen/GPU Signature Matches (Lower confidence - 200/180 points)
    if (allMatches.length === 0) {
      if (screen_value) {
        console.log(`📺 Searching by screen signature: ${screen_value}`);
        const screenMatches = await searchByScreenSignature(redis, screen_value, windowStart, conversionTime);
        if (screenMatches.length > 0) {
          screenMatches.forEach(match => {
            match.attribution_method = 'screen_signature_match';
            match.confidence = 200;
          });
          allMatches = allMatches.concat(screenMatches);
          queryMethods.push('screen_signature_lookup');
        }
      }
      
      if (gpu_signature && allMatches.length === 0) {
        console.log(`🎮 Searching by GPU signature: ${gpu_signature}`);
        const gpuMatches = await searchByGpuSignature(redis, gpu_signature, windowStart, conversionTime);
        if (gpuMatches.length > 0) {
          gpuMatches.forEach(match => {
            match.attribution_method = 'webgl_signature_match';
            match.confidence = 180;
          });
          allMatches = allMatches.concat(gpuMatches);
          queryMethods.push('webgl_signature_lookup');
        }
      }
    }

    // Remove duplicates and sort by confidence
    const uniqueMatches = removeDuplicateMatches(allMatches);
    uniqueMatches.sort((a, b) => (b.confidence || 0) - (a.confidence || 0));

    const processingTime = Date.now() - startTime;
    console.log(`🏁 Query complete: ${uniqueMatches.length} unique matches in ${processingTime}ms`);

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

// Search by session ID (highest priority)
async function searchBySessionId(redis, sessionId, windowStart, conversionTime) {
  try {
    // Check session index
    const sessionKey = `attribution_session_${sessionId}`;
    const sessionResult = await redis(`get/${sessionKey}`);
    
    if (sessionResult.result) {
      // Get the actual attribution data
      const attributionResult = await redis(`get/${sessionResult.result}`);
      if (attributionResult.result) {
        const pageview = JSON.parse(decodeURIComponent(attributionResult.result));
        const pageviewTime = new Date(pageview.timestamp).getTime();
        
        // Check if within time window
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

// Search by device signature
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

// Search by IP address (existing logic enhanced)
async function searchByIpAddress(redis, ip, windowStart, conversionTime) {
  try {
    const encodedIp = ip.replace(/:/g, '_');
    const ipKey = `attribution_ip_${encodedIp}`;
    const ipResult = await redis(`get/${ipKey}`);
    
    if (ipResult.result) {
      // Could be multiple pageviews for same IP
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

// Search by screen signature
async function searchByScreenSignature(redis, screenValue, windowStart, conversionTime) {
  try {
    const screenKey = `attribution_screen_${screenValue}`;
    const screenResult = await redis(`get/${screenKey}`);
    
    if (screenResult.result) {
      const attributionResult = await redis(`get/${screenResult.result}`);
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
    console.warn('⚠️ Screen signature search failed:', error);
    return [];
  }
}

// Search by GPU signature  
async function searchByGpuSignature(redis, gpuSig, windowStart, conversionTime) {
  try {
    const gpuKey = `attribution_webgl_${gpuSig}`;
    const gpuResult = await redis(`get/${gpuKey}`);
    
    if (gpuResult.result) {
      const attributionResult = await redis(`get/${gpuResult.result}`);
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
    console.warn('⚠️ GPU signature search failed:', error);
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
