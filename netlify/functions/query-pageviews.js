// Fast Pageview Query System
// Path: netlify/functions/query-pageviews.js 
// Purpose: Provide instant pageview queries using pre-built indexes
// Used by staged recovery script to get 24-hour window data without timeout

exports.handler = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false;
  
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST, GET, OPTIONS'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  try {
    const redis = initializeRedis();
    
    if (event.httpMethod === 'POST') {
      const { conversion_timestamp, ips_to_check, window_hours = 24 } = JSON.parse(event.body);
      
      console.log(`🔍 Fast pageview query: ${window_hours}h window before ${conversion_timestamp}`);
      console.log(`🌐 IPs to check: ${ips_to_check?.length || 0}`);
      
      const result = await queryPageviewsInWindow(redis, conversion_timestamp, ips_to_check, window_hours);
      
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify(result)
      };
    }
    
    // GET request - return system status
    const status = await getQuerySystemStatus(redis);
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify(status)
    };
    
  } catch (error) {
    console.error('❌ Query system error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: 'Query failed', message: error.message })
    };
  }
};

// Initialize Redis helper (same as extractor)
function initializeRedis() {
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  
  return async (command, timeoutMs = 5000) => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    
    try {
      const response = await fetch(`${redisUrl}/${command}`, {
        headers: { 
          Authorization: `Bearer ${redisToken}`,
          'Content-Type': 'application/json'
        },
        signal: controller.signal
      });
      
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        throw new Error(`Redis error: ${response.status}`);
      }
      
      return await response.json();
    } catch (error) {
      clearTimeout(timeoutId);
      throw error;
    }
  };
}

// Main query function - get pageviews in time window for specific IPs
async function queryPageviewsInWindow(redis, conversionTimestamp, ipsToCheck, windowHours) {
  const startTime = Date.now();
  const conversionTime = new Date(conversionTimestamp);
  const windowStart = new Date(conversionTime.getTime() - (windowHours * 60 * 60 * 1000));
  
  console.log(`📅 Window: ${windowStart.toISOString()} to ${conversionTime.toISOString()}`);
  
  const results = {
    query_timestamp: new Date().toISOString(),
    conversion_timestamp: conversionTimestamp,
    window_hours: windowHours,
    window_start: windowStart.toISOString(),
    window_end: conversionTime.toISOString(),
    ips_queried: ipsToCheck || [],
    matches_found: [],
    query_methods_used: [],
    processing_time_ms: 0,
    index_status: {}
  };
  
  try {
    // Method 1: Try IP-based indexes first (fastest)
    if (ipsToCheck && ipsToCheck.length > 0) {
      console.log('🚀 Method 1: IP-based index lookup');
      const ipMatches = await queryByIPIndexes(redis, ipsToCheck, windowStart, conversionTime);
      results.matches_found.push(...ipMatches.matches);
      results.query_methods_used.push('ip_index_lookup');
      results.index_status.ip_indexes = ipMatches.status;
      
      if (ipMatches.matches.length > 0) {
        console.log(`✅ IP indexes found ${ipMatches.matches.length} matches`);
        results.processing_time_ms = Date.now() - startTime;
        return results;
      }
    }
    
    // Method 2: Time-based index scanning (fallback)
    console.log('🕐 Method 2: Time-based index scanning');
    const timeMatches = await queryByTimeIndexes(redis, ipsToCheck, windowStart, conversionTime);
    results.matches_found.push(...timeMatches.matches);
    results.query_methods_used.push('time_index_scan');
    results.index_status.time_indexes = timeMatches.status;
    
    if (timeMatches.matches.length > 0) {
      console.log(`✅ Time indexes found ${timeMatches.matches.length} matches`);
    }
    
    // Method 3: Direct Redis scanning (last resort)
    if (results.matches_found.length === 0) {
      console.log('🔍 Method 3: Direct Redis scanning (last resort)');
      const directMatches = await queryByDirectScanning(redis, ipsToCheck, windowStart, conversionTime);
      results.matches_found.push(...directMatches.matches);
      results.query_methods_used.push('direct_scan');
      results.index_status.direct_scan = directMatches.status;
    }
    
    // Sort matches by timestamp (closest to conversion first)
    results.matches_found.sort((a, b) => 
      Math.abs(conversionTime - new Date(a.timestamp)) - 
      Math.abs(conversionTime - new Date(b.timestamp))
    );
    
    results.processing_time_ms = Date.now() - startTime;
    console.log(`🎯 Query complete: ${results.matches_found.length} matches in ${results.processing_time_ms}ms`);
    
    return results;
    
  } catch (error) {
    results.processing_time_ms = Date.now() - startTime;
    results.error = error.message;
    return results;
  }
}

// Query using IP-based indexes (fastest method)
async function queryByIPIndexes(redis, ipsToCheck, windowStart, windowEnd) {
  const matches = [];
  const status = { ips_checked: 0, indexes_found: 0, total_pageviews: 0 };
  
  for (const ip of ipsToCheck) {
    const encodedIP = ip.replace(/:/g, '_');
    const ipIndexKey = `pageview_index_ip:${encodedIP}`;
    
    try {
      status.ips_checked++;
      const indexData = await redis(`get/${ipIndexKey}`);
      
      if (indexData?.result) {
        status.indexes_found++;
        const parsed = JSON.parse(decodeURIComponent(indexData.result));
        status.total_pageviews += parsed.pageview_count;
        
        // Filter pageviews within time window
        const windowMatches = parsed.pageviews.filter(pv => {
          const pvTime = new Date(pv.timestamp);
          return pvTime >= windowStart && pvTime <= windowEnd;
        });
        
        matches.push(...windowMatches.map(pv => ({
          ...pv,
          matched_ip: ip,
          match_method: 'ip_index',
          confidence: 'high'
        })));
      }
      
    } catch (error) {
      console.log(`⚠️ IP index lookup failed for ${ip}: ${error.message}`);
    }
  }
  
  return { matches, status };
}

// Query using time-based indexes (fallback method)
async function queryByTimeIndexes(redis, ipsToCheck, windowStart, windowEnd) {
  const matches = [];
  const status = { hours_checked: 0, indexes_found: 0, total_pageviews: 0 };
  
  // Generate hourly keys for the time window
  const hourlyKeys = generateHourlyKeys(windowStart, windowEnd);
  
  for (const hourKey of hourlyKeys) {
    const timeIndexKey = `pageview_index_hour:${hourKey}`;
    
    try {
      status.hours_checked++;
      const indexData = await redis(`get/${timeIndexKey}`);
      
      if (indexData?.result) {
        status.indexes_found++;
        const parsed = JSON.parse(decodeURIComponent(indexData.result));
        status.total_pageviews += parsed.pageview_count;
        
        // Filter by IP and exact time window
        for (const pageview of parsed.pageviews) {
          const pvTime = new Date(pageview.timestamp);
          
          if (pvTime >= windowStart && pvTime <= windowEnd) {
            // If IPs specified, check for matches
            if (!ipsToCheck || ipsToCheck.length === 0 || ipsToCheck.includes(pageview.ip_address)) {
              matches.push({
                ...pageview,
                matched_ip: pageview.ip_address,
                match_method: 'time_index',
                confidence: 'medium'
              });
            }
          }
        }
      }
      
    } catch (error) {
      console.log(`⚠️ Time index lookup failed for ${hourKey}: ${error.message}`);
    }
  }
  
  return { matches, status };
}

// Direct Redis scanning (last resort)
async function queryByDirectScanning(redis, ipsToCheck, windowStart, windowEnd) {
  const matches = [];
  const status = { keys_scanned: 0, valid_pageviews: 0, time_limit_hit: false };
  
  const scanStartTime = Date.now();
  const maxScanTime = 3000; // 3 seconds max for direct scanning
  
  try {
    let cursor = '0';
    let iteration = 0;
    const maxIterations = 10; // Limit iterations to prevent timeout
    
    do {
      if (Date.now() - scanStartTime > maxScanTime || iteration >= maxIterations) {
        status.time_limit_hit = true;
        break;
      }
      
      const scanResult = await redis(`scan/${cursor}/match/attribution_*/count/100`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      status.keys_scanned += keys.length;
      iteration++;
      
      // Filter main keys (skip lookup keys)
      const mainKeys = keys.filter(key => 
        !key.includes('_ip_') && 
        !key.includes('_session_') && 
        !key.includes('pageview_index_')
      );
      
      // Process in small batches for speed
      for (let i = 0; i < mainKeys.length && Date.now() - scanStartTime < maxScanTime; i += 10) {
        const batch = mainKeys.slice(i, i + 10);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const data = await redis(`get/${key}`);
            if (data?.result) {
              const pageview = JSON.parse(data.result);
              
              if (pageview.timestamp && pageview.ip_address) {
                const pvTime = new Date(pageview.timestamp);
                
                if (pvTime >= windowStart && pvTime <= windowEnd) {
                  status.valid_pageviews++;
                  
                  if (!ipsToCheck || ipsToCheck.length === 0 || ipsToCheck.includes(pageview.ip_address)) {
                    return {
                      ...pageview,
                      matched_ip: pageview.ip_address,
                      match_method: 'direct_scan',
                      confidence: 'low'
                    };
                  }
                }
              }
            }
          } catch (parseError) {
            // Skip invalid data
          }
          return null;
        });
        
        const batchResults = await Promise.all(batchPromises);
        const validResults = batchResults.filter(result => result !== null);
        matches.push(...validResults);
      }
      
    } while (cursor !== '0' && Date.now() - scanStartTime < maxScanTime && iteration < maxIterations);
    
  } catch (error) {
    console.error('❌ Direct scanning error:', error);
  }
  
  return { matches, status };
}

// Generate hourly keys for time window
function generateHourlyKeys(startTime, endTime) {
  const keys = [];
  const current = new Date(startTime);
  current.setMinutes(0, 0, 0); // Start at beginning of hour
  
  while (current <= endTime) {
    const hourKey = `${current.getFullYear()}-${String(current.getMonth() + 1).padStart(2, '0')}-${String(current.getDate()).padStart(2, '0')}-${String(current.getHours()).padStart(2, '0')}`;
    keys.push(hourKey);
    current.setHours(current.getHours() + 1);
  }
  
  return keys;
}

// Get system status
async function getQuerySystemStatus(redis) {
  try {
    const metadataKey = 'pageview_extraction_metadata';
    const metadata = await redis(`get/${metadataKey}`);
    
    if (metadata?.result) {
      const parsed = JSON.parse(decodeURIComponent(metadata.result));
      
      const now = new Date();
      const extractionTime = new Date(parsed.extraction_timestamp);
      const ageMinutes = Math.round((now - extractionTime) / (1000 * 60));
      
      return {
        system_status: 'operational',
        last_extraction: parsed.extraction_timestamp,
        extraction_age_minutes: ageMinutes,
        data_freshness: ageMinutes < 60 ? 'fresh' : ageMinutes < 120 ? 'acceptable' : 'stale',
        coverage: {
          total_pageviews: parsed.total_pageviews,
          time_indexes: parsed.time_indexes_created,
          ip_indexes: parsed.ip_indexes_created,
          coverage_start: parsed.coverage_start,
          coverage_end: parsed.coverage_end
        },
        recommendations: ageMinutes > 60 ? ['Run pageview extraction to refresh indexes'] : []
      };
    } else {
      return {
        system_status: 'needs_initialization',
        message: 'No extraction metadata found',
        recommendations: ['Run pageview extraction to build initial indexes']
      };
    }
    
  } catch (error) {
    return {
      system_status: 'error',
      error: error.message,
      recommendations: ['Check Redis connectivity and run pageview extraction']
    };
  }
}

module.exports = { handler };
