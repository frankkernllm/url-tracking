// Comprehensive Pageview Data Extractor
// Path: netlify/functions/extract-pageviews.js
// Purpose: Extract ALL pageview data from Redis and build time-indexed structures
// Run this periodically (every 30 minutes) to maintain fresh pageview indexes

exports.handler = async (event, context) => {
  // No timeout constraints - this can run for minutes if needed
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
    console.log('🚀 Starting comprehensive pageview extraction...');
    const startTime = Date.now();
    
    // Initialize Redis
    const redis = initializeRedis();
    
    // Step 1: Extract all pageview data (no time limits)
    const allPageviews = await extractAllPageviewsComprehensive(redis);
    
    // Step 2: Build time-based indexes for fast conversion attribution
    const timeIndexes = await buildTimeBasedIndexes(redis, allPageviews);
    
    // Step 3: Build IP-based indexes for instant lookups
    const ipIndexes = await buildIPBasedIndexes(redis, allPageviews);
    
    // Step 4: Store extraction metadata
    await storeExtractionMetadata(redis, {
      extraction_timestamp: new Date().toISOString(),
      total_pageviews: allPageviews.length,
      time_indexes_created: timeIndexes.length,
      ip_indexes_created: ipIndexes.length,
      processing_time_ms: Date.now() - startTime,
      coverage_start: timeIndexes.length > 0 ? timeIndexes[0].timestamp : null,
      coverage_end: timeIndexes.length > 0 ? timeIndexes[timeIndexes.length - 1].timestamp : null
    });
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ Extraction complete in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        extraction_summary: {
          total_pageviews: allPageviews.length,
          time_indexes: timeIndexes.length,
          ip_indexes: ipIndexes.length,
          processing_time_ms: totalTime,
          coverage_days: Math.round((new Date(timeIndexes[timeIndexes.length - 1]?.timestamp) - new Date(timeIndexes[0]?.timestamp)) / (1000 * 60 * 60 * 24)),
          next_extraction_recommended: new Date(Date.now() + 30 * 60 * 1000).toISOString()
        }
      })
    };
    
  } catch (error) {
    console.error('❌ Extraction failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: 'Extraction failed', message: error.message })
    };
  }
};

// Initialize Redis helper
function initializeRedis() {
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  
  return async (command, timeoutMs = 10000) => {
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

// Extract ALL pageview data comprehensively (no time limits)
async function extractAllPageviewsComprehensive(redis) {
  console.log('📊 Extracting all pageview data comprehensively...');
  
  const allPageviews = [];
  let totalKeysScanned = 0;
  let validPageviews = 0;
  let invalidKeys = 0;
  
  try {
    // Comprehensive scanning with multiple patterns
    const patterns = [
      'attribution_*',     // Main attribution data
      'attribution:*',     // Colon format data
      'pageview_*',        // Direct pageview data
      'pageviews:*'        // Alternate pageview format
    ];
    
    for (const pattern of patterns) {
      console.log(`🔍 Scanning pattern: ${pattern}`);
      
      let cursor = '0';
      let patternKeys = 0;
      
      do {
        const scanResult = await redis(`scan/${cursor}/match/${pattern}/count/1000`);
        
        if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
          break;
        }
        
        cursor = scanResult.result[0];
        const keys = scanResult.result[1] || [];
        totalKeysScanned += keys.length;
        patternKeys += keys.length;
        
        // Filter out lookup keys to get main data only
        const mainKeys = keys.filter(key => 
          !key.includes('_ip_') && 
          !key.includes('_session_') && 
          !key.includes('_fp_') && 
          !key.includes('_screen_') && 
          !key.includes('_webgl_') && 
          !key.includes('_geo_')
        );
        
        // Process keys in batches for memory efficiency
        const batchSize = 50;
        for (let i = 0; i < mainKeys.length; i += batchSize) {
          const batch = mainKeys.slice(i, i + batchSize);
          
          // Process batch in parallel for speed
          const batchPromises = batch.map(async (key) => {
            try {
              const data = await redis(`get/${key}`);
              if (data?.result) {
                const parsed = JSON.parse(data.result);
                
                // Validate this is pageview data
                if (parsed.timestamp && parsed.ip_address && 
                    (parsed.landing_page || parsed.url || parsed.page_url)) {
                  return {
                    timestamp: parsed.timestamp,
                    ip_address: parsed.ip_address,
                    landing_page: parsed.landing_page || parsed.url || parsed.page_url,
                    source: parsed.source || 'unknown',
                    utm_campaign: parsed.utm_campaign,
                    utm_medium: parsed.utm_medium,
                    utm_source: parsed.utm_source,
                    utm_term: parsed.utm_term,
                    utm_content: parsed.utm_content,
                    referrer_url: parsed.referrer_url,
                    redis_key: key // Store original key for debugging
                  };
                }
              }
            } catch (parseError) {
              invalidKeys++;
            }
            return null;
          });
          
          const batchResults = await Promise.all(batchPromises);
          const validResults = batchResults.filter(result => result !== null);
          allPageviews.push(...validResults);
          validPageviews += validResults.length;
          
          // Progress logging
          if (allPageviews.length % 1000 === 0) {
            console.log(`📊 Progress: ${allPageviews.length} pageviews extracted from ${totalKeysScanned} keys scanned`);
          }
        }
        
      } while (cursor !== '0');
      
      console.log(`✅ Pattern ${pattern}: ${patternKeys} keys scanned, ${validPageviews} valid pageviews so far`);
    }
    
    // Sort by timestamp for time-based operations
    allPageviews.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    
    console.log(`🎯 Extraction complete: ${allPageviews.length} pageviews from ${totalKeysScanned} keys`);
    console.log(`📅 Date range: ${allPageviews[0]?.timestamp} to ${allPageviews[allPageviews.length - 1]?.timestamp}`);
    console.log(`⚠️ Invalid keys skipped: ${invalidKeys}`);
    
    return allPageviews;
    
  } catch (error) {
    console.error('❌ Comprehensive extraction error:', error);
    return allPageviews; // Return what we have so far
  }
}

// Build time-based indexes for fast 24-hour window queries
async function buildTimeBasedIndexes(redis, pageviews) {
  console.log('🕐 Building time-based indexes...');
  
  const timeIndexes = [];
  const indexedDates = new Set();
  
  // Group pageviews by hour for efficient 24-hour window queries
  const pageviewsByHour = {};
  
  for (const pageview of pageviews) {
    const timestamp = new Date(pageview.timestamp);
    const hourKey = `${timestamp.getFullYear()}-${String(timestamp.getMonth() + 1).padStart(2, '0')}-${String(timestamp.getDate()).padStart(2, '0')}-${String(timestamp.getHours()).padStart(2, '0')}`;
    
    if (!pageviewsByHour[hourKey]) {
      pageviewsByHour[hourKey] = [];
    }
    
    pageviewsByHour[hourKey].push(pageview);
  }
  
  // Store hourly indexes in Redis
  for (const [hourKey, hourPageviews] of Object.entries(pageviewsByHour)) {
    const indexKey = `pageview_index_hour:${hourKey}`;
    const indexData = {
      hour_key: hourKey,
      pageview_count: hourPageviews.length,
      pageviews: hourPageviews,
      created_at: new Date().toISOString()
    };
    
    await redis(`setex/${indexKey}/3600/${encodeURIComponent(JSON.stringify(indexData))}`); // 1 hour TTL
    timeIndexes.push({ timestamp: hourPageviews[0].timestamp, hour_key: hourKey, count: hourPageviews.length });
    indexedDates.add(hourKey);
  }
  
  console.log(`✅ Time indexes: ${timeIndexes.length} hourly indexes created`);
  return timeIndexes;
}

// Build IP-based indexes for instant attribution lookups
async function buildIPBasedIndexes(redis, pageviews) {
  console.log('🌐 Building IP-based indexes...');
  
  const ipIndexes = {};
  let totalIPs = 0;
  
  // Group pageviews by IP address
  for (const pageview of pageviews) {
    const ip = pageview.ip_address;
    if (!ip || ip === 'unknown') continue;
    
    const encodedIP = ip.replace(/:/g, '_'); // Handle IPv6
    const ipKey = `pageview_index_ip:${encodedIP}`;
    
    if (!ipIndexes[ipKey]) {
      ipIndexes[ipKey] = [];
      totalIPs++;
    }
    
    ipIndexes[ipKey].push(pageview);
  }
  
  // Store IP indexes in Redis with longer TTL
  for (const [ipKey, ipPageviews] of Object.entries(ipIndexes)) {
    // Sort by timestamp (most recent first)
    ipPageviews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    
    const indexData = {
      ip_address: ipPageviews[0].ip_address,
      pageview_count: ipPageviews.length,
      latest_timestamp: ipPageviews[0].timestamp,
      earliest_timestamp: ipPageviews[ipPageviews.length - 1].timestamp,
      pageviews: ipPageviews.slice(0, 50), // Store up to 50 most recent pageviews per IP
      created_at: new Date().toISOString()
    };
    
    await redis(`setex/${ipKey}/7200/${encodeURIComponent(JSON.stringify(indexData))}`); // 2 hours TTL
  }
  
  console.log(`✅ IP indexes: ${totalIPs} unique IPs indexed`);
  return Object.keys(ipIndexes);
}

// Store extraction metadata for monitoring
async function storeExtractionMetadata(redis, metadata) {
  const metadataKey = 'pageview_extraction_metadata';
  await redis(`setex/${metadataKey}/3600/${encodeURIComponent(JSON.stringify(metadata))}`); // 1 hour TTL
  console.log('📋 Extraction metadata stored');
}

// Handler is already exported via exports.handler above
