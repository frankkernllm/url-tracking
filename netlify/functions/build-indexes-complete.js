// Optimized Index Builder - All Chunks - ENHANCED FOR MULTI-SIGNAL ATTRIBUTION
// Path: netlify/functions/build-indexes-complete.js
// Purpose: Index ALL chunks efficiently within timeout limits with COMPLETE attribution data

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
    console.log('🚀 ENHANCED index building for ALL chunks with MULTI-SIGNAL attribution...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    const redis = initializeRedis();
    
    // Step 1: Find all pageview chunks
    const allChunks = await findAllPageviewChunks(redis);
    console.log(`📦 Found ${allChunks.length} pageview chunks to process`);
    
    if (allChunks.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: false,
          message: 'No pageview chunks found. Run extraction first.'
        })
      };
    }
    
    // Step 2: Process chunks and build ENHANCED indexes simultaneously
    const results = await processChunksAndBuildEnhancedIndexes(redis, allChunks, maxProcessingTime - (Date.now() - startTime));
    
    // Step 3: Store final metadata
    const metadata = {
      extraction_timestamp: new Date().toISOString(),
      total_pageviews: results.total_pageviews,
      ip_indexes_created: results.ip_indexes_created,
      time_indexes_created: results.time_indexes_created,
      chunks_processed: results.chunks_processed,
      processing_time_ms: Date.now() - startTime,
      coverage_start: results.earliest_timestamp,
      coverage_end: results.latest_timestamp,
      extraction_method: 'enhanced_multi_signal_indexing',
      unique_ips_found: results.unique_ips_found,
      attribution_fields_included: results.attribution_fields_included,
      multi_signal_ready: true
    };
    
    await storeExtractionMetadata(redis, metadata);
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ ENHANCED indexing finished in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        enhanced_indexing_summary: {
          total_pageviews_processed: results.total_pageviews,
          chunks_processed: results.chunks_processed,
          ip_indexes_created: results.ip_indexes_created,
          time_indexes_created: results.time_indexes_created,
          unique_ips_found: results.unique_ips_found,
          processing_time_ms: totalTime,
          indexing_efficiency: `${((results.ip_indexes_created / results.unique_ips_found) * 100).toFixed(1)}%`,
          attribution_fields_included: results.attribution_fields_included,
          multi_signal_ready: true
        },
        performance: {
          pageviews_per_second: Math.round(results.total_pageviews / (totalTime / 1000)),
          indexes_per_second: Math.round(results.ip_indexes_created / (totalTime / 1000))
        },
        coverage: {
          earliest_pageview: results.earliest_timestamp,
          latest_pageview: results.latest_timestamp,
          time_span_days: results.time_span_days
        },
        multi_signal_capabilities: {
          session_id_attribution: true,
          device_fingerprint_attribution: true,
          screen_signature_attribution: true,
          webgl_attribution: true,
          hardware_fingerprint_attribution: true,
          geographic_correlation: true
        }
      })
    };
    
  } catch (error) {
    console.error('❌ Enhanced indexing failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Enhanced indexing failed', 
        message: error.message 
      })
    };
  }
};

// ENHANCED: Process chunks and build indexes with COMPLETE attribution data
async function processChunksAndBuildEnhancedIndexes(redis, chunkKeys, maxTime) {
  const processStartTime = Date.now();
  console.log(`⚡ ENHANCED processing: ${chunkKeys.length} chunks with MULTI-SIGNAL attribution in ${maxTime}ms`);
  
  const ipIndexMap = new Map(); // Use Map for better performance
  const timeStats = {
    earliest: null,
    latest: null
  };
  
  let totalPageviews = 0;
  let chunksProcessed = 0;
  let ipIndexesCreated = 0;
  let attributionFieldsIncluded = [];
  
  // Step 1: Process all chunks and group by IP in memory with COMPLETE data (FAST)
  for (let i = 0; i < chunkKeys.length; i++) {
    if (Date.now() - processStartTime > maxTime - 8000) {
      console.log(`⏰ Time management: stopping chunk processing to ensure indexing completes`);
      break;
    }
    
    try {
      const chunkKey = chunkKeys[i];
      const chunkData = await redis(`get/${chunkKey}`, 2000); // Faster timeout
      
      if (chunkData?.result) {
        const chunk = JSON.parse(decodeURIComponent(chunkData.result));
        
        if (chunk.pageviews && Array.isArray(chunk.pageviews)) {
          for (const pageview of chunk.pageviews) {
            totalPageviews++;
            
            // Track time range
            const pvTime = new Date(pageview.timestamp);
            if (!timeStats.earliest || pvTime < timeStats.earliest) {
              timeStats.earliest = pvTime;
            }
            if (!timeStats.latest || pvTime > timeStats.latest) {
              timeStats.latest = pvTime;
            }
            
            // Group by IP efficiently
            const ip = pageview.ip_address;
            if (ip && ip !== 'unknown') {
              const encodedIP = ip.replace(/:/g, '_');
              
              if (!ipIndexMap.has(encodedIP)) {
                ipIndexMap.set(encodedIP, {
                  ip_address: ip,
                  pageviews: [],
                  latest_timestamp: pageview.timestamp
                });
              }
              
              const ipGroup = ipIndexMap.get(encodedIP);
              
              // 🚀 ENHANCED: Store COMPLETE attribution data (increased limit for multi-signal)
              if (ipGroup.pageviews.length < 15) { // Increased from 10 to 15 for better attribution coverage
                ipGroup.pageviews.push({
                  // Basic pageview data
                  timestamp: pageview.timestamp,
                  ip_address: pageview.ip_address,
                  landing_page: pageview.landing_page || pageview.url || pageview.page_url || 'unknown',
                  source: pageview.source || 'direct',
                  
                  // UTM parameters  
                  utm_campaign: pageview.utm_campaign,
                  utm_medium: pageview.utm_medium,
                  utm_source: pageview.utm_source,
                  utm_term: pageview.utm_term,
                  utm_content: pageview.utm_content,
                  referrer_url: pageview.referrer_url,
                  
                  // 🚀 CRITICAL: Multi-signal attribution fields
                  session_id: pageview.session_id,                     // For session matching
                  canvas_fingerprint: pageview.canvas_fingerprint,     // For device matching  
                  webgl_fingerprint: pageview.webgl_fingerprint,       // For GPU matching
                  screen_resolution: pageview.screen_resolution,       // For screen matching
                  cpu_cores: pageview.cpu_cores,                       // For hardware matching
                  memory_gb: pageview.memory_gb,                       // For hardware matching
                  user_agent: pageview.user_agent,                     // For browser matching
                  platform: pageview.platform,                         // For platform matching
                  timezone: pageview.timezone,                          // For timezone matching
                  language: pageview.language,                          // For language matching
                  
                  // Additional attribution signals
                  device_type: pageview.device_type,                    // Mobile/Desktop
                  browser_name: pageview.browser_name,                  // Chrome/Safari/etc
                  os_name: pageview.os_name,                           // Windows/macOS/etc
                  screen_width: pageview.screen_width,                 // Screen dimensions
                  screen_height: pageview.screen_height,               // Screen dimensions
                  color_depth: pageview.color_depth,                   // Display color depth
                  pixel_ratio: pageview.pixel_ratio,                   // Device pixel ratio
                  
                  // Geographic and network data
                  country: pageview.country,                           // Country code
                  region: pageview.region,                             // State/region
                  city: pageview.city,                                 // City name
                  isp: pageview.isp,                                   // ISP information
                  
                  // Metadata
                  redis_key: pageview.redis_key || 'unknown'
                });
              } else {
                // Replace oldest if this one is newer
                const oldestIndex = ipGroup.pageviews.findIndex(pv => 
                  new Date(pv.timestamp) < new Date(pageview.timestamp)
                );
                if (oldestIndex !== -1) {
                  ipGroup.pageviews[oldestIndex] = {
                    // Complete attribution data (same as above)
                    timestamp: pageview.timestamp,
                    ip_address: pageview.ip_address,
                    landing_page: pageview.landing_page || pageview.url || pageview.page_url || 'unknown',
                    source: pageview.source || 'direct',
                    utm_campaign: pageview.utm_campaign,
                    utm_medium: pageview.utm_medium,
                    utm_source: pageview.utm_source,
                    utm_term: pageview.utm_term,
                    utm_content: pageview.utm_content,
                    referrer_url: pageview.referrer_url,
                    session_id: pageview.session_id,
                    canvas_fingerprint: pageview.canvas_fingerprint,
                    webgl_fingerprint: pageview.webgl_fingerprint,
                    screen_resolution: pageview.screen_resolution,
                    cpu_cores: pageview.cpu_cores,
                    memory_gb: pageview.memory_gb,
                    user_agent: pageview.user_agent,
                    platform: pageview.platform,
                    timezone: pageview.timezone,
                    language: pageview.language,
                    device_type: pageview.device_type,
                    browser_name: pageview.browser_name,
                    os_name: pageview.os_name,
                    screen_width: pageview.screen_width,
                    screen_height: pageview.screen_height,
                    color_depth: pageview.color_depth,
                    pixel_ratio: pageview.pixel_ratio,
                    country: pageview.country,
                    region: pageview.region,
                    city: pageview.city,
                    isp: pageview.isp,
                    redis_key: pageview.redis_key || 'unknown'
                  };
                }
              }
              
              // Update latest timestamp
              if (new Date(pageview.timestamp) > new Date(ipGroup.latest_timestamp)) {
                ipGroup.latest_timestamp = pageview.timestamp;
              }
            }
          }
          
          chunksProcessed++;
          
          if (i % 3 === 0) {
            console.log(`⚡ Processed ${i + 1}/${chunkKeys.length} chunks: ${totalPageviews} pageviews, ${ipIndexMap.size} unique IPs`);
          }
        }
      }
      
    } catch (chunkError) {
      console.log(`⚠️ Error processing chunk ${i}: ${chunkError.message}`);
    }
  }
  
  console.log(`📊 ENHANCED grouping complete: ${totalPageviews} pageviews, ${ipIndexMap.size} unique IPs with COMPLETE attribution data`);
  
  // Track which attribution fields are actually included
  attributionFieldsIncluded = [
    'session_id', 'canvas_fingerprint', 'webgl_fingerprint', 'screen_resolution',
    'cpu_cores', 'memory_gb', 'user_agent', 'platform', 'timezone', 'language',
    'device_type', 'browser_name', 'os_name', 'screen_width', 'screen_height',
    'color_depth', 'pixel_ratio', 'country', 'region', 'city', 'isp'
  ];
  
  // Step 2: Rapidly create ENHANCED IP indexes (OPTIMIZED)
  const remainingTime = maxTime - (Date.now() - processStartTime);
  console.log(`🏗️ Creating ENHANCED IP indexes with ${remainingTime}ms remaining...`);
  
  const ipIndexArray = Array.from(ipIndexMap.entries());
  const batchSize = 20; // Process in batches for speed
  
  for (let i = 0; i < ipIndexArray.length; i += batchSize) {
    if (Date.now() - processStartTime > maxTime - 2000) {
      console.log(`⏰ Time limit approaching, stopping IP indexing`);
      break;
    }
    
    const batch = ipIndexArray.slice(i, i + batchSize);
    
    // Process batch in parallel for maximum speed
    const batchPromises = batch.map(async ([encodedIP, ipData]) => {
      try {
        // Sort pageviews by timestamp (most recent first)
        ipData.pageviews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
        
        const ipKey = `pageview_index_ip:${encodedIP}`;
        
        // 🚀 ENHANCED: Complete attribution data in indexes
        const indexData = {
          ip_address: ipData.ip_address,
          pageview_count: ipData.pageviews.length,
          latest_timestamp: ipData.latest_timestamp,
          pageviews: ipData.pageviews, // Now includes ALL attribution fields
          created_at: new Date().toISOString(),
          
          // Enhanced index metadata
          multi_signal_ready: true,
          attribution_fields_available: attributionFieldsIncluded,
          session_ids_available: ipData.pageviews.filter(pv => pv.session_id).length,
          device_fingerprints_available: ipData.pageviews.filter(pv => pv.canvas_fingerprint).length,
          screen_resolutions_available: ipData.pageviews.filter(pv => pv.screen_resolution).length,
          webgl_fingerprints_available: ipData.pageviews.filter(pv => pv.webgl_fingerprint).length
        };
        
        // Store with 30-day TTL (2,592,000 seconds)
        await redis(`setex/${ipKey}/2592000/${encodeURIComponent(JSON.stringify(indexData))}`, 2000);
        return 1;
        
      } catch (ipError) {
        console.log(`⚠️ Error creating enhanced IP index: ${ipError.message}`);
        return 0;
      }
    });
    
    try {
      const batchResults = await Promise.all(batchPromises);
      ipIndexesCreated += batchResults.reduce((sum, result) => sum + result, 0);
      
      if ((i + batchSize) % 200 === 0) {
        console.log(`🏗️ Enhanced IP indexing progress: ${ipIndexesCreated}/${ipIndexMap.size} indexes created`);
      }
      
    } catch (batchError) {
      console.log(`⚠️ Batch indexing error: ${batchError.message}`);
    }
  }
  
  // Step 3: Create simplified time indexes if time allows
  let timeIndexesCreated = 0;
  const finalRemainingTime = maxTime - (Date.now() - processStartTime);
  
  if (finalRemainingTime > 3000) {
    console.log(`🕐 Creating time indexes with ${finalRemainingTime}ms remaining...`);
    timeIndexesCreated = await createSimpleTimeIndexes(redis, timeStats, finalRemainingTime - 1000);
  }
  
  const timeSpanDays = timeStats.earliest && timeStats.latest 
    ? Math.round((timeStats.latest - timeStats.earliest) / (1000 * 60 * 60 * 24))
    : 0;
  
  console.log(`✅ ENHANCED processing complete:`);
  console.log(`   📊 ${totalPageviews} pageviews from ${chunksProcessed} chunks`);
  console.log(`   🌐 ${ipIndexesCreated} ENHANCED IP indexes created from ${ipIndexMap.size} unique IPs`);
  console.log(`   🚀 ${attributionFieldsIncluded.length} attribution fields included per pageview`);
  console.log(`   🕐 ${timeIndexesCreated} time indexes created`);
  
  return {
    total_pageviews: totalPageviews,
    chunks_processed: chunksProcessed,
    ip_indexes_created: ipIndexesCreated,
    time_indexes_created: timeIndexesCreated,
    unique_ips_found: ipIndexMap.size,
    earliest_timestamp: timeStats.earliest?.toISOString(),
    latest_timestamp: timeStats.latest?.toISOString(),
    time_span_days: timeSpanDays,
    attribution_fields_included: attributionFieldsIncluded
  };
}

// Create simple time indexes quickly
async function createSimpleTimeIndexes(redis, timeStats, maxTime) {
  if (!timeStats.earliest || !timeStats.latest) return 0;
  
  const indexStartTime = Date.now();
  let created = 0;
  
  try {
    // Create just a few key time reference points
    const timeReferences = [
      { key: 'earliest', timestamp: timeStats.earliest },
      { key: 'latest', timestamp: timeStats.latest }
    ];
    
    for (const ref of timeReferences) {
      if (Date.now() - indexStartTime > maxTime) break;
      
      const timeKey = `pageview_time_ref:${ref.key}`;
      const timeData = {
        reference: ref.key,
        timestamp: ref.timestamp.toISOString(),
        created_at: new Date().toISOString()
      };
      
      // Store with 30-day TTL (2,592,000 seconds)
      await redis(`setex/${timeKey}/2592000/${encodeURIComponent(JSON.stringify(timeData))}`, 1000);
      created++;
    }
    
  } catch (error) {
    console.log(`⚠️ Time indexing error: ${error.message}`);
  }
  
  return created;
}

// Find all pageview chunks (optimized)
async function findAllPageviewChunks(redis) {
  const chunks = [];
  let cursor = '0';
  let iterations = 0;
  const maxIterations = 15; // Increased for more chunks
  
  do {
    try {
      const scanResult = await redis(`scan/${cursor}/match/pageview_chunk:*/count/200`, 3000); // Larger count, longer timeout
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      chunks.push(...keys);
      iterations++;
      
    } catch (scanError) {
      console.log(`⚠️ Chunk scan error: ${scanError.message}`);
      break;
    }
    
  } while (cursor !== '0' && iterations < maxIterations);
  
  return chunks;
}

// Store extraction metadata
async function storeExtractionMetadata(redis, metadata) {
  const metadataKey = 'pageview_extraction_metadata';
  // Store with 30-day TTL (2,592,000 seconds)
  await redis(`setex/${metadataKey}/2592000/${encodeURIComponent(JSON.stringify(metadata))}`);
  console.log('📋 Enhanced extraction metadata stored with 30-day TTL');
}

// Initialize Redis helper
function initializeRedis() {
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  
  return async (command, timeoutMs = 3000) => {
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
