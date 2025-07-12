// Optimized Index Builder - All Chunks with 30-Day TTL
// Path: netlify/functions/build-indexes-complete.js
// Purpose: Index ALL chunks efficiently within timeout limits

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
    console.log('🚀 OPTIMIZED index building for ALL chunks...');
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
    
    // Step 2: Process chunks and build indexes simultaneously (OPTIMIZED)
    const results = await processChunksAndBuildIndexes(redis, allChunks, maxProcessingTime - (Date.now() - startTime));
    
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
      extraction_method: 'optimized_complete_indexing',
      unique_ips_found: results.unique_ips_found,
      ttl_days: 30
    };
    
    await storeExtractionMetadata(redis, metadata);
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ COMPLETE indexing finished in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        complete_indexing_summary: {
          total_pageviews_processed: results.total_pageviews,
          chunks_processed: results.chunks_processed,
          ip_indexes_created: results.ip_indexes_created,
          time_indexes_created: results.time_indexes_created,
          unique_ips_found: results.unique_ips_found,
          processing_time_ms: totalTime,
          indexing_efficiency: `${((results.ip_indexes_created / results.unique_ips_found) * 100).toFixed(1)}%`,
          ttl_days: 30
        },
        performance: {
          pageviews_per_second: Math.round(results.total_pageviews / (totalTime / 1000)),
          indexes_per_second: Math.round(results.ip_indexes_created / (totalTime / 1000))
        },
        coverage: {
          earliest_pageview: results.earliest_timestamp,
          latest_pageview: results.latest_timestamp,
          time_span_days: results.time_span_days
        }
      })
    };
    
  } catch (error) {
    console.error('❌ Optimized indexing failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Optimized indexing failed', 
        message: error.message 
      })
    };
  }
};

// OPTIMIZED: Process chunks and build indexes simultaneously
async function processChunksAndBuildIndexes(redis, chunkKeys, maxTime) {
  const processStartTime = Date.now();
  console.log(`⚡ OPTIMIZED processing: ${chunkKeys.length} chunks in ${maxTime}ms`);
  
  const ipIndexMap = new Map(); // Use Map for better performance
  const timeStats = {
    earliest: null,
    latest: null
  };
  
  let totalPageviews = 0;
  let chunksProcessed = 0;
  let ipIndexesCreated = 0;
  
  // Step 1: Process all chunks and group by IP in memory (FAST)
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
              
              // Store only essential data + keep most recent
              if (ipGroup.pageviews.length < 10) { // Limit to 10 per IP for speed
                ipGroup.pageviews.push({
                  timestamp: pageview.timestamp,
                  landing_page: pageview.landing_page,
                  source: pageview.source,
                  utm_campaign: pageview.utm_campaign,
                  utm_medium: pageview.utm_medium,
                  utm_source: pageview.utm_source
                });
              } else {
                // Replace oldest if this one is newer
                const oldestIndex = ipGroup.pageviews.findIndex(pv => 
                  new Date(pv.timestamp) < new Date(pageview.timestamp)
                );
                if (oldestIndex !== -1) {
                  ipGroup.pageviews[oldestIndex] = {
                    timestamp: pageview.timestamp,
                    landing_page: pageview.landing_page,
                    source: pageview.source,
                    utm_campaign: pageview.utm_campaign,
                    utm_medium: pageview.utm_medium,
                    utm_source: pageview.utm_source
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
  
  console.log(`📊 Grouping complete: ${totalPageviews} pageviews, ${ipIndexMap.size} unique IPs`);
  
  // Step 2: Rapidly create IP indexes with 30-day TTL (OPTIMIZED)
  const remainingTime = maxTime - (Date.now() - processStartTime);
  console.log(`🏗️ Creating IP indexes with 30-day TTL using ${remainingTime}ms remaining...`);
  
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
        const indexData = {
          ip_address: ipData.ip_address,
          pageview_count: ipData.pageviews.length,
          latest_timestamp: ipData.latest_timestamp,
          pageviews: ipData.pageviews, // Already limited to 10
          created_at: new Date().toISOString(),
          ttl_days: 30
        };
        
        await redis(`setex/${ipKey}/2592000/${encodeURIComponent(JSON.stringify(indexData))}`, 2000); // 30 days TTL, faster timeout
        return 1;
        
      } catch (ipError) {
        console.log(`⚠️ Error creating IP index: ${ipError.message}`);
        return 0;
      }
    });
    
    try {
      const batchResults = await Promise.all(batchPromises);
      ipIndexesCreated += batchResults.reduce((sum, result) => sum + result, 0);
      
      if ((i + batchSize) % 200 === 0) {
        console.log(`🏗️ IP indexing progress: ${ipIndexesCreated}/${ipIndexMap.size} indexes created (30-day TTL)`);
      }
      
    } catch (batchError) {
      console.log(`⚠️ Batch indexing error: ${batchError.message}`);
    }
  }
  
  // Step 3: Create simplified time indexes if time allows
  let timeIndexesCreated = 0;
  const finalRemainingTime = maxTime - (Date.now() - processStartTime);
  
  if (finalRemainingTime > 3000) {
    console.log(`🕐 Creating time indexes with 30-day TTL using ${finalRemainingTime}ms remaining...`);
    timeIndexesCreated = await createSimpleTimeIndexes(redis, timeStats, finalRemainingTime - 1000);
  }
  
  const timeSpanDays = timeStats.earliest && timeStats.latest 
    ? Math.round((timeStats.latest - timeStats.earliest) / (1000 * 60 * 60 * 24))
    : 0;
  
  console.log(`✅ OPTIMIZED processing complete:`);
  console.log(`   📊 ${totalPageviews} pageviews from ${chunksProcessed} chunks`);
  console.log(`   🌐 ${ipIndexesCreated} IP indexes created from ${ipIndexMap.size} unique IPs (30-day TTL)`);
  console.log(`   🕐 ${timeIndexesCreated} time indexes created (30-day TTL)`);
  
  return {
    total_pageviews: totalPageviews,
    chunks_processed: chunksProcessed,
    ip_indexes_created: ipIndexesCreated,
    time_indexes_created: timeIndexesCreated,
    unique_ips_found: ipIndexMap.size,
    earliest_timestamp: timeStats.earliest?.toISOString(),
    latest_timestamp: timeStats.latest?.toISOString(),
    time_span_days: timeSpanDays
  };
}

// Create simple time indexes quickly with 30-day TTL
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
        created_at: new Date().toISOString(),
        ttl_days: 30
      };
      
      await redis(`setex/${timeKey}/2592000/${encodeURIComponent(JSON.stringify(timeData))}`, 1000); // 30 days TTL
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

// Store extraction metadata with 30-day TTL
async function storeExtractionMetadata(redis, metadata) {
  const metadataKey = 'pageview_extraction_metadata';
  await redis(`setex/${metadataKey}/2592000/${encodeURIComponent(JSON.stringify(metadata))}`); // 30 days TTL
  console.log('📋 Complete extraction metadata stored (30-day TTL)');
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
