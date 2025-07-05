// Build Indexes from Existing Chunks
// Path: netlify/functions/build-indexes.js
// Purpose: Build IP and time indexes from previously extracted pageview chunks

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
    console.log('🏗️ Building indexes from existing pageview chunks...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    const redis = initializeRedis();
    
    // Step 1: Find all pageview chunks
    const allChunks = await findAllPageviewChunks(redis);
    console.log(`📦 Found ${allChunks.length} pageview chunks`);
    
    if (allChunks.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: false,
          message: 'No pageview chunks found. Run extraction first.',
          recommendations: ['Run extract-pageviews-chunked to extract data first']
        })
      };
    }
    
    // Step 2: Load and combine all pageviews from chunks
    const allPageviews = await loadPageviewsFromChunks(redis, allChunks, maxProcessingTime - (Date.now() - startTime));
    console.log(`📊 Loaded ${allPageviews.length} total pageviews`);
    
    if (allPageviews.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: false,
          message: 'No valid pageviews found in chunks'
        })
      };
    }
    
    // Step 3: Build IP-based indexes
    const ipIndexResults = await buildIPIndexes(redis, allPageviews, maxProcessingTime - (Date.now() - startTime));
    
    // Step 4: Build time-based indexes (simplified)
    const timeIndexResults = await buildTimeIndexes(redis, allPageviews, maxProcessingTime - (Date.now() - startTime));
    
    // Step 5: Store extraction metadata
    const metadata = {
      extraction_timestamp: new Date().toISOString(),
      total_pageviews: allPageviews.length,
      ip_indexes_created: ipIndexResults.created,
      time_indexes_created: timeIndexResults.created,
      processing_time_ms: Date.now() - startTime,
      coverage_start: allPageviews[0]?.timestamp,
      coverage_end: allPageviews[allPageviews.length - 1]?.timestamp,
      extraction_method: 'built_from_chunks',
      chunks_processed: allChunks.length
    };
    
    await storeExtractionMetadata(redis, metadata);
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ Index building complete in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        index_summary: {
          total_pageviews_indexed: allPageviews.length,
          ip_indexes_created: ipIndexResults.created,
          time_indexes_created: timeIndexResults.created,
          chunks_processed: allChunks.length,
          processing_time_ms: totalTime
        },
        coverage: {
          start_date: allPageviews[0]?.timestamp,
          end_date: allPageviews[allPageviews.length - 1]?.timestamp,
          unique_ips: ipIndexResults.unique_ips,
          date_range_days: Math.round((new Date(allPageviews[allPageviews.length - 1]?.timestamp) - new Date(allPageviews[0]?.timestamp)) / (1000 * 60 * 60 * 24))
        },
        next_steps: [
          'Test query system with: curl /query-pageviews',
          'Test staged recovery V2 with real conversion data'
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Index building failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Index building failed', 
        message: error.message 
      })
    };
  }
};

// Find all pageview chunks in Redis
async function findAllPageviewChunks(redis) {
  console.log('🔍 Scanning for pageview chunks...');
  
  const chunks = [];
  let cursor = '0';
  let iterations = 0;
  const maxIterations = 10;
  
  do {
    try {
      const scanResult = await redis(`scan/${cursor}/match/pageview_chunk:*/count/100`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      
      for (const key of keys) {
        chunks.push(key);
      }
      
      iterations++;
      
    } catch (scanError) {
      console.log(`⚠️ Chunk scan error: ${scanError.message}`);
      break;
    }
    
  } while (cursor !== '0' && iterations < maxIterations);
  
  console.log(`📦 Found ${chunks.length} chunk keys`);
  return chunks;
}

// Load pageviews from all chunks
async function loadPageviewsFromChunks(redis, chunkKeys, maxTime) {
  const loadStartTime = Date.now();
  const allPageviews = [];
  
  console.log(`📊 Loading pageviews from ${chunkKeys.length} chunks...`);
  
  for (let i = 0; i < chunkKeys.length; i++) {
    if (Date.now() - loadStartTime > maxTime - 2000) {
      console.log(`⏰ Time limit reached while loading chunks`);
      break;
    }
    
    try {
      const chunkKey = chunkKeys[i];
      const chunkData = await redis(`get/${chunkKey}`);
      
      if (chunkData?.result) {
        const chunk = JSON.parse(decodeURIComponent(chunkData.result));
        
        if (chunk.pageviews && Array.isArray(chunk.pageviews)) {
          allPageviews.push(...chunk.pageviews);
          
          if (i % 5 === 0) {
            console.log(`📊 Loaded ${allPageviews.length} pageviews from ${i + 1}/${chunkKeys.length} chunks`);
          }
        }
      }
      
    } catch (chunkError) {
      console.log(`⚠️ Error loading chunk ${i}: ${chunkError.message}`);
    }
  }
  
  // Sort by timestamp for consistent processing
  allPageviews.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  
  console.log(`✅ Loaded ${allPageviews.length} total pageviews`);
  return allPageviews;
}

// Build IP-based indexes
async function buildIPIndexes(redis, pageviews, maxTime) {
  const indexStartTime = Date.now();
  console.log(`🌐 Building IP indexes for ${pageviews.length} pageviews...`);
  
  const ipGroups = {};
  let uniqueIPs = 0;
  
  // Group pageviews by IP
  for (const pageview of pageviews) {
    if (Date.now() - indexStartTime > maxTime - 3000) break;
    
    const ip = pageview.ip_address;
    if (!ip || ip === 'unknown') continue;
    
    const encodedIP = ip.replace(/:/g, '_');
    if (!ipGroups[encodedIP]) {
      ipGroups[encodedIP] = [];
      uniqueIPs++;
    }
    
    ipGroups[encodedIP].push(pageview);
  }
  
  console.log(`📊 Grouped into ${uniqueIPs} unique IP addresses`);
  
  // Store IP indexes (limit for performance)
  let created = 0;
  const ipEntries = Object.entries(ipGroups).slice(0, 2000); // Limit to 2000 IPs for speed
  
  for (const [encodedIP, ipPageviews] of ipEntries) {
    if (Date.now() - indexStartTime > maxTime - 1000) {
      console.log(`⏰ Time limit reached for IP indexing`);
      break;
    }
    
    try {
      // Sort by timestamp (most recent first)
      ipPageviews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
      
      const ipKey = `pageview_index_ip:${encodedIP}`;
      const indexData = {
        ip_address: ipPageviews[0].ip_address,
        pageview_count: ipPageviews.length,
        latest_timestamp: ipPageviews[0].timestamp,
        earliest_timestamp: ipPageviews[ipPageviews.length - 1].timestamp,
        pageviews: ipPageviews.slice(0, 30), // Store up to 30 most recent pageviews
        created_at: new Date().toISOString()
      };
      
      await redis(`setex/${ipKey}/7200/${encodeURIComponent(JSON.stringify(indexData))}`); // 2 hours TTL
      created++;
      
      if (created % 100 === 0) {
        console.log(`📊 IP indexing progress: ${created}/${ipEntries.length} indexes created`);
      }
      
    } catch (ipError) {
      console.log(`⚠️ Error creating IP index for ${encodedIP}: ${ipError.message}`);
    }
  }
  
  console.log(`✅ IP indexes: ${created} created from ${uniqueIPs} unique IPs`);
  
  return {
    created: created,
    unique_ips: uniqueIPs,
    processing_time_ms: Date.now() - indexStartTime
  };
}

// Build simplified time-based indexes
async function buildTimeIndexes(redis, pageviews, maxTime) {
  const indexStartTime = Date.now();
  console.log(`🕐 Building time indexes...`);
  
  // Group by day for simplified time indexing
  const dayGroups = {};
  
  for (const pageview of pageviews) {
    if (Date.now() - indexStartTime > maxTime - 1000) break;
    
    const timestamp = new Date(pageview.timestamp);
    const dayKey = `${timestamp.getFullYear()}-${String(timestamp.getMonth() + 1).padStart(2, '0')}-${String(timestamp.getDate()).padStart(2, '0')}`;
    
    if (!dayGroups[dayKey]) {
      dayGroups[dayKey] = [];
    }
    
    dayGroups[dayKey].push(pageview);
  }
  
  // Store day indexes (simplified)
  let created = 0;
  const dayEntries = Object.entries(dayGroups);
  
  for (const [dayKey, dayPageviews] of dayEntries) {
    if (Date.now() - indexStartTime > maxTime - 500) break;
    
    try {
      const indexKey = `pageview_index_day:${dayKey}`;
      const indexData = {
        day_key: dayKey,
        pageview_count: dayPageviews.length,
        created_at: new Date().toISOString()
      };
      
      await redis(`setex/${indexKey}/3600/${encodeURIComponent(JSON.stringify(indexData))}`); // 1 hour TTL
      created++;
      
    } catch (dayError) {
      console.log(`⚠️ Error creating day index for ${dayKey}: ${dayError.message}`);
    }
  }
  
  console.log(`✅ Time indexes: ${created} day indexes created`);
  
  return {
    created: created,
    days_indexed: Object.keys(dayGroups).length,
    processing_time_ms: Date.now() - indexStartTime
  };
}

// Store extraction metadata
async function storeExtractionMetadata(redis, metadata) {
  const metadataKey = 'pageview_extraction_metadata';
  await redis(`setex/${metadataKey}/3600/${encodeURIComponent(JSON.stringify(metadata))}`); // 1 hour TTL
  console.log('📋 Extraction metadata stored');
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
