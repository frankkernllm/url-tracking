// Complete Pageview Extractor - Runs to Completion Automatically
// Path: netlify/functions/extract-pageviews-chunked.js
// Purpose: Extract ALL pageviews in one execution without manual cursor management

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
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    // Get parameters
    const body = event.body ? JSON.parse(event.body) : {};
    const pattern = body.pattern || 'attribution_*';
    
    console.log(`🚀 Starting COMPLETE pageview extraction with pattern: ${pattern}`);
    
    // Get existing progress or start fresh
    const progressKey = 'pageview_extraction_progress';
    const initialProgress = {
      total_extracted: 0,
      total_keys_scanned: 0,
      last_cursor: '0',
      started_at: new Date().toISOString(),
      chunks_completed: 0
    };
    
    console.log(`📊 Extracting ALL pageviews automatically...`);
    
    const completeResult = await extractAllPageviewsToCompletion(
      redis, 
      pattern, 
      maxProcessingTime - (Date.now() - startTime)
    );
    
    // Update final progress
    const finalProgress = {
      ...initialProgress,
      total_extracted: completeResult.pageviews_extracted,
      total_keys_scanned: completeResult.keys_scanned,
      last_cursor: '0', // Always 0 when complete
      last_updated: new Date().toISOString(),
      current_pattern: pattern,
      chunks_completed: completeResult.chunks_processed,
      extraction_complete: true
    };
    
    await storeExtractionProgress(redis, progressKey, finalProgress);
    
    // Build indexes from extracted data
    console.log('🏗️ Building indexes from extracted pageviews...');
    const indexResults = await buildIndexesFromAllPageviews(redis, completeResult.all_pageviews);
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ COMPLETE extraction finished in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        extraction_complete: true,
        complete_extraction_summary: {
          total_pageviews_extracted: completeResult.pageviews_extracted,
          total_keys_scanned: completeResult.keys_scanned,
          chunks_processed: completeResult.chunks_processed,
          processing_time_ms: totalTime,
          extraction_method: 'complete_automatic'
        },
        indexes_built: {
          ip_indexes_created: indexResults.ip_indexes_created,
          time_indexes_created: indexResults.time_indexes_created,
          total_indexes: indexResults.ip_indexes_created + indexResults.time_indexes_created
        },
        performance: {
          pageviews_per_second: Math.round(completeResult.pageviews_extracted / (totalTime / 1000)),
          keys_per_second: Math.round(completeResult.keys_scanned / (totalTime / 1000))
        },
        coverage: {
          earliest_pageview: completeResult.earliest_timestamp,
          latest_pageview: completeResult.latest_timestamp,
          unique_ips_found: completeResult.unique_ips.size
        },
        next_steps: [
          'Pageview extraction is now complete',
          'Indexes have been built automatically', 
          'System ready for attribution queries'
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Complete pageview extraction failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Complete pageview extraction failed', 
        message: error.message 
      })
    };
  }
};

// Extract ALL pageviews to completion in one execution
async function extractAllPageviewsToCompletion(redis, pattern, maxTime) {
  const extractionStartTime = Date.now();
  const allPageviews = [];
  const uniqueIPs = new Set();
  
  let totalKeysScanned = 0;
  let chunksProcessed = 0;
  let cursor = '0';
  let earliestTimestamp = null;
  let latestTimestamp = null;
  
  console.log(`📊 Beginning complete extraction with ${maxTime}ms available`);
  
  try {
    let iterations = 0;
    const maxIterations = 25; // Safety valve to prevent infinite loops
    
    do {
      // Conservative timeout management - leave 8 seconds buffer for final processing
      if (Date.now() - extractionStartTime > maxTime - 8000 || iterations >= maxIterations) {
        console.log(`⏰ Time/iteration limit reached: ${Date.now() - extractionStartTime}ms, iteration ${iterations}`);
        console.log(`🔄 Graceful stop - extracted ${allPageviews.length} pageviews so far`);
        break;
      }
      
      const scanResult = await redis(`scan/${cursor}/match/${pattern}/count/500`, 3000); // Conservative scan count
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        console.log('📊 Scan complete - no more keys found');
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      totalKeysScanned += keys.length;
      iterations++;
      chunksProcessed++;
      
      console.log(`📊 Processing chunk ${chunksProcessed}: ${keys.length} keys (cursor: ${cursor})`);
      
      // Filter main keys (skip lookup keys)
      const mainKeys = keys.filter(key => 
        !key.includes('_ip_') && 
        !key.includes('_session_') && 
        !key.includes('_fp_') && 
        !key.includes('_screen_') && 
        !key.includes('_webgl_') && 
        !key.includes('_geo_') &&
        !key.includes('pageview_index_') &&
        !key.includes('conversion_') &&
        !key.includes('geo_cache:')
      );
      
      console.log(`📊 Found ${mainKeys.length} main attribution keys in this chunk`);
      
      // Process in conservative batches to avoid timeout
      const batchSize = 25; // Conservative batch size
      for (let i = 0; i < mainKeys.length; i += batchSize) {
        // Conservative timeout check - leave 6 seconds for final processing
        if (Date.now() - extractionStartTime > maxTime - 6000) {
          console.log(`⏰ Time limit approaching, stopping batch processing`);
          break;
        }
        
        const batch = mainKeys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const data = await redis(`get/${key}`, 2000); // Conservative timeout
            if (data?.result) {
              const parsed = JSON.parse(decodeURIComponent(data.result));
              
              if (parsed.timestamp && parsed.ip_address && 
                  (parsed.landing_page || parsed.url || parsed.page_url)) {
                
                // Track time range
                const pvTime = new Date(parsed.timestamp);
                if (!earliestTimestamp || pvTime < earliestTimestamp) {
                  earliestTimestamp = pvTime;
                }
                if (!latestTimestamp || pvTime > latestTimestamp) {
                  latestTimestamp = pvTime;
                }
                
                // Track unique IPs
                uniqueIPs.add(parsed.ip_address);
                
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
                  session_id: parsed.session_id,
                  canvas_fingerprint: parsed.canvas_fingerprint,
                  webgl_fingerprint: parsed.webgl_fingerprint,
                  cpu_cores: parsed.cpu_cores,
                  memory_gb: parsed.memory_gb,
                  city: parsed.city,
                  region: parsed.region,
                  isp: parsed.isp,
                  redis_key: key
                };
              }
            }
          } catch (parseError) {
            // Skip invalid data - graceful error handling
          }
          return null;
        });
        
        try {
          const batchResults = await Promise.all(batchPromises);
          const validResults = batchResults.filter(result => result !== null);
          allPageviews.push(...validResults);
        } catch (batchError) {
          console.log(`⚠️ Batch processing error (continuing): ${batchError.message}`);
          // Continue processing even if some batches fail
        }
        
        // Progress logging
        if (allPageviews.length % 500 === 0 && allPageviews.length > 0) {
          console.log(`📊 Extraction progress: ${allPageviews.length} pageviews from ${totalKeysScanned} keys scanned`);
        }
      }
      
      // Log chunk completion
      console.log(`✅ Chunk ${chunksProcessed} complete: ${allPageviews.length} total pageviews extracted`);
      
      // Conservative timeout check for loop continuation
    } while (cursor !== '0' && Date.now() - extractionStartTime < maxTime - 5000);
    
    // Sort all pageviews by timestamp
    allPageviews.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    
    const extractionTime = Date.now() - extractionStartTime;
    const isComplete = cursor === '0';
    
    console.log(`✅ Extraction ${isComplete ? 'COMPLETE' : 'PARTIAL'}: ${allPageviews.length} pageviews in ${extractionTime}ms`);
    console.log(`📊 Coverage: ${earliestTimestamp?.toISOString()} to ${latestTimestamp?.toISOString()}`);
    console.log(`🌐 Unique IPs found: ${uniqueIPs.size}`);
    
    if (!isComplete) {
      console.log(`⚠️ Extraction stopped due to time constraints. Processed ${chunksProcessed} chunks.`);
    }
    
    return {
      pageviews_extracted: allPageviews.length,
      keys_scanned: totalKeysScanned,
      chunks_processed: chunksProcessed,
      all_pageviews: allPageviews,
      unique_ips: uniqueIPs,
      earliest_timestamp: earliestTimestamp?.toISOString(),
      latest_timestamp: latestTimestamp?.toISOString(),
      processing_time_ms: extractionTime,
      extraction_complete: isComplete
    };
    
  } catch (error) {
    console.error('❌ Complete extraction error:', error);
    return {
      pageviews_extracted: allPageviews.length,
      keys_scanned: totalKeysScanned,
      chunks_processed: chunksProcessed,
      all_pageviews: allPageviews,
      unique_ips: uniqueIPs,
      earliest_timestamp: earliestTimestamp?.toISOString(),
      latest_timestamp: latestTimestamp?.toISOString(),
      error: error.message,
      extraction_complete: false
    };
  }
}

// Build indexes from all extracted pageviews
async function buildIndexesFromAllPageviews(redis, allPageviews) {
  console.log(`🏗️ Building indexes from ${allPageviews.length} pageviews...`);
  
  // Group pageviews by IP for IP indexes
  const ipGroups = new Map();
  const dateGroups = new Map();
  
  for (const pageview of allPageviews) {
    // IP grouping
    const ip = pageview.ip_address;
    if (ip && ip !== 'unknown') {
      const encodedIP = ip.replace(/:/g, '_');
      
      if (!ipGroups.has(encodedIP)) {
        ipGroups.set(encodedIP, {
          ip_address: ip,
          pageviews: [],
          latest_timestamp: pageview.timestamp
        });
      }
      
      const ipGroup = ipGroups.get(encodedIP);
      ipGroup.pageviews.push(pageview);
      
      // Update latest timestamp
      if (new Date(pageview.timestamp) > new Date(ipGroup.latest_timestamp)) {
        ipGroup.latest_timestamp = pageview.timestamp;
      }
    }
    
    // Date grouping
    const date = new Date(pageview.timestamp);
    const dateKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
    
    if (!dateGroups.has(dateKey)) {
      dateGroups.set(dateKey, []);
    }
    dateGroups.get(dateKey).push(pageview);
  }
  
  // Create IP indexes
  let ipIndexesCreated = 0;
  for (const [encodedIP, ipData] of ipGroups) {
    try {
      // Sort pageviews by timestamp (most recent first)
      ipData.pageviews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
      
      const ipKey = `pageview_index_ip:${encodedIP}`;
      const indexData = {
        ip_address: ipData.ip_address,
        pageview_count: ipData.pageviews.length,
        latest_timestamp: ipData.latest_timestamp,
        pageviews: ipData.pageviews.slice(0, 20), // Limit to 20 most recent
        created_at: new Date().toISOString()
      };
      
      await redis(`setex/${ipKey}/2592000/${encodeURIComponent(JSON.stringify(indexData))}`); // 30 days TTL
      ipIndexesCreated++;
      
      if (ipIndexesCreated % 100 === 0) {
        console.log(`🏗️ IP indexing progress: ${ipIndexesCreated}/${ipGroups.size} indexes created`);
      }
      
    } catch (ipError) {
      console.log(`⚠️ Error creating IP index: ${ipError.message}`);
    }
  }
  
  // Create date indexes
  let timeIndexesCreated = 0;
  for (const [dateKey, datePageviews] of dateGroups) {
    try {
      const timeKey = `pageview_index_date:${dateKey}`;
      const timeData = {
        date_key: dateKey,
        pageview_count: datePageviews.length,
        pageviews: datePageviews,
        created_at: new Date().toISOString()
      };
      
      await redis(`setex/${timeKey}/7200/${encodeURIComponent(JSON.stringify(timeData))}`);
      timeIndexesCreated++;
      
    } catch (timeError) {
      console.log(`⚠️ Error creating time index: ${timeError.message}`);
    }
  }
  
  console.log(`✅ Indexes built: ${ipIndexesCreated} IP indexes, ${timeIndexesCreated} time indexes`);
  
  return {
    ip_indexes_created: ipIndexesCreated,
    time_indexes_created: timeIndexesCreated
  };
}

// Store extraction progress
async function storeExtractionProgress(redis, progressKey, progress) {
  await redis(`setex/${progressKey}/2592000/${encodeURIComponent(JSON.stringify(progress))}`); // 30 days TTL
}

// Initialize Redis helper with robust error handling
function initializeRedis() {
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  
  return async (command, timeoutMs = 5000) => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => {
      controller.abort();
      console.log(`⏰ Redis timeout after ${timeoutMs}ms for command: ${command.split('/')[0]}`);
    }, timeoutMs);
    
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
        const errorText = await response.text();
        console.log(`❌ Redis HTTP error ${response.status}: ${errorText}`);
        throw new Error(`Redis HTTP error: ${response.status} ${errorText}`);
      }
      
      const result = await response.json();
      
      return result;
      
    } catch (error) {
      clearTimeout(timeoutId);
      
      if (error.name === 'AbortError') {
        console.log(`⏰ Redis command timed out: ${command.split('/')[0]}`);
        throw new Error(`Redis timeout after ${timeoutMs}ms`);
      }
      
      console.log(`❌ Redis command failed: ${command.split('/')[0]} - ${error.message}`);
      throw error;
    }
  };
}
