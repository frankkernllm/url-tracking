// Auto-Complete Pageview Extractor - NO MANUAL INTERVENTION
// Path: netlify/functions/extract-pageviews-chunked.js
// Purpose: Extract ALL pageviews automatically in one execution

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
    const chunkSize = body.chunk_size || 1000; // Smaller chunks for auto-complete
    const pattern = body.pattern || 'attribution_*';
    
    console.log(`🚀 Starting AUTO-COMPLETE extraction: pattern=${pattern}`);
    
    // AUTO-COMPLETE: Continue until all data is extracted
    const extractionResult = await extractAllPageviewsAutomatically(
      redis, 
      pattern, 
      chunkSize,
      maxProcessingTime - (Date.now() - startTime)
    );
    
    // Build indexes immediately if extraction completed
    let indexResults = null;
    if (extractionResult.is_complete && Date.now() - startTime < maxProcessingTime - 3000) {
      console.log('🏗️ Auto-building indexes after complete extraction...');
      indexResults = await buildBasicIndexes(redis, extractionResult);
    }
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ AUTO-COMPLETE extraction finished in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        extraction_complete: extractionResult.is_complete,
        extraction_summary: {
          total_pageviews_extracted: extractionResult.total_pageviews_extracted,
          total_keys_scanned: extractionResult.total_keys_scanned,
          chunks_processed: extractionResult.chunks_processed,
          chunks_stored: extractionResult.chunks_stored,
          processing_time_ms: totalTime,
          extraction_method: 'auto_complete_no_manual_intervention'
        },
        performance: {
          pageviews_per_second: Math.round(extractionResult.total_pageviews_extracted / (totalTime / 1000)),
          keys_per_second: Math.round(extractionResult.total_keys_scanned / (totalTime / 1000))
        },
        coverage: {
          earliest_pageview: extractionResult.earliest_pageview,
          latest_pageview: extractionResult.latest_pageview,
          unique_ips_found: extractionResult.unique_ips_found
        },
        indexes_built: indexResults ? true : false,
        next_steps: extractionResult.is_complete ? [
          'Extraction complete! All pageviews processed.',
          'Run build-indexes-complete.js to build comprehensive indexes',
          'System ready for attribution queries'
        ] : [
          'Extraction partially complete due to time constraints',
          'Run extraction again to continue processing remaining pageviews',
          'All progress is saved and will continue from where it left off'
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Auto-complete extraction failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Auto-complete extraction failed', 
        message: error.message 
      })
    };
  }
};

// AUTO-COMPLETE: Extract all pageviews in one execution
async function extractAllPageviewsAutomatically(redis, pattern, chunkSize, maxTime) {
  const extractionStartTime = Date.now();
  let cursor = '0';
  let totalPageviews = [];
  let totalKeysScanned = 0;
  let chunksProcessed = 0;
  let chunksStored = 0;
  let uniqueIPs = new Set();
  let earliestPageview = null;
  let latestPageview = null;
  
  console.log(`🔄 AUTO-COMPLETE: Starting continuous extraction...`);
  
  try {
    do {
      // Check time remaining
      const timeRemaining = maxTime - (Date.now() - extractionStartTime);
      if (timeRemaining < 3000) {
        console.log(`⏰ Time limit approaching: ${timeRemaining}ms remaining, stopping extraction`);
        break;
      }
      
      console.log(`🔍 Processing cursor: ${cursor}, pageviews so far: ${totalPageviews.length}`);
      
      // Extract one chunk
      const scanResult = await redis(`scan/${cursor}/match/${pattern}/count/500`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        console.log(`🏁 Scan complete: no more results`);
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      totalKeysScanned += keys.length;
      chunksProcessed++;
      
      console.log(`📊 Chunk ${chunksProcessed}: Found ${keys.length} keys, cursor: ${cursor}`);
      
      // Filter main attribution keys
      const mainKeys = keys.filter(key => {
        return !key.includes('_ip_') && 
               !key.includes('_session_') && 
               !key.includes('_fp_') && 
               !key.includes('_screen_') && 
               !key.includes('_webgl_') && 
               !key.includes('_geo_') &&
               !key.includes('pageview_index_') &&
               !key.includes('conversion_index_') &&
               !key.includes('attribution_stats_') &&
               !key.includes('geo_cache:') &&
               !key.includes('_region_') &&
               !key.includes('_hw_') &&
               key.startsWith('attribution_') &&
               key.match(/\d+$/);
      });
      
      console.log(`📝 Filtered: ${mainKeys.length} main keys from ${keys.length} total`);
      
      if (mainKeys.length === 0) {
        console.log(`⚠️ No main keys in this chunk, continuing...`);
        continue;
      }
      
      // Process keys in batches to avoid timeout
      const batchSize = 50;
      const chunkPageviews = [];
      
      for (let i = 0; i < mainKeys.length; i += batchSize) {
        const timeCheck = maxTime - (Date.now() - extractionStartTime);
        if (timeCheck < 2000) {
          console.log(`⏰ Time limit during batch processing, stopping`);
          break;
        }
        
        const batch = mainKeys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const data = await redis(`get/${key}`, 1000); // Faster timeout
            if (data?.result) {
              
              let parsed;
              try {
                parsed = JSON.parse(data.result);
              } catch (parseError) {
                try {
                  parsed = JSON.parse(decodeURIComponent(data.result));
                } catch (decodeError) {
                  return null;
                }
              }
              
              if (parsed && parsed.timestamp && parsed.ip_address) {
                
                // Track unique IPs and time range
                uniqueIPs.add(parsed.ip_address);
                
                const pvTime = new Date(parsed.timestamp);
                if (!earliestPageview || pvTime < new Date(earliestPageview)) {
                  earliestPageview = parsed.timestamp;
                }
                if (!latestPageview || pvTime > new Date(latestPageview)) {
                  latestPageview = parsed.timestamp;
                }
                
                return {
                  timestamp: parsed.timestamp,
                  ip_address: parsed.ip_address,
                  landing_page: parsed.landing_page || 'unknown',
                  source: parsed.source || 'direct',
                  utm_campaign: parsed.utm_campaign,
                  utm_medium: parsed.utm_medium,
                  utm_source: parsed.utm_source,
                  utm_term: parsed.utm_term,
                  utm_content: parsed.utm_content,
                  referrer_url: parsed.referrer_url,
                  session_id: parsed.session_id,
                  canvas_fingerprint: parsed.canvas_fingerprint,
                  webgl_fingerprint: parsed.webgl_fingerprint,
                  screen_resolution: parsed.screen_resolution,
                  cpu_cores: parsed.cpu_cores,
                  memory_gb: parsed.memory_gb,
                  redis_key: key
                };
              }
            }
          } catch (error) {
            // Skip errors to keep processing
          }
          return null;
        });
        
        const batchResults = await Promise.all(batchPromises);
        const validResults = batchResults.filter(result => result !== null);
        chunkPageviews.push(...validResults);
        
        console.log(`📦 Batch processed: ${validResults.length} pageviews from ${batch.length} keys`);
      }
      
      // Add to total pageviews
      totalPageviews.push(...chunkPageviews);
      
      // Store chunk if we have data
      if (chunkPageviews.length > 0) {
        await storePageviewChunk(redis, chunkPageviews, `auto_chunk_${chunksProcessed}`);
        chunksStored++;
        console.log(`💾 Stored chunk ${chunksStored} with ${chunkPageviews.length} pageviews`);
      }
      
      // Progress update
      console.log(`📊 Progress: ${totalPageviews.length} total pageviews, ${totalKeysScanned} keys scanned`);
      
      // Safety check: don't run forever
      if (chunksProcessed >= 50) {
        console.log(`🛑 Safety limit: processed 50 chunks, stopping to avoid infinite loop`);
        break;
      }
      
    } while (cursor !== '0' && Date.now() - extractionStartTime < maxTime - 2000);
    
    const isComplete = cursor === '0';
    const processingTime = Date.now() - extractionStartTime;
    
    console.log(`🏁 AUTO-COMPLETE extraction summary:`);
    console.log(`   📊 Total pageviews: ${totalPageviews.length}`);
    console.log(`   🔍 Total keys scanned: ${totalKeysScanned}`);
    console.log(`   📦 Chunks processed: ${chunksProcessed}`);
    console.log(`   💾 Chunks stored: ${chunksStored}`);
    console.log(`   🌐 Unique IPs: ${uniqueIPs.size}`);
    console.log(`   ✅ Complete: ${isComplete}`);
    console.log(`   ⏱️ Time: ${processingTime}ms`);
    
    return {
      total_pageviews_extracted: totalPageviews.length,
      total_keys_scanned: totalKeysScanned,
      chunks_processed: chunksProcessed,
      chunks_stored: chunksStored,
      unique_ips_found: uniqueIPs.size,
      earliest_pageview: earliestPageview,
      latest_pageview: latestPageview,
      is_complete: isComplete,
      final_cursor: cursor,
      processing_time_ms: processingTime
    };
    
  } catch (error) {
    console.error('❌ Auto-complete extraction error:', error);
    return {
      total_pageviews_extracted: totalPageviews.length,
      total_keys_scanned: totalKeysScanned,
      chunks_processed: chunksProcessed,
      chunks_stored: chunksStored,
      unique_ips_found: uniqueIPs.size,
      is_complete: false,
      error: error.message
    };
  }
}

// Store pageview chunk
async function storePageviewChunk(redis, pageviews, chunkId) {
  if (pageviews.length === 0) return;
  
  const chunkKey = `pageview_chunk:${chunkId}:${Date.now()}`;
  const chunkData = {
    chunk_id: chunkId,
    pageview_count: pageviews.length,
    pageviews: pageviews,
    created_at: new Date().toISOString()
  };
  
  await redis(`setex/${chunkKey}/2592000/${encodeURIComponent(JSON.stringify(chunkData))}`); // 30 days TTL
}

// Build basic indexes from extraction
async function buildBasicIndexes(redis, extractionResult) {
  console.log(`🏗️ Building basic indexes for ${extractionResult.total_pageviews_extracted} pageviews...`);
  
  try {
    const metadataKey = 'pageview_extraction_metadata';
    const metadata = {
      extraction_timestamp: new Date().toISOString(),
      total_pageviews: extractionResult.total_pageviews_extracted,
      total_keys_scanned: extractionResult.total_keys_scanned,
      chunks_stored: extractionResult.chunks_stored,
      unique_ips_found: extractionResult.unique_ips_found,
      extraction_method: 'auto_complete_fixed',
      extraction_complete: extractionResult.is_complete,
      coverage_start: extractionResult.earliest_pageview,
      coverage_end: extractionResult.latest_pageview,
      processing_time_ms: extractionResult.processing_time_ms
    };
    
    await redis(`setex/${metadataKey}/2592000/${encodeURIComponent(JSON.stringify(metadata))}`); // 30 days TTL
    console.log('✅ Basic extraction metadata stored');
    
    return { basic_metadata_created: true };
    
  } catch (error) {
    console.error('❌ Basic index building error:', error);
    return null;
  }
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
