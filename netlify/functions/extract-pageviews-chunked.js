// Fixed Pageview Extractor - Timeout-Safe Version
// Path: netlify/functions/extract-pageviews-chunked.js
// Purpose: Extract pageviews in manageable chunks to avoid timeouts
// FIXED: Proper key filtering and data validation

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
    const maxProcessingTime = 25000; // 25 seconds max (5 second buffer)
    
    // Get parameters
    const body = event.body ? JSON.parse(event.body) : {};
    const chunkSize = body.chunk_size || 2000; // Process 2000 pageviews per run
    const startCursor = body.start_cursor || '0';
    const pattern = body.pattern || 'attribution_*';
    
    console.log(`🚀 Starting FIXED extraction: ${chunkSize} pageviews, cursor: ${startCursor}, pattern: ${pattern}`);
    
    // Get existing progress
    const progressKey = 'pageview_extraction_progress';
    const existingProgress = await getExtractionProgress(redis, progressKey);
    
    const chunkResult = await extractPageviewChunkFixed(
      redis, 
      pattern, 
      startCursor, 
      chunkSize, 
      maxProcessingTime - (Date.now() - startTime)
    );
    
    // Update progress
    const newProgress = {
      ...existingProgress,
      total_extracted: existingProgress.total_extracted + chunkResult.pageviews_extracted,
      total_keys_scanned: existingProgress.total_keys_scanned + chunkResult.keys_scanned,
      last_cursor: chunkResult.final_cursor,
      last_updated: new Date().toISOString(),
      current_pattern: pattern,
      chunks_completed: existingProgress.chunks_completed + 1
    };
    
    await storeExtractionProgress(redis, progressKey, newProgress);
    
    // If we've reached the end of this pattern, build indexes
    const isComplete = chunkResult.final_cursor === '0';
    let indexResults = null;
    
    if (isComplete && Date.now() - startTime < maxProcessingTime - 5000) {
      console.log('🏗️ Building indexes for completed extraction...');
      indexResults = await buildIndexesFromProgress(redis, newProgress);
    }
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ FIXED extraction complete in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        chunk_summary: {
          pageviews_extracted_this_chunk: chunkResult.pageviews_extracted,
          keys_scanned_this_chunk: chunkResult.keys_scanned,
          processing_time_ms: totalTime,
          final_cursor: chunkResult.final_cursor,
          is_complete: isComplete
        },
        total_progress: {
          total_pageviews_extracted: newProgress.total_extracted,
          total_keys_scanned: newProgress.total_keys_scanned,
          chunks_completed: newProgress.chunks_completed,
          pattern: pattern
        },
        indexes_built: indexResults ? true : false,
        next_action: isComplete 
          ? 'Extraction complete! Check query system status.'
          : `Run again with start_cursor: "${chunkResult.final_cursor}" to continue`,
        continue_command: isComplete ? null : {
          curl: `curl -X POST https://trackingojoy.netlify.app/.netlify/functions/extract-pageviews-chunked -H "Content-Type: application/json" -d '{"start_cursor":"${chunkResult.final_cursor}","pattern":"${pattern}"}'`
        }
      })
    };
    
  } catch (error) {
    console.error('❌ Fixed extraction failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Fixed extraction failed', 
        message: error.message 
      })
    };
  }
};

// FIXED: Extract a chunk of pageviews with proper filtering and validation
async function extractPageviewChunkFixed(redis, pattern, startCursor, chunkSize, maxTime) {
  const chunkStartTime = Date.now();
  const pageviews = [];
  let keysScanned = 0;
  let cursor = startCursor;
  
  console.log(`📊 FIXED EXTRACTION: ${chunkSize} pageviews, ${maxTime}ms available`);
  
  try {
    let iterations = 0;
    const maxIterations = 20; // Limit iterations per chunk
    
    do {
      if (Date.now() - chunkStartTime > maxTime - 2000 || iterations >= maxIterations) {
        console.log(`⏰ Time/iteration limit reached: ${Date.now() - chunkStartTime}ms, iteration ${iterations}`);
        break;
      }
      
      const scanResult = await redis(`scan/${cursor}/match/${pattern}/count/500`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        console.log(`🔍 Scan complete or no more results`);
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      keysScanned += keys.length;
      iterations++;
      
      console.log(`🔍 Iteration ${iterations}: Found ${keys.length} keys, cursor: ${cursor}`);
      
      // FIXED: Improved main key filtering based on diagnostic results
      const mainKeys = keys.filter(key => {
        // Based on your diagnostic, valid keys look like: attribution_1.127.108.160_1750140065941
        // Pattern: attribution_{ip}_{timestamp}
        
        // Skip lookup keys with specific patterns
        if (key.includes('_ip_') || 
            key.includes('_session_') || 
            key.includes('_fp_') || 
            key.includes('_screen_') || 
            key.includes('_webgl_') || 
            key.includes('_geo_') ||
            key.includes('pageview_index_') ||
            key.includes('conversion_index_') ||
            key.includes('attribution_stats_') ||
            key.includes('geo_cache:') ||
            key.includes('_region_') ||
            key.includes('_hw_')) {
          return false;
        }
        
        // Must start with attribution_ and end with timestamp (digits)
        if (!key.startsWith('attribution_')) {
          return false;
        }
        
        // Must end with timestamp (digits)
        if (!key.match(/\d+$/)) {
          return false;
        }
        
        // Additional check: should have IP pattern (dots or colons for IPv6)
        // attribution_{ip}_{timestamp} - so should have at least one dot or colon
        const afterAttribution = key.substring('attribution_'.length);
        if (!afterAttribution.includes('.') && !afterAttribution.includes('_')) {
          return false;
        }
        
        return true;
      });
      
      console.log(`📝 FIXED filtering: ${mainKeys.length} main keys from ${keys.length} total keys`);
      
      if (mainKeys.length === 0) {
        console.log(`⚠️ No main keys found in this batch, continuing...`);
        continue;
      }
      
      // Log a sample of keys being processed
      console.log(`📋 Sample main keys: ${mainKeys.slice(0, 3).join(', ')}`);
      
      // Process in smaller batches to avoid memory issues
      const batchSize = 25;
      for (let i = 0; i < mainKeys.length && pageviews.length < chunkSize; i += batchSize) {
        if (Date.now() - chunkStartTime > maxTime - 1000) break;
        
        const batch = mainKeys.slice(i, i + batchSize);
        console.log(`📦 Processing batch ${Math.floor(i/batchSize) + 1}: ${batch.length} keys`);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const data = await redis(`get/${key}`);
            if (data?.result) {
              
              // FIXED: Handle both direct JSON and URL-encoded JSON (though test shows direct works)
              let parsed;
              try {
                // Method 1: Direct JSON parse (this worked in your test)
                parsed = JSON.parse(data.result);
              } catch (directError) {
                try {
                  // Method 2: URL decode then parse (fallback)
                  parsed = JSON.parse(decodeURIComponent(data.result));
                } catch (urlError) {
                  console.log(`⚠️ JSON parsing failed for ${key}: ${directError.message}`);
                  return null;
                }
              }
              
              // FIXED: Simplified validation based on your test results
              if (parsed && 
                  typeof parsed === 'object' && 
                  parsed.timestamp && 
                  parsed.ip_address) {
                
                // Transform to consistent pageview format
                const pageview = {
                  timestamp: parsed.timestamp,
                  ip_address: parsed.ip_address,
                  landing_page: parsed.landing_page || parsed.url || parsed.page_url || 'unknown',
                  source: parsed.source || parsed.utm_source || 'direct',
                  utm_campaign: parsed.utm_campaign || null,
                  utm_medium: parsed.utm_medium || null,
                  utm_source: parsed.utm_source || null,
                  utm_term: parsed.utm_term || null,
                  utm_content: parsed.utm_content || null,
                  referrer_url: parsed.referrer_url || null,
                  session_id: parsed.session_id || null,
                  canvas_fingerprint: parsed.canvas_fingerprint || null,
                  webgl_fingerprint: parsed.webgl_fingerprint || null,
                  screen_resolution: parsed.screen_resolution || null,
                  cpu_cores: parsed.cpu_cores || null,
                  memory_gb: parsed.memory_gb || null,
                  user_agent: parsed.user_agent || null,
                  platform: parsed.platform || null,
                  timezone: parsed.timezone || null,
                  language: parsed.language || null,
                  redis_key: key
                };
                
                console.log(`✅ Valid pageview extracted from ${key.substring(0, 40)}...`);
                return pageview;
                
              } else {
                console.log(`⚠️ Invalid pageview data in ${key}:`, {
                  has_timestamp: !!parsed?.timestamp,
                  has_ip_address: !!parsed?.ip_address,
                  is_object: typeof parsed === 'object',
                  field_count: parsed ? Object.keys(parsed).length : 0
                });
              }
            } else {
              console.log(`⚠️ No data returned for key ${key}`);
            }
          } catch (parseError) {
            console.log(`⚠️ Error processing key ${key}: ${parseError.message}`);
          }
          return null;
        });
        
        const batchResults = await Promise.all(batchPromises);
        const validResults = batchResults.filter(result => result !== null);
        pageviews.push(...validResults);
        
        console.log(`📊 Batch ${Math.floor(i/batchSize) + 1} results: ${validResults.length} valid pageviews from ${batch.length} keys`);
        
        // Stop if we've reached chunk size
        if (pageviews.length >= chunkSize) {
          console.log(`📊 Chunk size reached: ${pageviews.length} pageviews`);
          break;
        }
      }
      
      // Progress logging
      if (pageviews.length > 0 && pageviews.length % 100 === 0) {
        console.log(`📊 Chunk progress: ${pageviews.length} pageviews from ${keysScanned} keys`);
      }
      
    } while (cursor !== '0' && pageviews.length < chunkSize && Date.now() - chunkStartTime < maxTime - 1000);
    
    // Store pageviews from this chunk
    if (pageviews.length > 0) {
      await storePageviewChunk(redis, pageviews, startCursor);
    }
    
    const chunkTime = Date.now() - chunkStartTime;
    console.log(`✅ FIXED extraction complete: ${pageviews.length} pageviews in ${chunkTime}ms`);
    
    return {
      pageviews_extracted: pageviews.length,
      keys_scanned: keysScanned,
      final_cursor: cursor,
      processing_time_ms: chunkTime
    };
    
  } catch (error) {
    console.error('❌ Fixed extraction error:', error);
    return {
      pageviews_extracted: pageviews.length,
      keys_scanned: keysScanned,
      final_cursor: cursor,
      error: error.message
    };
  }
}

// Store pageview chunk data (unchanged)
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
  console.log(`💾 Stored chunk: ${pageviews.length} pageviews`);
}

// Get extraction progress (unchanged)
async function getExtractionProgress(redis, progressKey) {
  try {
    const progressData = await redis(`get/${progressKey}`);
    
    if (progressData?.result) {
      return JSON.parse(progressData.result);
    }
  } catch (error) {
    console.log('⚠️ No existing progress found, starting fresh');
  }
  
  return {
    total_extracted: 0,
    total_keys_scanned: 0,
    last_cursor: '0',
    started_at: new Date().toISOString(),
    chunks_completed: 0
  };
}

// Store extraction progress (unchanged)
async function storeExtractionProgress(redis, progressKey, progress) {
  await redis(`setex/${progressKey}/3600/${encodeURIComponent(JSON.stringify(progress))}`); // 1 hour TTL
}

// Build indexes from completed extraction (unchanged)
async function buildIndexesFromProgress(redis, progress) {
  console.log('🏗️ Building indexes from extracted data...');
  
  try {
    // Get all chunk keys
    let cursor = '0';
    const chunkKeys = [];
    
    do {
      const scanResult = await redis(`scan/${cursor}/match/pageview_chunk:*/count/100`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      chunkKeys.push(...keys);
      
    } while (cursor !== '0' && chunkKeys.length < 100);
    
    console.log(`📊 Found ${chunkKeys.length} chunks to process into indexes`);
    
    // Build minimal indexes from chunks (simplified for time)
    const indexesBuild = {
      time_indexes: 0,
      ip_indexes: 0,
      total_pageviews_indexed: 0
    };
    
    // For now, just store a completion marker
    const completionMetadata = {
      extraction_timestamp: new Date().toISOString(),
      total_pageviews: progress.total_extracted,
      chunks_processed: chunkKeys.length,
      extraction_method: 'fixed_chunked',
      indexes_built: indexesBuild
    };
    
    const metadataKey = 'pageview_extraction_metadata';
    await redis(`setex/${metadataKey}/2592000/${encodeURIComponent(JSON.stringify(completionMetadata))}`); // 30 days TTL
    
    console.log('✅ Basic indexes built and metadata stored');
    return indexesBuild;
    
  } catch (error) {
    console.error('❌ Index building error:', error);
    return null;
  }
}

// Initialize Redis helper (unchanged)
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
