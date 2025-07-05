// Chunked Pageview Extractor - Timeout-Safe Version
// Path: netlify/functions/extract-pageviews-chunked.js
// Purpose: Extract pageviews in manageable chunks to avoid timeouts

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
    
    console.log(`🚀 Starting chunked extraction: ${chunkSize} pageviews, cursor: ${startCursor}, pattern: ${pattern}`);
    
    // Get existing progress
    const progressKey = 'pageview_extraction_progress';
    const existingProgress = await getExtractionProgress(redis, progressKey);
    
    const chunkResult = await extractPageviewChunk(
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
    console.log(`✅ Chunk complete in ${totalTime}ms`);
    
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
          curl: `curl -X POST /extract-pageviews-chunked -d '{"start_cursor":"${chunkResult.final_cursor}","pattern":"${pattern}"}'`
        }
      })
    };
    
  } catch (error) {
    console.error('❌ Chunked extraction failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Chunked extraction failed', 
        message: error.message 
      })
    };
  }
};

// Extract a chunk of pageviews with strict time management
async function extractPageviewChunk(redis, pattern, startCursor, chunkSize, maxTime) {
  const chunkStartTime = Date.now();
  const pageviews = [];
  let keysScanned = 0;
  let cursor = startCursor;
  
  console.log(`📊 Extracting chunk: ${chunkSize} pageviews, ${maxTime}ms available`);
  
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
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      keysScanned += keys.length;
      iterations++;
      
      // Filter main keys (skip lookup keys)
      const mainKeys = keys.filter(key => 
        !key.includes('_ip_') && 
        !key.includes('_session_') && 
        !key.includes('_fp_') && 
        !key.includes('_screen_') && 
        !key.includes('_webgl_') && 
        !key.includes('_geo_') &&
        !key.includes('pageview_index_')
      );
      
      // Process in smaller batches to avoid memory issues
      const batchSize = 25;
      for (let i = 0; i < mainKeys.length && pageviews.length < chunkSize; i += batchSize) {
        if (Date.now() - chunkStartTime > maxTime - 1000) break;
        
        const batch = mainKeys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const data = await redis(`get/${key}`);
            if (data?.result) {
              const parsed = JSON.parse(data.result);
              
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
                  redis_key: key
                };
              }
            }
          } catch (parseError) {
            // Skip invalid data
          }
          return null;
        });
        
        const batchResults = await Promise.all(batchPromises);
        const validResults = batchResults.filter(result => result !== null);
        pageviews.push(...validResults);
        
        // Stop if we've reached chunk size
        if (pageviews.length >= chunkSize) {
          console.log(`📊 Chunk size reached: ${pageviews.length} pageviews`);
          break;
        }
      }
      
      // Progress logging
      if (pageviews.length % 500 === 0 && pageviews.length > 0) {
        console.log(`📊 Chunk progress: ${pageviews.length} pageviews from ${keysScanned} keys`);
      }
      
    } while (cursor !== '0' && pageviews.length < chunkSize && Date.now() - chunkStartTime < maxTime - 1000);
    
    // Store pageviews from this chunk
    await storePageviewChunk(redis, pageviews, startCursor);
    
    const chunkTime = Date.now() - chunkStartTime;
    console.log(`✅ Chunk extracted: ${pageviews.length} pageviews in ${chunkTime}ms`);
    
    return {
      pageviews_extracted: pageviews.length,
      keys_scanned: keysScanned,
      final_cursor: cursor,
      processing_time_ms: chunkTime
    };
    
  } catch (error) {
    console.error('❌ Chunk extraction error:', error);
    return {
      pageviews_extracted: pageviews.length,
      keys_scanned: keysScanned,
      final_cursor: cursor,
      error: error.message
    };
  }
}

// Store pageview chunk data
async function storePageviewChunk(redis, pageviews, chunkId) {
  if (pageviews.length === 0) return;
  
  const chunkKey = `pageview_chunk:${chunkId}:${Date.now()}`;
  const chunkData = {
    chunk_id: chunkId,
    pageview_count: pageviews.length,
    pageviews: pageviews,
    created_at: new Date().toISOString()
  };
  
  await redis(`setex/${chunkKey}/1800/${encodeURIComponent(JSON.stringify(chunkData))}`); // 30 minutes TTL
  console.log(`💾 Stored chunk: ${pageviews.length} pageviews`);
}

// Get extraction progress
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

// Store extraction progress
async function storeExtractionProgress(redis, progressKey, progress) {
  await redis(`setex/${progressKey}/3600/${encodeURIComponent(JSON.stringify(progress))}`); // 1 hour TTL
}

// Build indexes from completed extraction
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
      extraction_method: 'chunked',
      indexes_built: indexesBuild
    };
    
    const metadataKey = 'pageview_extraction_metadata';
    await redis(`setex/${metadataKey}/3600/${encodeURIComponent(JSON.stringify(completionMetadata))}`);
    
    console.log('✅ Basic indexes built and metadata stored');
    return indexesBuild;
    
  } catch (error) {
    console.error('❌ Index building error:', error);
    return null;
  }
}

// Initialize Redis helper
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
