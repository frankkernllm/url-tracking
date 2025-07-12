// Smart Resume Auto-Complete Pageview Extractor
// Path: netlify/functions/extract-pageviews-chunked.js
// Purpose: Extract ALL pageviews with smart resume from last position

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
    
    console.log(`🚀 Starting SMART RESUME extraction: pattern=${pattern}`);
    
    // Load existing progress or start fresh
    const progressKey = 'smart_extraction_progress';
    const existingProgress = await getSmartProgress(redis, progressKey);
    
    console.log(`📊 Resuming from previous progress:`, {
      total_extracted: existingProgress.total_extracted,
      last_cursor: existingProgress.last_cursor,
      chunks_completed: existingProgress.chunks_completed
    });
    
    // SMART RESUME: Continue from last cursor position
    const extractionResult = await extractWithSmartResume(
      redis, 
      pattern, 
      existingProgress,
      maxProcessingTime - (Date.now() - startTime)
    );
    
    // Update progress after extraction
    const updatedProgress = {
      total_extracted: existingProgress.total_extracted + extractionResult.pageviews_extracted_this_run,
      total_keys_scanned: existingProgress.total_keys_scanned + extractionResult.keys_scanned_this_run,
      last_cursor: extractionResult.final_cursor,
      chunks_completed: existingProgress.chunks_completed + extractionResult.chunks_processed_this_run,
      chunks_stored: existingProgress.chunks_stored + extractionResult.chunks_stored_this_run,
      unique_ips_found: extractionResult.total_unique_ips,
      last_updated: new Date().toISOString(),
      is_complete: extractionResult.is_complete,
      earliest_pageview: extractionResult.earliest_pageview || existingProgress.earliest_pageview,
      latest_pageview: extractionResult.latest_pageview || existingProgress.latest_pageview
    };
    
    await storeSmartProgress(redis, progressKey, updatedProgress);
    
    // Build indexes if complete
    let indexResults = null;
    if (extractionResult.is_complete && Date.now() - startTime < maxProcessingTime - 3000) {
      console.log('🏗️ Building indexes after complete extraction...');
      indexResults = await buildExtractionMetadata(redis, updatedProgress);
    }
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ SMART RESUME extraction finished in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        extraction_complete: extractionResult.is_complete,
        extraction_summary: {
          // This run stats
          pageviews_extracted_this_run: extractionResult.pageviews_extracted_this_run,
          keys_scanned_this_run: extractionResult.keys_scanned_this_run,
          chunks_processed_this_run: extractionResult.chunks_processed_this_run,
          processing_time_ms: totalTime,
          
          // Total stats across all runs
          total_pageviews_extracted: updatedProgress.total_extracted,
          total_keys_scanned: updatedProgress.total_keys_scanned,
          total_chunks_completed: updatedProgress.chunks_completed,
          extraction_method: 'smart_resume_auto_complete'
        },
        performance: {
          pageviews_per_second_this_run: Math.round(extractionResult.pageviews_extracted_this_run / (totalTime / 1000)),
          total_pageviews_per_second: Math.round(updatedProgress.total_extracted / ((Date.now() - new Date(existingProgress.started_at).getTime()) / 1000))
        },
        coverage: {
          earliest_pageview: updatedProgress.earliest_pageview,
          latest_pageview: updatedProgress.latest_pageview,
          unique_ips_found: updatedProgress.unique_ips_found
        },
        resume_info: {
          started_from_cursor: existingProgress.last_cursor,
          final_cursor: extractionResult.final_cursor,
          can_continue: !extractionResult.is_complete
        },
        indexes_built: indexResults ? true : false,
        next_steps: extractionResult.is_complete ? [
          'Extraction complete! All pageviews processed.',
          'Run build-indexes-complete.js to build comprehensive indexes',
          'System ready for attribution queries'
        ] : [
          'Extraction continuing... Run the same command again to continue',
          'Progress is automatically saved and will resume from where it left off',
          `Next run will start from cursor: ${extractionResult.final_cursor}`
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Smart resume extraction failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Smart resume extraction failed', 
        message: error.message 
      })
    };
  }
};

// Get smart progress (resume from last position)
async function getSmartProgress(redis, progressKey) {
  try {
    const progressData = await redis(`get/${progressKey}`);
    
    if (progressData?.result) {
      const progress = JSON.parse(decodeURIComponent(progressData.result));
      console.log(`🔄 Found existing progress: ${progress.total_extracted} pageviews extracted`);
      return progress;
    }
  } catch (error) {
    console.log('⚠️ No existing progress found, starting fresh');
  }
  
  // Default fresh start
  return {
    total_extracted: 0,
    total_keys_scanned: 0,
    last_cursor: '0',
    chunks_completed: 0,
    chunks_stored: 0,
    unique_ips_found: 0,
    started_at: new Date().toISOString(),
    is_complete: false,
    earliest_pageview: null,
    latest_pageview: null
  };
}

// Store smart progress
async function storeSmartProgress(redis, progressKey, progress) {
  await redis(`setex/${progressKey}/3600/${encodeURIComponent(JSON.stringify(progress))}`); // 1 hour TTL
  console.log(`💾 Progress saved: ${progress.total_extracted} total pageviews, cursor: ${progress.last_cursor}`);
}

// Extract with smart resume capability
async function extractWithSmartResume(redis, pattern, existingProgress, maxTime) {
  const extractionStartTime = Date.now();
  let cursor = existingProgress.last_cursor; // RESUME FROM LAST POSITION
  let thisRunPageviews = [];
  let thisRunKeysScanned = 0;
  let thisRunChunksProcessed = 0;
  let thisRunChunksStored = 0;
  let allUniqueIPs = new Set();
  let earliestPageview = existingProgress.earliest_pageview;
  let latestPageview = existingProgress.latest_pageview;
  
  console.log(`🔄 SMART RESUME: Starting from cursor: ${cursor}`);
  console.log(`📊 Previous progress: ${existingProgress.total_extracted} pageviews already extracted`);
  
  try {
    do {
      // Check time remaining
      const timeRemaining = maxTime - (Date.now() - extractionStartTime);
      if (timeRemaining < 3000) {
        console.log(`⏰ Time limit approaching: ${timeRemaining}ms remaining, stopping extraction`);
        break;
      }
      
      console.log(`🔍 Processing cursor: ${cursor}, this run: ${thisRunPageviews.length} pageviews`);
      
      // Scan for next batch
      const scanResult = await redis(`scan/${cursor}/match/${pattern}/count/500`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        console.log(`🏁 Scan complete: no more results`);
        cursor = '0'; // Mark as complete
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      thisRunKeysScanned += keys.length;
      thisRunChunksProcessed++;
      
      console.log(`📊 Chunk ${thisRunChunksProcessed}: Found ${keys.length} keys, cursor: ${cursor}`);
      
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
      
      // Process keys in batches
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
            const data = await redis(`get/${key}`, 1000);
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
                allUniqueIPs.add(parsed.ip_address);
                
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
      
      // Add to this run's pageviews
      thisRunPageviews.push(...chunkPageviews);
      
      // Store chunk if we have data
      if (chunkPageviews.length > 0) {
        const chunkId = `smart_chunk_${existingProgress.chunks_completed + thisRunChunksStored + 1}`;
        await storePageviewChunk(redis, chunkPageviews, chunkId);
        thisRunChunksStored++;
        console.log(`💾 Stored chunk ${chunkId} with ${chunkPageviews.length} pageviews`);
      }
      
      // Progress update
      const totalSoFar = existingProgress.total_extracted + thisRunPageviews.length;
      console.log(`📊 Progress: ${thisRunPageviews.length} this run, ${totalSoFar} total pageviews`);
      
      // Safety check: don't run forever
      if (thisRunChunksProcessed >= 50) {
        console.log(`🛑 Safety limit: processed 50 chunks this run, stopping to avoid infinite loop`);
        break;
      }
      
    } while (cursor !== '0' && Date.now() - extractionStartTime < maxTime - 2000);
    
    const isComplete = cursor === '0';
    const processingTime = Date.now() - extractionStartTime;
    
    console.log(`🏁 SMART RESUME extraction summary for this run:`);
    console.log(`   📊 This run pageviews: ${thisRunPageviews.length}`);
    console.log(`   📊 Total pageviews: ${existingProgress.total_extracted + thisRunPageviews.length}`);
    console.log(`   🔍 This run keys scanned: ${thisRunKeysScanned}`);
    console.log(`   📦 This run chunks: ${thisRunChunksProcessed}`);
    console.log(`   💾 This run chunks stored: ${thisRunChunksStored}`);
    console.log(`   🌐 Total unique IPs: ${allUniqueIPs.size}`);
    console.log(`   ✅ Complete: ${isComplete}`);
    console.log(`   🎯 Final cursor: ${cursor}`);
    console.log(`   ⏱️ This run time: ${processingTime}ms`);
    
    return {
      pageviews_extracted_this_run: thisRunPageviews.length,
      keys_scanned_this_run: thisRunKeysScanned,
      chunks_processed_this_run: thisRunChunksProcessed,
      chunks_stored_this_run: thisRunChunksStored,
      total_unique_ips: allUniqueIPs.size,
      earliest_pageview: earliestPageview,
      latest_pageview: latestPageview,
      is_complete: isComplete,
      final_cursor: cursor,
      processing_time_ms: processingTime
    };
    
  } catch (error) {
    console.error('❌ Smart resume extraction error:', error);
    return {
      pageviews_extracted_this_run: thisRunPageviews.length,
      keys_scanned_this_run: thisRunKeysScanned,
      chunks_processed_this_run: thisRunChunksProcessed,
      chunks_stored_this_run: thisRunChunksStored,
      total_unique_ips: allUniqueIPs.size,
      is_complete: false,
      final_cursor: cursor,
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

// Build extraction metadata
async function buildExtractionMetadata(redis, progress) {
  console.log(`🏗️ Building extraction metadata for ${progress.total_extracted} pageviews...`);
  
  try {
    const metadataKey = 'pageview_extraction_metadata';
    const metadata = {
      extraction_timestamp: new Date().toISOString(),
      total_pageviews: progress.total_extracted,
      total_keys_scanned: progress.total_keys_scanned,
      chunks_stored: progress.chunks_stored,
      unique_ips_found: progress.unique_ips_found,
      extraction_method: 'smart_resume_complete',
      extraction_complete: progress.is_complete,
      coverage_start: progress.earliest_pageview,
      coverage_end: progress.latest_pageview,
      started_at: progress.started_at,
      completed_at: new Date().toISOString()
    };
    
    await redis(`setex/${metadataKey}/2592000/${encodeURIComponent(JSON.stringify(metadata))}`); // 30 days TTL
    console.log('✅ Extraction metadata stored');
    
    return { metadata_created: true };
    
  } catch (error) {
    console.error('❌ Metadata building error:', error);
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
