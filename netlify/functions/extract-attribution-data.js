// Simple Attribution Data Extractor - Based on Working extract-pageviews-chunked.js
// Path: netlify/functions/extract-attribution-simple.js  
// Purpose: Extract pageviews using the PROVEN simple method, output attribution format

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
    
    // Get parameters (using simple single pattern approach)
    const body = event.body ? JSON.parse(event.body) : {};
    const pattern = body.pattern || 'attribution_*';  // SINGLE PATTERN ONLY
    
    console.log(`🚀 Starting SIMPLE attribution extraction: pattern=${pattern}`);
    
    // Load existing progress or start fresh (simplified)
    const progressKey = 'simple_attribution_progress';
    const existingProgress = await getSimpleProgress(redis, progressKey);
    
    console.log(`📊 Resuming from simple progress:`, {
      total_extracted: existingProgress.total_extracted,
      last_cursor: existingProgress.last_cursor,
      chunks_completed: existingProgress.chunks_completed
    });
    
    // SIMPLE EXTRACTION: Use proven method from extract-pageviews-chunked.js
    const extractionResult = await extractWithSimpleMethod(
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
    
    await storeSimpleProgress(redis, progressKey, updatedProgress);
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ SIMPLE attribution extraction finished in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        extraction_complete: extractionResult.is_complete,
        attribution_extraction_summary: {
          // This run stats
          pageviews_extracted_this_run: extractionResult.pageviews_extracted_this_run,
          keys_scanned_this_run: extractionResult.keys_scanned_this_run,
          chunks_processed_this_run: extractionResult.chunks_processed_this_run,
          processing_time_ms: totalTime,
          
          // Total stats across all runs
          total_pageviews_extracted: updatedProgress.total_extracted,
          total_keys_scanned: updatedProgress.total_keys_scanned,
          total_chunks_completed: updatedProgress.chunks_completed,
          extraction_method: 'simple_attribution_v1'
        },
        performance: {
          pageviews_per_second_this_run: Math.round(extractionResult.pageviews_extracted_this_run / (totalTime / 1000)),
          unique_ips_found: updatedProgress.unique_ips_found
        },
        coverage: {
          earliest_pageview: updatedProgress.earliest_pageview,
          latest_pageview: updatedProgress.latest_pageview,
          attribution_fields_coverage: extractionResult.attribution_fields_found
        },
        next_steps: extractionResult.is_complete ? [
          '✅ Simple attribution extraction complete!',
          'All attribution_* pageviews processed successfully', 
          'Run build-attribution-indexes.js to build attribution indexes',
          'System ready for attribution analysis'
        ] : [
          'Simple extraction continuing...',
          'Run the same command again to continue processing',
          `Progress: ${updatedProgress.total_extracted} pageviews extracted`,
          `Next run will start from cursor: ${extractionResult.final_cursor}`
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Simple attribution extraction failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Simple attribution extraction failed', 
        message: error.message 
      })
    };
  }
};

// Get simple progress (resume from last position)
async function getSimpleProgress(redis, progressKey) {
  try {
    const progressData = await redis(`get/${progressKey}`);
    
    if (progressData?.result) {
      const progress = JSON.parse(decodeURIComponent(progressData.result));
      console.log(`🔄 Found existing simple progress: ${progress.total_extracted} pageviews extracted`);
      return progress;
    }
  } catch (error) {
    console.log('⚠️ No existing simple progress found, starting fresh');
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

// Store simple progress
async function storeSimpleProgress(redis, progressKey, progress) {
  await redis(`setex/${progressKey}/3600/${encodeURIComponent(JSON.stringify(progress))}`); // 1 hour TTL
  console.log(`💾 Simple progress saved: ${progress.total_extracted} total pageviews, cursor: ${progress.last_cursor}`);
}

// Extract with simple method (proven working approach from extract-pageviews-chunked.js)
async function extractWithSimpleMethod(redis, pattern, existingProgress, maxTime) {
  const extractionStartTime = Date.now();
  let cursor = existingProgress.last_cursor; // RESUME FROM LAST POSITION
  let thisRunPageviews = [];
  let thisRunKeysScanned = 0;
  let thisRunChunksProcessed = 0;
  let thisRunChunksStored = 0;
  let allUniqueIPs = new Set();
  let earliestPageview = existingProgress.earliest_pageview;
  let latestPageview = existingProgress.latest_pageview;
  let attributionFieldsFound = new Set();
  
  console.log(`🔄 SIMPLE RESUME: Starting from cursor: ${cursor}`);
  console.log(`📊 Previous progress: ${existingProgress.total_extracted} pageviews already extracted`);
  
  // 🎯🎯🎯 DEBUG: Track target IP findings
  let targetIPKeysFound = 0;
  let targetIPKeysAfterFilter = 0;
  let targetIPPageviewsExtracted = 0;
  
  try {
    do {
      // Check time remaining
      const timeRemaining = maxTime - (Date.now() - extractionStartTime);
      if (timeRemaining < 3000) {
        console.log(`⏰ Time limit approaching: ${timeRemaining}ms remaining, stopping extraction`);
        break;
      }
      
      console.log(`🔍 Processing cursor: ${cursor}, this run: ${thisRunPageviews.length} pageviews`);
      
      // Scan for next batch (EXACT same method as working script)
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
      
      // 🎯🎯🎯 DEBUG: Check for target IP keys
      const targetKeys = keys.filter(key => key.includes('42.61.210.120'));
      if (targetKeys.length > 0) {
        targetIPKeysFound += targetKeys.length;
        console.log(`🎯🎯🎯 FOUND TARGET IP KEYS IN SCAN (chunk ${thisRunChunksProcessed}):`, targetKeys);
        
        // Test specific keys we're looking for
        const specificTargetKeys = [
          'attribution_42.61.210.120_1753484654828',
          'attribution_42.61.210.120_1753484618503', 
          'attribution_42.61.210.120_1753463169445'
        ];
        
        specificTargetKeys.forEach(targetKey => {
          if (keys.includes(targetKey)) {
            console.log(`🎯🎯🎯 FOUND SPECIFIC TARGET KEY: ${targetKey}`);
          }
        });
      }
      
      // Filter main attribution keys (EXACT same filter as working script)
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
      
      // 🎯🎯🎯 DEBUG: Check if target keys passed filter
      if (targetKeys.length > 0) {
        const filteredTargetKeys = mainKeys.filter(key => key.includes('42.61.210.120'));
        targetIPKeysAfterFilter += filteredTargetKeys.length;
        console.log(`🎯🎯🎯 TARGET KEYS AFTER FILTER: ${filteredTargetKeys.length}/${targetKeys.length} passed`);
        if (filteredTargetKeys.length > 0) {
          console.log(`🎯🎯🎯 TARGET KEYS THAT PASSED:`, filteredTargetKeys);
        }
      }
      
      if (mainKeys.length === 0) {
        console.log(`⚠️ No main keys in this chunk, continuing...`);
        continue;
      }
      
      // Process keys in batches (EXACT same method as working script)
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
                
                // Track attribution fields
                Object.keys(parsed).forEach(field => attributionFieldsFound.add(field));
                
                // 🎯🎯🎯 DEBUG: Check if this is target IP pageview
                if (parsed.ip_address === '42.61.210.120') {
                  targetIPPageviewsExtracted++;
                  console.log(`🎯🎯🎯 EXTRACTED TARGET IP PAGEVIEW:`, {
                    timestamp: parsed.timestamp,
                    session_id: parsed.session_id,
                    source: parsed.source,
                    landing_page: parsed.landing_page,
                    redis_key: key
                  });
                }
                
                // Return pageview in attribution format (compatible with new system)
                return {
                  // Core attribution fields
                  session_id: parsed.session_id || null,
                  timestamp: parsed.timestamp,
                  landing_page: parsed.landing_page || 'unknown',
                  source: parsed.source || 'direct',
                  ip_address: parsed.ip_address,
                  
                  // Canvas/WebGL fingerprints for matching
                  canvas_fingerprint: parsed.canvas_fingerprint || null,
                  webgl_fingerprint: parsed.webgl_fingerprint || null,
                  
                  // Attribution context
                  referrer_url: parsed.referrer_url || null,
                  utm_campaign: parsed.utm_campaign || null,
                  utm_source: parsed.utm_source || null,
                  utm_medium: parsed.utm_medium || null,
                  utm_term: parsed.utm_term || null,
                  utm_content: parsed.utm_content || null,
                  
                  // Technical data for matching
                  screen_resolution: parsed.screen_resolution || null,
                  cpu_cores: parsed.cpu_cores || null,
                  memory_gb: parsed.memory_gb || null,
                  
                  // Original key reference
                  redis_key: key,
                  source_pattern: pattern
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
      
      // Store chunk if we have data (compatible with new system)
      if (chunkPageviews.length > 0) {
        const chunkId = `simple_chunk_${existingProgress.chunks_completed + thisRunChunksStored + 1}`;
        await storeAttributionChunk(redis, chunkPageviews, chunkId);
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
    
    console.log(`🏁 SIMPLE extraction summary for this run:`);
    console.log(`   📊 This run pageviews: ${thisRunPageviews.length}`);
    console.log(`   📊 Total pageviews: ${existingProgress.total_extracted + thisRunPageviews.length}`);
    console.log(`   🔍 This run keys scanned: ${thisRunKeysScanned}`);
    console.log(`   📦 This run chunks: ${thisRunChunksProcessed}`);
    console.log(`   💾 This run chunks stored: ${thisRunChunksStored}`);
    console.log(`   🌐 Total unique IPs: ${allUniqueIPs.size}`);
    console.log(`   ✅ Complete: ${isComplete}`);
    console.log(`   🎯 Final cursor: ${cursor}`);
    console.log(`   ⏱️ This run time: ${processingTime}ms`);
    
    // 🎯🎯🎯 DEBUG: Final target IP summary
    console.log(`🎯🎯🎯 TARGET IP 42.61.210.120 FINAL SUMMARY:`);
    console.log(`   🎯 Keys found in scan: ${targetIPKeysFound}`);
    console.log(`   🎯 Keys after filter: ${targetIPKeysAfterFilter}`);
    console.log(`   🎯 Pageviews extracted: ${targetIPPageviewsExtracted}`);
    console.log(`   🎯 Target IP in unique IPs: ${allUniqueIPs.has('42.61.210.120')}`);
    
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
      processing_time_ms: processingTime,
      attribution_fields_found: Array.from(attributionFieldsFound)
    };
    
  } catch (error) {
    console.error('❌ Simple extraction error:', error);
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

// Store attribution chunk (compatible with new system)
async function storeAttributionChunk(redis, pageviews, chunkId) {
  if (pageviews.length === 0) return;
  
  const chunkKey = `attribution_data_chunk:v1_${chunkId}:${Date.now()}`;
  const chunkData = {
    chunk_id: chunkId,
    pageview_count: pageviews.length,
    pageviews: pageviews,
    created_at: new Date().toISOString(),
    version: 'v1',
    attribution_ready: true
  };
  
  await redis(`setex/${chunkKey}/2592000/${encodeURIComponent(JSON.stringify(chunkData))}`); // 30 days TTL
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
